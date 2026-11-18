//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIOAuthCtxPoolHTTPFallbackPreservesUserMessageID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const body = `{"model":"gpt-6.1-sol","instructions":"fixture","stream":true,"input":[
		{"type":"message","role":"assistant","id":"msg_assistant","content":[{"type":"output_text","text":"prior assistant reply"}]},
		{"type":"message","role":"user","id":"rs_invalid","content":[{"type":"input_text","text":"prior user message"}]},
		{"type":"message","role":"user","id":"msg_1234567890123456789012345678901234567890123456789012345678901234567890","content":[{"type":"input_text","text":"another prior user message"}]},
		{"type":"message","role":"user","id":"msg_170replay_abc","content":[{"type":"input_text","text":"current user message"}]}
	]}`
	wsRequests := make(chan []byte, 4)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("upstream did not receive the fixture token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_, frame, err := conn.Read(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		select {
		case wsRequests <- frame:
		case <-ctx.Done():
			t.Error(ctx.Err())
			return
		}
		_ = conn.Close(coderws.StatusMessageTooBig, "fixture rejected the request before generating")
	}))
	t.Cleanup(upstream.Close)

	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	account := passthroughLifecycleAccount()
	account.Type = AccountTypeOAuth
	account.Credentials = map[string]any{"access_token": "fixture"}
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool}
	svc := newPassthroughLifecycleService(cfg, nil)
	httpUpstream := instructionsEchoUpstream(`"fixture"`)
	svc.httpUpstream = httpUpstream
	svc.openaiWSPool = newOpenAIWSConnPool(cfg)
	svc.openaiWSPool.setClientDialerForTest(codexOAuthWSSmokeDialer{loopback: "ws" + strings.TrimPrefix(upstream.URL, "http") + "/v1/responses"})
	t.Cleanup(svc.CloseOpenAIWSPool)
	httpRequests := make(chan [][]byte, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		if r.Method == http.MethodPost {
			payload, err := io.ReadAll(r.Body)
			if err == nil {
				SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
				_, err = svc.Forward(r.Context(), c, account, payload)
			}
			if err != nil {
				t.Error(err)
			}
			httpRequests <- httpUpstream.bodies
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), first, nil)
		var closeErr *OpenAIWSClientCloseError
		if !errors.As(err, &closeErr) {
			t.Errorf("expected gateway to report a WebSocket close, got %v", err)
			return
		}
		_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
	}))
	t.Cleanup(gateway.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := gateway.Client()
	ws, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http")+"/v1/responses", &coderws.DialOptions{HTTPClient: client})
	require.NoError(t, err)
	defer ws.CloseNow()
	require.NoError(t, ws.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create",`+body[1:])))
	_, _, err = ws.Read(ctx)
	require.Equal(t, coderws.StatusMessageTooBig, coderws.CloseStatus(err))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/v1/responses", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	result, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(result), `"type":"response.completed"`)
	require.Len(t, wsRequests, 1, "gateway must not retry the rejected WebSocket request")
	require.Len(t, httpRequests, 1)
	forwardedRequests := <-httpRequests
	require.Len(t, forwardedRequests, 1)
	wsBody, httpBody := <-wsRequests, forwardedRequests[0]
	const userIDPath = "input.3.id"
	require.Equal(t, "msg_170replay_abc", gjson.GetBytes(wsBody, userIDPath).String())
	require.Equal(t, gjson.GetBytes(wsBody, userIDPath).String(), gjson.GetBytes(httpBody, userIDPath).String())
	require.False(t, gjson.GetBytes(httpBody, "input.0.id").Exists(), "unreferenced assistant history stays stripped")
	require.False(t, gjson.GetBytes(httpBody, "input.1.id").Exists(), "invalid user item ID stays stripped")
	require.False(t, gjson.GetBytes(httpBody, "input.2.id").Exists(), "overlength user item ID stays stripped")
}
