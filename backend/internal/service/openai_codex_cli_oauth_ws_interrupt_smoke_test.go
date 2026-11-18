//go:build unit

package service

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCodexCLI170OAuthCtxPoolInterrupt(t *testing.T) {
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to Codex 0.170.0 to run this test")
	}
	version, err := exec.Command(binary, "--version").Output()
	require.NoError(t, err)
	require.Equal(t, "codex-cli 0.170.0", strings.TrimSpace(string(version)))
	gin.SetMode(gin.TestMode)

	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, dialer := newInterruptTestService(t, upstream)
	results := make(chan *OpenAIForwardResult, 2)
	type downstreamResult struct {
		err          error
		cancelEvents []string
	}
	proxyDone := make(chan downstreamResult, 1)
	readLimit := ResolveOpenAIWSClientReadLimitBytes(svc.cfg)
	gateway := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/responses" {
			t.Errorf("unexpected gateway request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Codex-WebSocket-Max-Message-Bytes", strconv.FormatInt(readLimit, 10))
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(readLimit)
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		_, payload, err := ReadOpenAIWSClientMessage(r.Context(), conn, 5*time.Second, coderws.StatusPolicyViolation, "missing response.create")
		if err != nil {
			t.Error(err)
			return
		}
		hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err != nil {
				t.Error(err)
				return
			}
			select {
			case results <- result:
			default:
				t.Error("unexpected extra upstream turn")
			}
		}}
		outcome := downstreamResult{err: svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), payload, hooks)}
		// The app-server JSON-RPC result acknowledges the local request. Check
		// the separate upstream acknowledgement on the CLI's actual socket.
		wire := r.Context().Value(codexOAuthWSWireConnKey{}).(*codexOAuthWSWireConn)
		var message []byte
		wire.takeDataFrames(func(frame codexOAuthWSWireFrame, payload []byte) {
			message = append(message, payload...)
			if frame.final {
				event := gjson.ParseBytes(message)
				switch kind := event.Get("type").String(); kind {
				case "response.interrupt.accepted":
					outcome.cancelEvents = append(outcome.cancelEvents, kind+":"+event.Get("response_id").String())
				case "response.completed":
					if id := event.Get("response.id").String(); id == "resp_generated" {
						outcome.cancelEvents = append(outcome.cancelEvents, kind+":"+id)
					}
				}
				message = message[:0]
			}
		})
		proxyDone <- outcome
	}))
	observeCodexOAuthWSWire(gateway, true)
	gateway.Start()
	defer gateway.Close()

	app := startCodexCLI170ResponsesLiteAppServer(t, binary, gateway.URL)
	ctx, stdin, send, readUntil, thread, turn := app.ctx, app.stdin, app.send, app.readUntil, app.thread, app.turn

	warmup := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "false", gjson.GetBytes(warmup, "generate").Raw)
	for _, event := range codexOAuthWSSmokeEvents(false) {
		upstream.Send(event)
	}
	create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
	require.NotEqual(t, "false", gjson.GetBytes(create, "generate").Raw)
	require.Equal(t, "resp_warmup", gjson.GetBytes(create, "previous_response_id").String())
	require.Equal(t, "true", gjson.GetBytes(create, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String())
	// Start generation and leave it pending until the client cancels it.
	for _, event := range codexOAuthWSSmokeEvents(true)[:3] {
		upstream.Send(event)
	}
	readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "item/agentMessage/delta" })
	send(4, "turn/interrupt", map[string]any{"threadId": thread, "turnId": turn})
	control := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.JSONEq(t, `{"type":"response.interrupt","response_id":"resp_generated","mode":"discard_partial_items"}`, string(control))
	upstream.Send(`{"type":"response.interrupt.accepted","response_id":"resp_generated"}`)
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_generated","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":2}}}`)

	var interruptReplied, interrupted bool
	for !interruptReplied || !interrupted {
		message := readUntil(func(message gjson.Result) bool {
			return message.Get("id").Int() == 4 || message.Get("method").String() == "turn/completed"
		})
		if message.Get("id").Int() == 4 {
			require.True(t, message.Get("result").Exists(), "%s", message.Raw)
			interruptReplied = true
		} else {
			require.Equal(t, turn, message.Get("params.turn.id").String())
			require.Equal(t, "interrupted", message.Get("params.turn.status").String())
			interrupted = true
		}
	}
	require.Equal(t, 1, dialer.DialCount(), "cancel must use the same upstream connection as generation")
	select {
	case result := <-results:
		require.NotNil(t, result)
		require.Equal(t, "resp_warmup", result.RequestID)
	case <-ctx.Done():
		t.Fatal("gateway did not finish warmup")
	}
	select {
	case result := <-results:
		require.NotNil(t, result)
		require.Equal(t, "resp_generated", result.RequestID)
		require.Equal(t, 11, result.Usage.InputTokens)
		require.Equal(t, 2, result.Usage.OutputTokens)
	case <-ctx.Done():
		t.Fatal("gateway did not record the interrupted response usage")
	}
	require.NoError(t, stdin.Close())
	select {
	case outcome := <-proxyDone:
		require.NoError(t, outcome.err)
		require.Equal(t, []string{"response.interrupt.accepted:resp_generated", "response.completed:resp_generated"}, outcome.cancelEvents,
			"the gateway must write the upstream acknowledgement and terminal to the CLI socket")
	case <-ctx.Done():
		t.Fatal("gateway did not close the session after the CLI exited")
	}
	require.Empty(t, results)
}
