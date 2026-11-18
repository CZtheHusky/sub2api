//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
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
	proxyDone := make(chan error, 1)
	readLimit := ResolveOpenAIWSClientReadLimitBytes(svc.cfg)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		proxyDone <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), payload, hooks)
	}))
	defer gateway.Close()

	configDir := t.TempDir()
	catalogPath := filepath.Join(configDir, "models.json")
	catalog, err := json.Marshal(map[string][]json.RawMessage{"models": {openai.CodexGPT61SolMetadata}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(catalogPath, catalog, 0600))
	config := fmt.Sprintf(`model = %q
model_provider = "fixture"
model_catalog_json = %q
check_for_update_on_startup = false
[features]
interrupt_on_cancel = true
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 0
`, codexOAuthWSSmokeModel, catalogPath, gateway.URL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(config), 0600))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "app-server", "--listen", "stdio://")
	cmd.Dir = t.TempDir()
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + configDir, "SUB2API_TEST_TOKEN=sk-test"}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("CLI stderr:\n%s", stderr.String())
		}
	}()

	encoder, decoder := json.NewEncoder(stdin), json.NewDecoder(stdout)
	readUntil := func(predicate func(gjson.Result) bool) gjson.Result {
		for {
			var raw json.RawMessage
			require.NoError(t, decoder.Decode(&raw), "waiting for app-server event")
			message := gjson.ParseBytes(raw)
			require.NotEqual(t, "error", message.Get("method").String(), "%s", message.Raw)
			if predicate(message) {
				return message
			}
		}
	}
	send := func(id int64, method string, params map[string]any) {
		require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
	}
	call := func(id int64, method string, params map[string]any) gjson.Result {
		send(id, method, params)
		reply := readUntil(func(message gjson.Result) bool { return message.Get("id").Int() == id })
		require.True(t, reply.Get("result").Exists(), "%s: %s", method, reply.Raw)
		return reply.Get("result")
	}
	call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "sub2api-loopback", "version": "1.0.0"}})
	require.NoError(t, encoder.Encode(map[string]string{"jsonrpc": "2.0", "method": "initialized"}))
	thread := call(2, "thread/start", map[string]any{"cwd": cmd.Dir, "ephemeral": true}).Get("thread.id").String()
	require.NotEmpty(t, thread)
	turn := call(3, "turn/start", map[string]any{
		"threadId": thread,
		"input":    []map[string]string{{"type": "text", "text": "Generate a response we can interrupt"}},
	}).Get("turn.id").String()
	require.NotEmpty(t, turn)

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

	var acknowledged, interrupted bool
	for !acknowledged || !interrupted {
		message := readUntil(func(message gjson.Result) bool {
			return message.Get("id").Int() == 4 || message.Get("method").String() == "turn/completed"
		})
		if message.Get("id").Int() == 4 {
			require.True(t, message.Get("result").Exists(), "%s", message.Raw)
			acknowledged = true
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
	case err := <-proxyDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("gateway did not close the session after the CLI exited")
	}
	require.Empty(t, results)
}
