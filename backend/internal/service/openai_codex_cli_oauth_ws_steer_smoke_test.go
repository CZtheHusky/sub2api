//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func TestCodexCLI170OAuthCtxPoolSteering(t *testing.T) {
	binary := requireCodexCLI170ResponsesLiteBinary(t)
	server, upstream, dialer, results, proxyDone := startCodexCLI170OAuthCtxPoolSmokeGateway(t)
	app := startCodexCLI170ResponsesLiteAppServer(t, binary, server.URL)
	firstEvents := startCodexCLI170ResponsesLiteGeneration(t, app, upstream)

	const steeringText = "Continue with this steering detail"
	reply := app.call(4, "turn/steer", map[string]any{
		"threadId":       app.thread,
		"expectedTurnId": app.turn,
		"input":          []map[string]string{{"type": "text", "text": steeringText}},
	})
	require.Equal(t, app.turn, reply.Get("turnId").String())
	control := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.JSONEq(t, `{"type":"response.interrupt","response_id":"resp_generated","mode":"discard_partial_items"}`, string(control))

	// A steering Drain waits for a terminal under the normal stream deadline.
	// Keep the response open past the CLI cancellation window and omit the ACK.
	select {
	case <-upstream.closed:
		t.Fatal("steering must preserve the current upstream connection while draining")
	case <-time.After(1200 * time.Millisecond):
	}
	for _, event := range firstEvents[3:] {
		upstream.Send(event)
	}

	followUp := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "response.create", gjson.GetBytes(followUp, "type").String())
	require.Equal(t, "resp_generated", gjson.GetBytes(followUp, "previous_response_id").String())
	require.Equal(t, app.turn, gjson.GetBytes(followUp, "client_metadata.turn_id").String())
	input := gjson.GetBytes(followUp, "input").Array()
	require.Len(t, input, 1, "the continuation carries only the new steering input")
	require.Equal(t, "message", input[0].Get("type").String())
	require.Equal(t, "user", input[0].Get("role").String())
	require.Equal(t, steeringText, input[0].Get("content.0.text").String())
	require.Equal(t, 1, dialer.DialCount(), "steering should continue from the drained response on the same upstream lease")
	for _, event := range codexOAuthWSSmokeEvents(true) {
		upstream.Send(strings.NewReplacer("resp_generated", "resp_steered", "msg_smoke", "msg_steered").Replace(event))
	}
	finished := app.readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "turn/completed" })
	require.Equal(t, app.turn, finished.Get("params.turn.id").String())
	require.Equal(t, "completed", finished.Get("params.turn.status").String())

	for i, responseID := range []string{"resp_warmup", "resp_generated", "resp_steered"} {
		select {
		case outcome := <-results:
			require.Equal(t, i+1, outcome.turn)
			require.NoError(t, outcome.err)
			require.NotNil(t, outcome.result)
			require.Equal(t, responseID, outcome.result.RequestID)
			require.Equal(t, 1, outcome.result.Usage.InputTokens)
			require.Equal(t, 1, outcome.result.Usage.OutputTokens)
		case <-app.ctx.Done():
			t.Fatalf("gateway did not record usage for %s", responseID)
		}
	}
	require.NoError(t, app.stdin.Close())
	select {
	case err := <-proxyDone:
		require.NoError(t, err)
	case <-app.ctx.Done():
		t.Fatal("gateway did not close the session after the CLI exited")
	}
	require.Empty(t, results)
}

func TestCodexCLI170OAuthCtxPoolCancelWithoutAck(t *testing.T) {
	const terminal = `{"type":"response.completed","response":{"id":"resp_generated","object":"response","created_at":1,"model":"gpt-6.1-sol","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":2,"total_tokens":13}}}`
	testCodexCLI170OAuthCtxPoolCancelDeadline(t, terminal, true)
}

func TestCodexCLI170OAuthCtxPoolCancelWithoutTerminal(t *testing.T) {
	const accepted = `{"type":"response.interrupt.accepted","response_id":"resp_generated"}`
	testCodexCLI170OAuthCtxPoolCancelDeadline(t, accepted, false)
}

func testCodexCLI170OAuthCtxPoolCancelDeadline(t *testing.T, upstreamEvent string, gotTerminal bool) {
	t.Helper()
	binary := requireCodexCLI170ResponsesLiteBinary(t)
	server, upstream, _, results, proxyDone := startCodexCLI170OAuthCtxPoolSmokeGateway(t)
	app := startCodexCLI170ResponsesLiteAppServer(t, binary, server.URL)
	startCodexCLI170ResponsesLiteGeneration(t, app, upstream)

	app.send(4, "turn/interrupt", map[string]any{"threadId": app.thread, "turnId": app.turn})
	control := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.JSONEq(t, `{"type":"response.interrupt","response_id":"resp_generated","mode":"discard_partial_items"}`, string(control))

	// Keep the app-server running so the CLI's cancellation deadline closes
	// the socket when either half of the upstream handshake is missing.
	started := time.Now()
	upstream.Send(upstreamEvent)
	select {
	case err := <-proxyDone:
		require.ErrorIs(t, err, context.Canceled, "the client socket must close before the gateway's ordinary upstream timeout")
		require.GreaterOrEqual(t, time.Since(started), 800*time.Millisecond, "the CLI closed before waiting for cancellation to finish")
	case <-time.After(1600 * time.Millisecond):
		t.Fatal("the cancellation did not release the pooled upstream by the CLI deadline")
	}
	select {
	case <-upstream.closed:
	default:
		t.Fatal("the CLI abandoned the socket but the gateway left the upstream connection open")
	}

	finished := app.readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "turn/completed" })
	require.Equal(t, app.turn, finished.Get("params.turn.id").String())
	require.Equal(t, "interrupted", finished.Get("params.turn.status").String())
	require.Len(t, results, 2, "warmup and cancellation each trigger one callback")
	warmup := <-results
	require.Equal(t, 1, warmup.turn)
	require.NoError(t, warmup.err)
	require.NotNil(t, warmup.result)
	require.Equal(t, "resp_warmup", warmup.result.RequestID)
	canceled := <-results
	require.Equal(t, 2, canceled.turn)
	if gotTerminal {
		require.NoError(t, canceled.err)
		require.NotNil(t, canceled.result)
		require.Equal(t, "resp_generated", canceled.result.RequestID)
		require.Equal(t, 11, canceled.result.Usage.InputTokens)
		require.Equal(t, 2, canceled.result.Usage.OutputTokens)
	} else {
		require.ErrorIs(t, canceled.err, context.Canceled)
		require.Nil(t, canceled.result, "without a terminal event there is no generation usage to bill")
	}
	require.NoError(t, app.stdin.Close())
}

type codexCLI170TurnOutcome struct {
	turn   int
	result *OpenAIForwardResult
	err    error
}

func startCodexCLI170OAuthCtxPoolSmokeGateway(t *testing.T) (*httptest.Server, *interruptTestConn, *openAIWSQueueDialer, <-chan codexCLI170TurnOutcome, <-chan error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, dialer := newInterruptTestService(t, upstream)
	results := make(chan codexCLI170TurnOutcome, 3)
	proxyDone := make(chan error, 1)
	readLimit := ResolveOpenAIWSClientReadLimitBytes(svc.cfg)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/responses" {
			w.WriteHeader(http.StatusBadRequest)
			t.Errorf("unexpected gateway request %s %s", r.Method, r.URL.Path)
			return
		}
		w.Header().Set("X-Codex-WebSocket-Max-Message-Bytes", strconv.FormatInt(readLimit, 10))
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			proxyDone <- err
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(readLimit)
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		_, payload, err := ReadOpenAIWSClientMessage(r.Context(), conn, 5*time.Second, coderws.StatusPolicyViolation, "missing response.create")
		if err != nil {
			proxyDone <- err
			return
		}
		hooks := &OpenAIWSIngressHooks{AfterTurn: func(turn int, result *OpenAIForwardResult, err error) {
			select {
			case results <- codexCLI170TurnOutcome{turn: turn, result: result, err: err}:
			default:
				t.Error("unexpected extra upstream turn")
			}
		}}
		proxyDone <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), payload, hooks)
	}))
	t.Cleanup(server.Close)
	return server, upstream, dialer, results, proxyDone
}

func startCodexCLI170ResponsesLiteGeneration(t *testing.T, app codexCLI170ResponsesLiteAppServer, upstream *interruptTestConn) []string {
	t.Helper()
	warmup := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "false", gjson.GetBytes(warmup, "generate").Raw)
	for _, event := range codexOAuthWSSmokeEvents(false) {
		upstream.Send(event)
	}
	create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
	require.Equal(t, "resp_warmup", gjson.GetBytes(create, "previous_response_id").String())
	require.Equal(t, "true", gjson.GetBytes(create, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String())
	require.Equal(t, app.turn, gjson.GetBytes(create, "client_metadata.turn_id").String())
	events := codexOAuthWSSmokeEvents(true)
	for _, event := range events[:3] {
		upstream.Send(event)
	}
	app.readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "item/agentMessage/delta" })
	return events
}

func requireCodexCLI170ResponsesLiteBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to Codex 0.170.0 to run this test")
	}
	version, err := exec.Command(binary, "--version").Output()
	require.NoError(t, err)
	require.Equal(t, "codex-cli 0.170.0", strings.TrimSpace(string(version)))
	return binary
}

type codexCLI170ResponsesLiteAppServer struct {
	ctx       context.Context
	stdin     io.WriteCloser
	send      func(int64, string, map[string]any)
	call      func(int64, string, map[string]any) gjson.Result
	readUntil func(func(gjson.Result) bool) gjson.Result
	thread    string
	turn      string
}

func startCodexCLI170ResponsesLiteAppServer(t *testing.T, binary, baseURL string) codexCLI170ResponsesLiteAppServer {
	t.Helper()
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
plugins = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 0
`, codexOAuthWSSmokeModel, catalogPath, baseURL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(config), 0600))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, binary, "app-server", "--listen", "stdio://")
	cmd.Dir = t.TempDir()
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + configDir, "TMPDIR=" + t.TempDir(), "SUB2API_TEST_TOKEN=sk-test"}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("CLI stderr:\n%s", stderr.String())
		}
	})

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

	return codexCLI170ResponsesLiteAppServer{ctx: ctx, stdin: stdin, send: send, call: call, readUntil: readUntil, thread: thread, turn: turn}
}
