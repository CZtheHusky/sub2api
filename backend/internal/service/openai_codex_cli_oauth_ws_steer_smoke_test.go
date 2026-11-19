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

const (
	codexCLI170CancelTerminal = `{"type":"response.completed","response":{"id":"resp_generated","object":"response","created_at":1,"model":"gpt-6.1-sol","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":2,"total_tokens":13}}}`
	codexCLI170Reject         = `{"type":"response.interrupt.failed","response_id":"resp_generated"}`
)

func TestCodexCLI170OAuthCtxPoolSteering(t *testing.T) {
	testCodexCLI170OAuthCtxPoolSteering(t, "incremental")
}

func TestCodexCLI170OAuthCtxPoolSteeringAfterRejection(t *testing.T) {
	t.Run("rejection before terminal", func(t *testing.T) {
		testCodexCLI170OAuthCtxPoolSteering(t, "reject before terminal")
	})
	t.Run("rejection after terminal", func(t *testing.T) {
		testCodexCLI170OAuthCtxPoolSteering(t, "reject after terminal")
	})
}

func TestCodexCLI170OAuthCtxPoolSteeringAfterEffortChange(t *testing.T) {
	testCodexCLI170OAuthCtxPoolSteering(t, "changed effort")
}

func TestCodexCLI170OAuthCtxPoolSteeringAfterToolResult(t *testing.T) {
	testCodexCLI170OAuthCtxPoolSteering(t, "tool then changed effort")
}

func testCodexCLI170OAuthCtxPoolSteering(t *testing.T, scenario string) {
	t.Helper()
	binary := requireCodexCLI170ResponsesLiteBinary(t)
	server, upstream, dialer, results, proxyDone := startCodexCLI170OAuthCtxPoolSmokeGateway(t)
	app := startCodexCLI170ResponsesLiteAppServer(t, binary, server.URL)
	startCodexCLI170GenerationRequest(t, app, upstream)
	responseIDs := []string{"resp_warmup"}
	var toolOutput string
	if scenario == "tool then changed effort" {
		toolOutput = completeCodexCLI170ToolCall(t, upstream)
		responseIDs = append(responseIDs, "resp_tool")
	}
	firstEvents := startCodexCLI170TextStream(t, app, upstream)
	changedEffort := scenario == "changed effort" || scenario == "tool then changed effort"

	if changedEffort {
		app.call(5, "turn/settings/update", map[string]any{"threadId": app.thread, "turnId": app.turn, "effort": "high"})
	}
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
	if scenario == "reject before terminal" {
		upstream.Send(codexCLI170Reject)
	}
	select {
	case <-upstream.closed:
		t.Fatal("steering must preserve the current upstream connection while draining")
	case <-time.After(1200 * time.Millisecond):
	}
	for _, event := range firstEvents[3:] {
		upstream.Send(event)
	}
	if scenario == "reject after terminal" {
		upstream.Send(codexCLI170Reject)
	}

	followUp := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "response.create", gjson.GetBytes(followUp, "type").String())
	previous := gjson.GetBytes(followUp, "previous_response_id")
	require.Equal(t, app.turn, gjson.GetBytes(followUp, "client_metadata.turn_id").String())
	input := gjson.GetBytes(followUp, "input").Array()
	require.NotEmpty(t, input)
	if changedEffort {
		require.False(t, previous.Exists())
		require.Equal(t, "high", gjson.GetBytes(followUp, "reasoning.effort").String())
		var foundOriginal, foundCall, foundOutput bool
		for _, item := range input[:len(input)-1] {
			if item.Get("type").Str == "message" && item.Get("role").Str == "user" {
				for _, content := range item.Get("content").Array() {
					if content.Get("text").Str == "Generate a response we can interrupt" {
						foundOriginal = true
					}
				}
			}
			if item.Get("type").Str == "function_call" && item.Get("call_id").Str == "call_tool" {
				require.True(t, foundOriginal)
				require.Equal(t, "exec_command", item.Get("name").Str)
				foundCall = true
			}
			if item.Get("type").Str == "function_call_output" && item.Get("call_id").Str == "call_tool" {
				require.True(t, foundCall, "tool output must follow its matching call")
				require.Equal(t, toolOutput, item.Get("output").Str)
				foundOutput = true
			}
		}
		require.True(t, foundOriginal)
		if toolOutput != "" {
			require.True(t, foundOutput, "the full continuation must preserve the tool result")
		}
	} else {
		require.Equal(t, "resp_generated", previous.String())
		require.Len(t, input, 1, "the continuation carries only the new steering input")
	}
	last := input[len(input)-1]
	require.Equal(t, "message", last.Get("type").String())
	require.Equal(t, "user", last.Get("role").String())
	require.Equal(t, steeringText, last.Get("content.0.text").String())
	require.Equal(t, 1, dialer.DialCount(), "steering should continue from the drained response on the same upstream lease")
	for _, event := range codexOAuthWSSmokeEvents(true) {
		upstream.Send(strings.NewReplacer("resp_generated", "resp_steered", "msg_smoke", "msg_steered").Replace(event))
	}
	finished := app.readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "turn/completed" })
	require.Equal(t, app.turn, finished.Get("params.turn.id").String())
	require.Equal(t, "completed", finished.Get("params.turn.status").String())

	for i, responseID := range append(responseIDs, "resp_generated", "resp_steered") {
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
	testCodexCLI170OAuthCtxPoolCancelDeadline(t, codexCLI170CancelTerminal, true)
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
	requireCodexCLI170CancelResult(t, app, results, gotTerminal, context.Canceled)
}

func TestCodexCLI170OAuthCtxPoolCancelRejected(t *testing.T) {
	for _, terminalFirst := range []bool{false, true} {
		name := "rejection before terminal"
		if terminalFirst {
			name = "rejection after terminal"
		}
		t.Run(name, func(t *testing.T) {
			binary := requireCodexCLI170ResponsesLiteBinary(t)
			server, upstream, _, results, proxyDone := startCodexCLI170OAuthCtxPoolSmokeGateway(t)
			app := startCodexCLI170ResponsesLiteAppServer(t, binary, server.URL)
			startCodexCLI170ResponsesLiteGeneration(t, app, upstream)

			app.send(4, "turn/interrupt", map[string]any{"threadId": app.thread, "turnId": app.turn})
			control := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
			require.JSONEq(t, `{"type":"response.interrupt","response_id":"resp_generated","mode":"discard_partial_items"}`, string(control))
			if terminalFirst {
				upstream.Send(codexCLI170CancelTerminal)
			}
			upstream.Send(codexCLI170Reject)
			// An explicit rejection should make the live CLI retire its
			// socket before its one-second cancellation deadline.
			select {
			case err := <-proxyDone:
				require.ErrorIs(t, err, errOpenAIWSInterruptRejected)
			case <-time.After(800 * time.Millisecond):
				t.Fatal("the CLI did not abandon the rejected cancellation promptly")
			}
			select {
			case <-upstream.closed:
			default:
				t.Fatal("the rejected cancellation retained the upstream lease")
			}
			requireCodexCLI170CancelResult(t, app, results, terminalFirst, errOpenAIWSInterruptRejected)
		})
	}
}

func requireCodexCLI170CancelResult(t *testing.T, app codexCLI170ResponsesLiteAppServer, results <-chan codexCLI170TurnOutcome, gotTerminal bool, cancelErr error) {
	t.Helper()
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
		require.ErrorIs(t, canceled.err, cancelErr)
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
	results := make(chan codexCLI170TurnOutcome, 4)
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
	startCodexCLI170GenerationRequest(t, app, upstream)
	return startCodexCLI170TextStream(t, app, upstream)
}

func completeCodexCLI170ToolCall(t *testing.T, upstream *interruptTestConn) string {
	t.Helper()
	const item = `{"type":"function_call","id":"fc_tool","call_id":"call_tool","namespace":"functions","name":"exec_command","arguments":"{\"cmd\":\"echo CODEX_TOOL_OK\"}","status":"completed"}`
	upstream.Send(strings.ReplaceAll(codexOAuthWSSmokeEvents(true)[0], "resp_generated", "resp_tool"))
	upstream.Send(`{"type":"response.output_item.added","output_index":0,"item":` + item + `}`)
	upstream.Send(`{"type":"response.output_item.done","output_index":0,"item":` + item + `}`)
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_tool","status":"completed","output":[` + item + `],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
	next := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, 5*time.Second)
	require.Equal(t, "resp_tool", gjson.GetBytes(next, "previous_response_id").Str)
	items := gjson.GetBytes(next, "input").Array()
	require.Len(t, items, 1)
	require.Equal(t, "function_call_output", items[0].Get("type").Str)
	require.Equal(t, "call_tool", items[0].Get("call_id").Str)
	output := items[0].Get("output").Str
	require.Contains(t, output, "CODEX_TOOL_OK")
	return output
}

func startCodexCLI170GenerationRequest(t *testing.T, app codexCLI170ResponsesLiteAppServer, upstream *interruptTestConn) {
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
}

func startCodexCLI170TextStream(t *testing.T, app codexCLI170ResponsesLiteAppServer, upstream *interruptTestConn) []string {
	t.Helper()
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
step_model_switching = true
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
	call(1, "initialize", map[string]any{"capabilities": map[string]bool{"experimentalApi": true}, "clientInfo": map[string]string{"name": "sub2api-loopback", "version": "1.0.0"}})
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
