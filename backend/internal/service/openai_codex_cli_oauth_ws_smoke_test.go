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
	"reflect"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const (
	codexOAuthWSSmokeModel  = "gpt-6.1-sol"
	codexOAuthWSSmokePrompt = "Return exactly LOOPBACK_OK."
)

type codexOAuthWSSmokeDialer struct {
	loopback string
}

func (d codexOAuthWSSmokeDialer) Dial(ctx context.Context, target string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	if target != strings.Replace(chatgptCodexURL, "https://", "wss://", 1) {
		return nil, 0, nil, fmt.Errorf("unexpected OAuth Responses target %q", target)
	}
	return newDefaultOpenAIWSClientDialer().Dial(ctx, d.loopback, headers, "")
}

func TestCodexCLI170OAuthWebSocketParity(t *testing.T) {
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to Codex 0.170.0 to run this test")
	}
	version, err := exec.Command(binary, "--version").Output()
	require.NoError(t, err)
	require.Equal(t, "codex-cli 0.170.0", strings.TrimSpace(string(version)))
	gin.SetMode(gin.TestMode)

	upstreamRequests := make(chan []byte, 8)
	deltasReceived := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Method != http.MethodGet {
			t.Errorf("unexpected upstream request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != "Bearer sk-test" {
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
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		conn.SetReadLimit(1 << 20)
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			select {
			case upstreamRequests <- payload:
			case <-ctx.Done():
				return
			}
			generate := gjson.GetBytes(payload, "generate")
			for _, event := range codexOAuthWSSmokeEvents(!generate.Exists() || generate.Bool()) {
				if gjson.Get(event, "type").String() == "response.output_item.done" {
					// Hold back both events that repeat the complete answer.
					select {
					case <-deltasReceived:
					case <-ctx.Done():
						return
					}
				}
				if err := conn.Write(ctx, coderws.MessageText, []byte(event)); err != nil {
					return
				}
			}
		}
	}))
	defer upstream.Close()

	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 6
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 6
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 6
	account := passthroughLifecycleAccount()
	account.Type = AccountTypeOAuth
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
	account.Credentials = map[string]any{"access_token": "sk-test"}
	svc := newPassthroughLifecycleService(cfg, nil)
	svc.openaiWSPassthroughDialer = codexOAuthWSSmokeDialer{loopback: "ws" + strings.TrimPrefix(upstream.URL, "http") + "/v1/responses"}
	proxyErrors := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(ResolveOpenAIWSClientReadLimitBytes(cfg))
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		_, payload, err := ReadOpenAIWSClientMessage(r.Context(), conn, 8*time.Second, coderws.StatusPolicyViolation, "missing response.create")
		if err == nil {
			err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), payload, nil)
		}
		if err != nil {
			t.Errorf("OAuth WebSocket gateway: %v", err)
		}
		select {
		case proxyErrors <- err:
		default:
		}
	}))
	defer gateway.Close()

	configDir, workingDir := t.TempDir(), t.TempDir()
	results := make([][][]byte, 0, 2)
	for _, baseURL := range []string{upstream.URL, gateway.URL} {
		runCodexOAuthWSSmokeCLI(t, binary, baseURL, configDir, workingDir, deltasReceived)
		require.Len(t, upstreamRequests, 2, "each CLI turn must send one warmup and one generation")
		results = append(results, [][]byte{<-upstreamRequests, <-upstreamRequests})
	}

	direct, proxied := results[0], results[1]
	for i, original := range direct {
		forwarded := proxied[i]
		for _, key := range []string{
			"type", "model", "stream", "generate", "previous_response_id",
			"instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning",
			"include", "store", "text", "service_tier", "prompt_cache_options",
		} {
			before, after := gjson.GetBytes(original, key), gjson.GetBytes(forwarded, key)
			require.Equal(t, before.Exists(), after.Exists(), "frame %d field %s presence", i, key)
			require.Equal(t, before.Value(), after.Value(), "frame %d field %s value", i, key)
		}
		require.Equal(t, codexOAuthWSSmokeModel, gjson.GetBytes(forwarded, "model").String())
		require.True(t, gjson.GetBytes(forwarded, "stream").Bool())
		originalInput := gjson.GetBytes(original, "input").Array()
		forwardedInput := gjson.GetBytes(forwarded, "input").Array()
		require.Len(t, forwardedInput, len(originalInput), "frame %d input count", i)
		for j, originalItem := range originalInput {
			forwardedItem := forwardedInput[j]
			if originalItem.Get("id").Exists() || forwardedItem.Get("id").Exists() {
				originalID, _, hasOriginalID := strings.Cut(originalItem.Get("id").String(), "_")
				forwardedID, _, hasForwardedID := strings.Cut(forwardedItem.Get("id").String(), "_")
				require.True(t, hasOriginalID && hasForwardedID, "frame %d input %d ID shape", i, j)
				require.Equal(t, originalID, forwardedID, "frame %d input %d ID type prefix", i, j)
			}
			// Codex creates a new item ID for each ephemeral session.
			originalContent := originalItem.Value().(map[string]any)
			forwardedContent := forwardedItem.Value().(map[string]any)
			delete(originalContent, "id")
			delete(forwardedContent, "id")
			require.True(t, reflect.DeepEqual(originalContent, forwardedContent), "frame %d input %d content differs", i, j)
		}
	}
	require.Equal(t, "false", gjson.GetBytes(proxied[0], "generate").Raw)
	require.Equal(t, "resp_warmup", gjson.GetBytes(proxied[1], "previous_response_id").String())
	require.Equal(t, "medium", gjson.GetBytes(proxied[1], "reasoning.effort").String())
	require.Contains(t, gjson.GetBytes(proxied[1], "input").Raw, codexOAuthWSSmokePrompt)
	select {
	case proxyErr := <-proxyErrors:
		require.NoError(t, proxyErr)
	case <-time.After(5 * time.Second):
		t.Fatal("OAuth WebSocket gateway did not finish")
	}
}

func codexOAuthWSSmokeEvents(generate bool) []string {
	responseID := "resp_warmup"
	if generate {
		responseID = "resp_generated"
	}
	response := fmt.Sprintf(`"id":%q,"object":"response","created_at":1,"model":%q`, responseID, codexOAuthWSSmokeModel)
	events := []string{`{"type":"response.created","response":{` + response + `,"status":"in_progress","output":[]}}`}
	output := "[]"
	if generate {
		item := `{"id":"msg_smoke","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"LOOPBACK_OK","annotations":[]}]}`
		events = append(events,
			`{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_smoke","type":"message","role":"assistant","status":"in_progress","content":[]}}`,
			`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_smoke","delta":"LOOPBACK_"}`,
			`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_smoke","delta":"OK"}`,
			`{"type":"response.output_item.done","output_index":0,"item":`+item+`}`,
		)
		output = "[" + item + "]"
	}
	return append(events, `{"type":"response.completed","response":{`+response+`,"status":"completed","output":`+output+`,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
}

func runCodexOAuthWSSmokeCLI(t *testing.T, binary, baseURL, configDir, workingDir string, deltasReceived chan<- struct{}) {
	t.Helper()
	tmpDir := t.TempDir()
	contents := fmt.Sprintf(`model = %q
model_provider = "fixture"
model_reasoning_effort = "medium"
check_for_update_on_startup = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 1
`, codexOAuthWSSmokeModel, baseURL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(contents), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "app-server", "--listen", "stdio://")
	cmd.Dir = workingDir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + configDir, "TMPDIR=" + tmpDir, "SUB2API_TEST_TOKEN=sk-test"}
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
	read := func() gjson.Result {
		var raw json.RawMessage
		require.NoError(t, decoder.Decode(&raw), "waiting for app-server event")
		message := gjson.ParseBytes(raw)
		require.NotEqual(t, "error", message.Get("method").String(), "%s", message.Raw)
		return message
	}
	call := func(id int64, method string, params map[string]any) gjson.Result {
		require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
		for {
			reply := read()
			if reply.Get("id").Int() == id {
				require.True(t, reply.Get("result").Exists(), "%s: %s", method, reply.Raw)
				return reply.Get("result")
			}
		}
	}
	call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "sub2api-loopback", "version": "1.0.0"}})
	require.NoError(t, encoder.Encode(map[string]string{"jsonrpc": "2.0", "method": "initialized"}))
	threadID := call(2, "thread/start", map[string]any{"cwd": workingDir, "ephemeral": true}).Get("thread.id").String()
	require.NotEmpty(t, threadID)
	turnID := call(3, "turn/start", map[string]any{
		"threadId": threadID,
		"input":    []map[string]string{{"type": "text", "text": codexOAuthWSSmokePrompt}},
	}).Get("turn.id").String()
	require.NotEmpty(t, turnID)

	var streamed string
	var answered bool
	for {
		message := read()
		switch message.Get("method").String() {
		case "item/agentMessage/delta":
			require.Equal(t, turnID, message.Get("params.turnId").String())
			streamed += message.Get("params.delta").String()
			require.True(t, strings.HasPrefix("LOOPBACK_OK", streamed), "unexpected streamed answer %q", streamed)
			if streamed == "LOOPBACK_OK" {
				select {
				case deltasReceived <- struct{}{}:
				case <-ctx.Done():
					t.Fatal("fake upstream was not waiting for the CLI deltas")
				}
			}
		case "item/completed":
			if message.Get("params.item.type").String() == "agentMessage" {
				require.Equal(t, "LOOPBACK_OK", streamed, "the CLI must receive the deltas before item completion")
				require.Equal(t, "LOOPBACK_OK", message.Get("params.item.text").String())
				answered = true
			}
		case "turn/completed":
			require.Equal(t, turnID, message.Get("params.turn.id").String())
			require.Equal(t, "completed", message.Get("params.turn.status").String())
			require.True(t, answered, "the CLI did not complete the expected text item")
			return
		}
	}
}
