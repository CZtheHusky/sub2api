//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// TestCodexCLI170OAuthResponses uses a single real CLI request as the baseline
// for both OAuth forwarders. The HTTPUpstream recorder never dials outside the test.
func TestCodexCLI170OAuthResponses(t *testing.T) {
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to a Codex 0.170.0 executable to run this test")
	}
	version, err := exec.Command(binary, "--version").Output()
	require.NoError(t, err)
	require.Equal(t, "codex-cli 0.170.0", strings.TrimSpace(string(version)))
	gin.SetMode(gin.TestMode)
	t.Run("default-instructions", func(t *testing.T) {
		runCodexCLI170OAuthResponses(t, binary, "gpt-6.1-sol", "")
	})
	t.Run("explicit-instructions-file", func(t *testing.T) {
		runCodexCLI170OAuthResponses(t, binary, "gpt-5.4", "Use the fixture marker when requested.")
	})
}

func runCodexCLI170OAuthResponses(t *testing.T, binary, model, instructions string) {
	t.Helper()
	const answer = "OAUTH_SMOKE_OK"
	const prompt = "Return exactly the fixture marker."
	const item = `{"id":"msg_oauth_smoke","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"` + answer + `","annotations":[]}]}`
	response := `"id":"resp_oauth_smoke","object":"response","created_at":1,"model":"` + model + `"`
	sse := `data: {"type":"response.created","response":{` + response + `,"status":"in_progress","output":[]}}` + "\n\n" +
		`data: {"type":"response.output_item.done","output_index":0,"item":` + item + `}` + "\n\n" +
		`data: {"type":"response.completed","response":{` + response + `,"status":"completed","output":[` + item + `],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"

	type forwarded struct {
		body   []byte
		header http.Header
		resp   *httptest.ResponseRecorder
		result *OpenAIForwardResult
		err    error
		calls  int
	}
	type capture struct {
		inbound []byte
		header  http.Header
		modes   [2]forwarded
	}
	observed := make(chan capture, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer cli-fixture" {
			http.Error(w, "unexpected fixture request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		observation := capture{inbound: body, header: codexOAuthSmokeProtocolHeaders(r.Header)}
		for i, passthrough := range []bool{false, true} {
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(sse)),
			}}
			service := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = r.Clone(r.Context())
			SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
			account := &Account{ID: 1, Name: "oauth-fixture", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "oauth-fixture"}, Extra: map[string]any{"openai_passthrough": passthrough}}
			result, err := service.Forward(r.Context(), c, account, body)
			out := forwarded{body: upstream.lastBody, resp: rec, result: result, err: err, calls: len(upstream.requests)}
			if upstream.lastReq != nil {
				out.header = codexOAuthSmokeProtocolHeaders(upstream.lastReq.Header)
			}
			observation.modes[i] = out
		}
		observed <- observation
		standard := observation.modes[0].resp
		for key, values := range standard.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(standard.Code)
		_, _ = w.Write(standard.Body.Bytes())
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	var instructionsConfig string
	if instructions != "" {
		path := filepath.Join(dir, "fixture-instructions.txt")
		require.NoError(t, os.WriteFile(path, []byte(instructions), 0600))
		instructionsConfig = fmt.Sprintf("model_instructions_file = %q\n", path)
	}
	contents := fmt.Sprintf(`model = %q
model_provider = "fixture"
model_reasoning_effort = "medium"
%scheck_for_update_on_startup = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_SMOKE_KEY"
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
`, model, instructionsConfig, server.URL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.toml"), []byte(contents), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "exec", "--skip-git-repo-check", "--ephemeral", "--json", prompt)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + dir, "SUB2API_SMOKE_KEY=cli-fixture"}
	output, err := cmd.Output()
	if exit, ok := err.(*exec.ExitError); ok {
		t.Logf("CLI stderr:\n%s", exit.Stderr)
	}
	require.NoError(t, err, "CLI stdout:\n%s", output)
	require.Contains(t, string(output), `"text":"`+answer+`"`)
	require.Contains(t, string(output), `"type":"turn.completed"`)
	require.Len(t, observed, 1, "both OAuth paths must use the same CLI request")
	obs := <-observed
	base := gjson.ParseBytes(obs.inbound)
	require.Equal(t, model, base.Get("model").String())
	require.True(t, base.Get("stream").Bool())
	require.Contains(t, base.Get("input").Raw, prompt)
	require.False(t, base.Get("instructions").Exists(), "Codex 0.170.0 wraps the instructions in input")
	var developerText string
	for _, item := range base.Get("input").Array() {
		if item.Get("role").String() == "developer" && item.Get("type").String() == "message" {
			developerText = item.Get("content.0.text").String()
			break
		}
	}
	require.NotEmpty(t, developerText)
	if instructions != "" {
		require.Equal(t, instructions, developerText)
		require.NotEmpty(t, base.Get("tools").Array())
	} else {
		require.False(t, base.Get("tools").Exists(), "incremental tool definitions are sent in input")
		require.Equal(t, "additional_tools", base.Get("input.0.type").String())
	}
	require.Equal(t, "medium", base.Get("reasoning.effort").String())
	t.Logf("CLI request tools: %d", len(base.Get("tools").Array()))
	t.Logf("CLI protocol headers: %v", obs.header)
	for i, name := range []string{"standard", "passthrough"} {
		t.Run(name, func(t *testing.T) {
			out := obs.modes[i]
			require.NoError(t, out.err)
			require.Equal(t, 1, out.calls)
			require.NotNil(t, out.result)
			require.Equal(t, http.StatusOK, out.resp.Code)
			require.Contains(t, out.resp.Header().Get("Content-Type"), "text/event-stream")
			require.Contains(t, out.resp.Body.String(), `"type":"response.completed"`)
			require.Contains(t, out.resp.Body.String(), `"text":"`+answer+`"`)
			require.Equal(t, "application/json", out.header.Get("Content-Type"))
			require.Equal(t, "text/event-stream", out.header.Get("Accept"))
			t.Logf("OAuth protocol headers: %v", out.header)
			body := gjson.ParseBytes(out.body)
			for _, field := range []string{"model", "stream", "input", "instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "include", "store", "text", "service_tier", "prompt_cache_options"} {
				before, after := base.Get(field), body.Get(field)
				if field == "input" && before.Get("0.type").String() == "additional_tools" {
					// The existing OAuth adapter compacts UUID-shaped tool declaration IDs.
					id := before.Get("0.id").String()
					require.Contains(t, id, "-")
					normalized, err := sjson.Set(before.Raw, "0.id", strings.ReplaceAll(id, "-", ""))
					require.NoError(t, err)
					before = gjson.Parse(normalized)
					t.Log("additional_tools ID compacted; comparing the rest of input as sent by CLI")
				}
				require.Equalf(t, before.Exists(), after.Exists(), "field presence differs: %s", field)
				if before.Exists() {
					require.Truef(t, reflect.DeepEqual(before.Value(), after.Value()), "field content differs: %s", field)
					t.Logf("preserved %s", field)
				}
			}
		})
	}
}

func codexOAuthSmokeProtocolHeaders(headers http.Header) http.Header {
	selected := make(http.Header)
	for _, key := range []string{"Accept", "Content-Type", "User-Agent", "OpenAI-Beta", "Version"} {
		if value := headers.Get(key); value != "" {
			selected.Set(key, value)
		}
	}
	return selected
}
