//go:build unit

package service

import (
	"bytes"
	"context"
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
		output := runCodexOAuthWSSmokeCLI(t, binary, baseURL, configDir, workingDir)
		var completed, answered bool
		for line := range strings.SplitSeq(output, "\n") {
			switch gjson.Get(line, "type").String() {
			case "item.completed":
				answered = answered || gjson.Get(line, "item.text").String() == "LOOPBACK_OK"
			case "turn.completed":
				completed = true
			}
		}
		require.True(t, completed && answered, "CLI output:\n%s", output)
		require.Len(t, upstreamRequests, 2, "each CLI turn must send one warmup and one generation")
		results = append(results, [][]byte{<-upstreamRequests, <-upstreamRequests})
	}

	direct, proxied := results[0], results[1]
	for i, original := range direct {
		forwarded := proxied[i]
		for _, key := range []string{"type", "model", "stream", "generate", "previous_response_id"} {
			require.Equal(t, gjson.GetBytes(original, key).Raw, gjson.GetBytes(forwarded, key).Raw, "frame %d field %s", i, key)
		}
		require.Equal(t, codexOAuthWSSmokeModel, gjson.GetBytes(forwarded, "model").String())
		require.True(t, gjson.GetBytes(forwarded, "stream").Bool())
		originalTools, forwardedTools := gjson.GetBytes(original, "tools"), gjson.GetBytes(forwarded, "tools")
		require.Equal(t, originalTools.Exists(), forwardedTools.Exists(), "frame %d tools presence", i)
		require.True(t, reflect.DeepEqual(originalTools.Value(), forwardedTools.Value()), "frame %d tools differ", i)
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

func runCodexOAuthWSSmokeCLI(t *testing.T, binary string, baseURL string, configDir, workingDir string) string {
	t.Helper()
	contents := fmt.Sprintf(`model = %q
model_provider = "fixture"
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
	cmd := exec.CommandContext(ctx, binary, "exec", "--skip-git-repo-check", "--ephemeral", "--json", codexOAuthWSSmokePrompt)
	cmd.Dir = workingDir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + configDir, "SUB2API_TEST_TOKEN=sk-test"}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	require.NoError(t, err, "CLI stderr:\n%s\nCLI stdout:\n%s", &stderr, stdout)
	return string(stdout)
}
