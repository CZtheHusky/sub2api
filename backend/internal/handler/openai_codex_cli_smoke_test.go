//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const (
	smokeModel    = "gpt-6.1-sol"
	smokeAnswer   = "LOOPBACK_OK"
	smokePrompt   = "Return exactly the fixture marker."
	smokeClient   = "smoke-client-key"
	smokeUpstream = "smoke-upstream-key"
)

// TestCodexCLIResponsesSmoke uses the real CLI and gateway HTTP/WS handlers.
// All network traffic for the configured provider is local to this test.
func TestCodexCLIResponsesSmoke(t *testing.T) {
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to a Codex executable to run this test")
	}
	version, err := exec.Command(binary, "--version").CombinedOutput()
	require.NoError(t, err, "%s", version)
	t.Logf("%s", strings.TrimSpace(string(version)))
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                string
		useWS               bool
		readLimit           int64
		rejectWebSocketTurn bool
	}{
		{name: "HTTP"},
		{name: "WebSocket", useWS: true},
		{name: "HTTPFallback", useWS: true, readLimit: 8 << 10},
		{name: "HTTPFallbackAfterUpstream1009", useWS: true, rejectWebSocketTurn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type request struct {
				payload []byte
				ws      bool
			}
			var mu sync.Mutex
			var requests []request
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer "+smokeUpstream {
					t.Errorf("unexpected upstream request %s %s or credentials", r.Method, r.URL.Path)
					http.Error(w, "unexpected upstream request", http.StatusBadRequest)
					return
				}
				record := func(payload []byte, ws bool) {
					mu.Lock()
					defer mu.Unlock()
					requests = append(requests, request{payload: payload, ws: ws})
				}
				if r.Method == http.MethodPost {
					payload, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					record(payload, false)
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range codexSmokeResponsesEvents("resp_http", true) {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
					}
					return
				}
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				conn.SetReadLimit(1 << 20)
				ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
				defer cancel()
				for i := 0; ; i++ {
					_, payload, err := conn.Read(ctx)
					if err != nil {
						return
					}
					record(payload, true)
					generate := gjson.GetBytes(payload, "generate")
					generates := !generate.Exists() || generate.Bool()
					if tc.rejectWebSocketTurn && generates {
						_ = conn.Close(coderws.StatusMessageTooBig, "fixture upstream message limit")
						return
					}
					for _, event := range codexSmokeResponsesEvents(fmt.Sprintf("resp_ws_%d", i), generates) {
						if err := conn.Write(ctx, coderws.MessageText, []byte(event)); err != nil {
							return
						}
					}
				}
			}))
			defer upstream.Close()

			gateway := newCodexSmokeGateway(t, upstream.URL, tc.readLimit, service.OpenAIWSIngressModePassthrough)
			defer gateway.Close()
			prompt := smokePrompt
			if tc.readLimit != 0 {
				prompt += strings.Repeat(" test", int(tc.readLimit)/4)
			}
			output := runCodexSmokeCLI(t, binary, gateway.URL, tc.useWS, prompt)
			var completed, answered bool
			for line := range strings.SplitSeq(output, "\n") {
				switch gjson.Get(line, "type").String() {
				case "item.completed":
					answered = answered || gjson.Get(line, "item.text").String() == smokeAnswer
				case "turn.completed":
					completed = true
				}
			}
			require.True(t, completed && answered, "CLI output:\n%s", output)

			mu.Lock()
			defer mu.Unlock()
			var generations, warmups, rejectedTurns int
			for _, hit := range requests {
				payload := hit.payload
				require.Equal(t, smokeModel, gjson.GetBytes(payload, "model").String())
				require.True(t, gjson.GetBytes(payload, "stream").Bool())
				if hit.ws {
					require.Equal(t, "response.create", gjson.GetBytes(payload, "type").String())
				}
				if generate := gjson.GetBytes(payload, "generate"); generate.Exists() && !generate.Bool() {
					require.True(t, tc.useWS && hit.ws, "warmup must use WebSocket")
					warmups++
					continue
				}
				require.Contains(t, string(payload), smokePrompt)
				if tc.rejectWebSocketTurn && hit.ws {
					rejectedTurns++
					continue
				}
				generations++
				require.Equal(t, tc.useWS && tc.readLimit == 0 && !tc.rejectWebSocketTurn, hit.ws, "unexpected transport for generation")
				if tc.readLimit != 0 {
					require.Greater(t, int64(len(payload)), tc.readLimit)
				}
			}
			require.Equal(t, 1, generations, "expected exactly one generation request")
			if tc.readLimit != 0 {
				require.Equal(t, 1, warmups, "WebSocket warmup must fit within the advertised limit")
				require.NotContains(t, output, "stream disconnected before completion", "fallback must happen before sending the oversized frame")
			}
			if tc.rejectWebSocketTurn {
				require.Equal(t, 1, warmups, "WebSocket must complete the warmup without retrying it")
				require.Equal(t, 1, rejectedTurns, "fallback must not retry the WebSocket generation")
			}
			t.Logf("%s completed with %d upstream requests", tc.name, len(requests))
		})
	}
}

func codexSmokeResponsesEvents(responseID string, generate bool) []string {
	response := fmt.Sprintf(`"id":%q,"object":"response","created_at":1,"model":%q`, responseID, smokeModel)
	events := []string{`{"type":"response.created","response":{` + response + `,"status":"in_progress","output":[]}}`}
	output := "[]"
	if generate {
		item := fmt.Sprintf(`{"id":"msg_smoke","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":%q,"annotations":[]}]}`, smokeAnswer)
		events = append(events, `{"type":"response.output_item.done","item":`+item+`,"output_index":0}`)
		output = "[" + item + "]"
	}
	return append(events, `{"type":"response.completed","response":{`+response+`,"status":"completed","output":`+output+`,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
}

func TestOpenAIResponsesWebSocket_UpstreamMessageTooBigDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeDedicated} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer func() { _ = conn.CloseNow() }()
				ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
				defer cancel()
				if _, _, err := conn.Read(ctx); err != nil {
					return
				}
				requests.Add(1)
				_ = conn.Close(coderws.StatusMessageTooBig, "fixture upstream request limit")
			}))
			t.Cleanup(upstream.Close)
			gateway := newCodexSmokeGateway(t, upstream.URL, 0, mode)
			t.Cleanup(gateway.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			headers := http.Header{"Authorization": {"Bearer " + smokeClient}}
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http")+"/v1/responses", &coderws.DialOptions{HTTPHeader: headers})
			require.NoError(t, err)
			defer func() { _ = conn.CloseNow() }()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6.1-sol","input":[]}`)))
			_, _, err = conn.Read(ctx)
			require.Equal(t, coderws.StatusMessageTooBig, coderws.CloseStatus(err), "client read error: %v", err)
			require.Equal(t, int32(1), requests.Load(), "the gateway must not replay a rejected message")
		})
	}
}

func newCodexSmokeGateway(t *testing.T, upstreamURL string, clientReadLimit int64, ingressMode string) *httptest.Server {
	t.Helper()
	accountRepo := &openAIWSUsageHandlerAccountRepoStub{account: service.Account{
		ID: 1, Name: "codex-smoke", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": smokeUpstream, "base_url": upstreamURL},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_enabled": true,
			"openai_apikey_responses_websockets_v2_mode":    ingressMode,
		},
	}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 5
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 5
	cfg.Gateway.OpenAIWS.ClientReadLimitBytes = clientReadLimit
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	gateway := service.NewOpenAIGatewayService(accountRepo, &openAIWSUsageHandlerUsageLogRepoStub{}, nil, nil, nil, nil, nil,
		cfg, nil, nil, service.NewBillingService(cfg, nil), nil, billing, repository.NewHTTPUpstream(cfg),
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(gateway.CloseOpenAIWSPool)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(&concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}), billing, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
	groupID := int64(1)
	key := &service.APIKey{ID: 1, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}, User: &service.User{ID: 1, Status: service.StatusActive}}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer "+smokeClient {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: key.User.ID, Concurrency: 1})
	})
	router.POST("/v1/responses", h.Responses)
	router.GET("/v1/responses", h.ResponsesWebSocket)
	return httptest.NewServer(router)
}

func runCodexSmokeCLI(t *testing.T, binary string, baseURL string, useWS bool, prompt string) string {
	t.Helper()
	dir := t.TempDir()
	contents := fmt.Sprintf(`model = %q
model_provider = "fixture"
check_for_update_on_startup = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_SMOKE_KEY"
supports_websockets = %t
request_max_retries = 0
stream_max_retries = 1
`, smokeModel, baseURL+"/v1", useWS)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.toml"), []byte(contents), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "exec", "--skip-git-repo-check", "--ephemeral", "--json", prompt)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + dir, "SUB2API_SMOKE_KEY=" + smokeClient}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	require.NoError(t, err, "CLI stderr:\n%s\nCLI stdout:\n%s", &stderr, stdout)
	return string(stdout)
}
