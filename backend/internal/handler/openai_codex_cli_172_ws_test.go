//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	codexTrackingModel  = "gpt-6.1-sol"
	codexTrackingPrompt = "Return the fixture marker."
	codexTrackingAnswer = "WS_TRACKING_OK"
	codexTrackingKey    = "fixture-key"
)

// TestCodexCLIWebSocketTracking checks that the gateway preserves the CLI's
// warmup and continuation, and delivers a provider rejection without replaying
// the request. The only configured provider is a loopback WebSocket server.
func TestCodexCLIWebSocketTracking(t *testing.T) {
	binary := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binary == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to a Codex CLI 0.170.0 or 0.172.0 executable")
	}
	binary, err := filepath.Abs(binary)
	require.NoError(t, err)
	output, err := exec.Command(binary, "--version").Output()
	require.NoError(t, err)
	version := strings.TrimSpace(string(output))
	require.Contains(t, []string{"codex-cli 0.170.0", "codex-cli 0.172.0"}, version)
	t.Log(version)
	gin.SetMode(gin.TestMode)

	for _, mode := range []string{"direct", service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		t.Run(mode, func(t *testing.T) {
			t.Run("completed", func(t *testing.T) { trackCodexCLIWebSocket(t, binary, mode, false, false) })
			t.Run("first_frame_statusless_spend_limit", func(t *testing.T) {
				trackCodexCLIWebSocket(t, binary, mode, true, version == "codex-cli 0.170.0")
			})
		})
	}
}

func trackCodexCLIWebSocket(t *testing.T, binary, mode string, reject, legacy bool) {
	t.Helper()
	type frame struct {
		fields                []string
		model, kind, previous string
		warmup, stream        bool
		hasPrompt             bool
	}
	var mu sync.Mutex
	var requests []frame
	var nonWebSocket, upgrades atomic.Int32
	deadline, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ctx, stopCLI := context.WithCancelCause(deadline)
	defer stopCLI(nil)
	legacyWindowElapsed := errors.New("legacy CLI observation window elapsed")
	var rejectionWritten sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Method != http.MethodGet || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			nonWebSocket.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		upgrades.Add(1)
		conn.SetReadLimit(1 << 20)
		for {
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			request := gjson.ParseBytes(payload)
			seen := frame{
				model:    request.Get("model").String(),
				kind:     request.Get("type").String(),
				previous: request.Get("previous_response_id").String(),
				warmup:   request.Get("generate").Type == gjson.False,
				stream:   request.Get("stream").Bool(),
			}
			request.ForEach(func(key, _ gjson.Result) bool {
				seen.fields = append(seen.fields, key.String())
				return true
			})
			slices.Sort(seen.fields)
			for _, item := range request.Get(`input.#(role=="user")#`).Array() {
				text := item.Get(`content.#(type=="input_text").text`).String()
				seen.hasPrompt = seen.hasPrompt || text == codexTrackingPrompt
			}
			mu.Lock()
			requests = append(requests, seen)
			mu.Unlock()
			for _, event := range codexTrackingEvents(!seen.warmup, reject) {
				if err := conn.Write(r.Context(), coderws.MessageText, []byte(event)); err != nil {
					return
				}
			}
			if !seen.warmup && reject && legacy {
				rejectionWritten.Do(func() {
					timer := time.AfterFunc(2*time.Second, func() { stopCLI(legacyWindowElapsed) })
					t.Cleanup(func() { timer.Stop() })
				})
			}
		}
	}))
	t.Cleanup(upstream.Close)
	baseURL := upstream.URL
	if mode != "direct" {
		gateway := newCodexTrackingGateway(t, upstream.URL, mode, &nonWebSocket)
		baseURL = gateway.URL
	}

	dir := t.TempDir()
	configTOML := fmt.Sprintf(`model = %q
model_provider = "fixture"
check_for_update_on_startup = false
[analytics]
enabled = false
[feedback]
enabled = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = %q
wire_api = "responses"
env_key = "SUB2API_CODEX_WS_FIXTURE_KEY"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 1
`, codexTrackingModel, baseURL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.toml"), []byte(configTOML), 0600))
	cmd := exec.CommandContext(ctx, binary, "app-server", "--listen", "stdio://")
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "CODEX_HOME=" + dir, "TMPDIR=" + dir,
		"SUB2API_CODEX_WS_FIXTURE_KEY=" + codexTrackingKey,
	}
	answered, turn, err := runCodexTrackingTurn(t, cmd)
	require.Zero(t, nonWebSocket.Load(), "unexpected non-WebSocket provider request")
	switch {
	case legacy:
		require.ErrorIs(t, context.Cause(ctx), legacyWindowElapsed,
			"0.170.0 ended the turn before its observation window elapsed")
		require.Error(t, err, "0.170.0 must leave this turn pending")
	case reject:
		require.NoError(t, err)
		require.Equal(t, "failed", turn.Get("status").String())
		require.Equal(t, "projectSpendLimitExceeded", turn.Get("error.codexErrorInfo").String())
	default:
		require.NoError(t, err)
		require.Equal(t, "completed", turn.Get("status").String())
	}
	require.Equal(t, !reject, answered)

	mu.Lock()
	defer mu.Unlock()
	require.EqualValues(t, 1, upgrades.Load())
	require.Len(t, requests, 2, "expected one warmup and one generation with no replay")
	for _, request := range requests {
		require.Equal(t, "response.create", request.kind)
		require.Equal(t, codexTrackingModel, request.model)
		require.True(t, request.stream)
	}
	require.True(t, requests[0].warmup, "initial request must be a warmup")
	require.Empty(t, requests[0].previous)
	require.False(t, requests[1].warmup)
	require.Equal(t, "resp_warmup", requests[1].previous, "generation must continue the warmup")
	require.True(t, requests[1].hasPrompt, "generation must include the fixture prompt")
	t.Logf("upgrades=%d frames=%d warmup_fields=%v generation_fields=%v http_requests=%d",
		upgrades.Load(), len(requests), requests[0].fields, requests[1].fields, nonWebSocket.Load())
}

func runCodexTrackingTurn(t *testing.T, cmd *exec.Cmd) (bool, gjson.Result, error) {
	t.Helper()
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	encoder, decoder := json.NewEncoder(stdin), json.NewDecoder(stdout)
	answered := false
	readUntil := func(predicate func(gjson.Result) bool) (gjson.Result, error) {
		for {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return gjson.Result{}, err
			}
			message := gjson.ParseBytes(raw)
			if message.Get("method").String() == "item/completed" &&
				message.Get("params.item.type").String() == "agentMessage" {
				answered = answered || message.Get("params.item.text").String() == codexTrackingAnswer
			}
			if predicate(message) {
				return message, nil
			}
		}
	}
	call := func(id int64, method string, params map[string]any) gjson.Result {
		require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
		message, err := readUntil(func(message gjson.Result) bool { return message.Get("id").Int() == id })
		require.NoError(t, err, "waiting for %s app-server reply", method)
		require.True(t, message.Get("result").Exists(), "%s app-server error: %s", method, message.Get("error.message").String())
		return message.Get("result")
	}
	call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "sub2api-loopback", "version": "1.0.0"}})
	require.NoError(t, encoder.Encode(map[string]string{"jsonrpc": "2.0", "method": "initialized"}))
	thread := call(2, "thread/start", map[string]any{"cwd": cmd.Dir, "ephemeral": true}).Get("thread.id").String()
	require.NotEmpty(t, thread)
	call(3, "turn/start", map[string]any{"threadId": thread, "input": []map[string]string{{"type": "text", "text": codexTrackingPrompt}}})
	message, err := readUntil(func(message gjson.Result) bool { return message.Get("method").String() == "turn/completed" })
	return answered, message.Get("params.turn"), err
}

func codexTrackingEvents(generate, reject bool) []string {
	responseID := "resp_warmup"
	item := ""
	if generate {
		if reject {
			return []string{`{"type":"error","error":{"type":"invalid_request_error","code":"project_spend_limit_exceeded","message":"Fixture spend limit"}}`}
		}
		responseID = "resp_generation"
		item = fmt.Sprintf(`{"id":"msg_fixture","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":%q,"annotations":[]}]}`, codexTrackingAnswer)
	}
	response := fmt.Sprintf(`"id":%q,"object":"response","created_at":1,"model":%q`, responseID, codexTrackingModel)
	events := []string{`{"type":"response.created","response":{` + response + `,"status":"in_progress","output":[]}}`}
	if item != "" {
		events = append(events, `{"type":"response.output_item.done","item":`+item+`,"output_index":0}`)
	}
	return append(events, `{"type":"response.completed","response":{`+response+`,"status":"completed","output":[`+item+`],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
}

func newCodexTrackingGateway(t *testing.T, upstreamURL, mode string, nonWebSocket *atomic.Int32) *httptest.Server {
	t.Helper()
	accountRepo := &openAIWSUsageHandlerAccountRepoStub{account: service.Account{
		ID: 1, Name: "codex-ws-fixture", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": codexTrackingKey, "base_url": upstreamURL},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_enabled": true,
			"openai_apikey_responses_websockets_v2_mode":    mode,
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
	router.GET("/v1/responses", func(c *gin.Context) {
		if !strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			nonWebSocket.Add(1)
			c.Status(http.StatusMethodNotAllowed)
			return
		}
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: key.User.ID, Concurrency: 1})
		h.ResponsesWebSocket(c)
	})
	router.NoRoute(func(c *gin.Context) {
		nonWebSocket.Add(1)
		c.Status(http.StatusMethodNotAllowed)
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

func TestOpenAIResponsesWebSocket_UpstreamMessageTooBigDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeDedicated} {
		t.Run(mode, func(t *testing.T) {
			var requests, nonWebSocket atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				if _, _, err := conn.Read(ctx); err == nil {
					requests.Add(1)
					_ = conn.Close(coderws.StatusMessageTooBig, "fixture request limit")
				}
			}))
			t.Cleanup(upstream.Close)
			gateway := newCodexTrackingGateway(t, upstream.URL, mode, &nonWebSocket)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6.1-sol","input":[]}`)))
			_, _, err = conn.Read(ctx)
			require.Equal(t, coderws.StatusMessageTooBig, coderws.CloseStatus(err), "client read error: %v", err)
			require.EqualValues(t, 1, requests.Load(), "a rejected turn must not be replayed")
			require.Zero(t, nonWebSocket.Load())
		})
	}
}
