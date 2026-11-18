//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCodexCLI170OAuthWebSocketLargeMessage(t *testing.T) {
	binaryPath := os.Getenv("SUB2API_CODEX_SMOKE_BINARY")
	if binaryPath == "" {
		t.Skip("set SUB2API_CODEX_SMOKE_BINARY to Codex 0.170.0 to run this test")
	}
	version, err := exec.Command(binaryPath, "--version").Output()
	require.NoError(t, err)
	require.Equal(t, "codex-cli 0.170.0", strings.TrimSpace(string(version)))
	gin.SetMode(gin.TestMode)

	const mebibyte = 1 << 20
	const ingressLimit = 4 * mebibyte
	// Unicode keeps the prompt below the CLI character limit while crossing a 2 MiB wire boundary.
	const term = "你好 "
	prompt := codexOAuthWSSmokePrompt + "\n" + strings.Repeat(term, 2*mebibyte/len(term)) + "end"
	type upstreamMessage struct {
		payload []byte
		frames  []codexOAuthWSWireFrame
	}
	messages := make(chan upstreamMessage, 8)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer sk-test" {
			t.Errorf("unexpected upstream request %s %s or credentials", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Codex-WebSocket-Max-Message-Bytes", strconv.Itoa(ingressLimit))
		// Leave compression disabled so frame lengths measure the JSON itself.
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(ingressLimit)
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		wire := r.Context().Value(codexOAuthWSWireConnKey{}).(*codexOAuthWSWireConn)
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			select {
			case messages <- upstreamMessage{payload: payload, frames: wire.takeDataFrames()}:
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
	upstream.Listener = codexOAuthWSWireListener{Listener: upstream.Listener}
	upstream.Config.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, codexOAuthWSWireConnKey{}, conn)
	}
	upstream.Start()
	defer upstream.Close()

	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.ClientReadLimitBytes = ingressLimit
	account := passthroughLifecycleAccount()
	account.Type = AccountTypeOAuth
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool}
	account.Credentials = map[string]any{"access_token": "sk-test"}
	svc := newPassthroughLifecycleService(cfg, nil)
	defer svc.CloseOpenAIWSPool()
	outboundHTTP := &httpUpstreamRecorder{}
	svc.httpUpstream = outboundHTTP
	svc.getOpenAIWSConnPool().setClientDialerForTest(codexOAuthWSSmokeDialer{loopback: "ws" + strings.TrimPrefix(upstream.URL, "http") + "/v1/responses"})
	proxyErrors := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/responses" {
			t.Errorf("unexpected gateway request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Codex-WebSocket-Max-Message-Bytes", strconv.FormatInt(ResolveOpenAIWSClientReadLimitBytes(cfg), 10))
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(ResolveOpenAIWSClientReadLimitBytes(cfg))
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		_, payload, err := ReadOpenAIWSClientMessage(r.Context(), conn, 10*time.Second, coderws.StatusPolicyViolation, "missing response.create")
		if err == nil {
			err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, account.GetOpenAIAccessToken(), payload, nil)
		}
		proxyErrors <- err
	}))
	defer gateway.Close()

	for _, baseURL := range []string{upstream.URL, gateway.URL} {
		output := runCodexOAuthWSLargeSmokeCLI(t, binaryPath, baseURL, prompt)
		require.Contains(t, output, `"type":"turn.completed"`)
		require.Contains(t, output, `"text":"LOOPBACK_OK"`)
		require.Len(t, messages, 2, "each turn must send one warmup and one generation")
		warmup, generation := <-messages, <-messages
		require.Equal(t, "false", gjson.GetBytes(warmup.payload, "generate").Raw)
		require.NotEqual(t, "false", gjson.GetBytes(generation.payload, "generate").Raw)
		require.Equal(t, "response.create", gjson.GetBytes(generation.payload, "type").String())
		require.Greater(t, len(generation.payload), 2*mebibyte)
		require.Less(t, len(generation.payload), ingressLimit)
		require.True(t, bytes.Contains(generation.payload, []byte(strings.TrimPrefix(prompt, codexOAuthWSSmokePrompt+"\n"))), "upstream must receive the full prompt")
		var frameBytes int
		for i, frame := range generation.frames {
			frameBytes += frame.length
			if i == 0 {
				require.Equal(t, byte(1), frame.opcode, "message begins with text frame")
			} else {
				require.Zero(t, frame.opcode, "remaining data frames must be continuations")
			}
			require.Equal(t, i == len(generation.frames)-1, frame.final)
		}
		require.Equal(t, len(generation.payload), frameBytes)
		if baseURL == upstream.URL {
			require.Len(t, generation.frames, 3)
			require.Equal(t, mebibyte, generation.frames[0].length)
			require.Equal(t, mebibyte, generation.frames[1].length)
			t.Logf("direct Codex generation: %d bytes in frames %+v", len(generation.payload), generation.frames)
		} else {
			t.Logf("OAuth ctx_pool generation: %d bytes in frames %+v", len(generation.payload), generation.frames)
		}
	}
	select {
	case proxyErr := <-proxyErrors:
		require.NoError(t, proxyErr)
	case <-time.After(5 * time.Second):
		t.Fatal("OAuth WebSocket gateway did not finish")
	}
	require.Empty(t, outboundHTTP.requests)
}

func runCodexOAuthWSLargeSmokeCLI(t *testing.T, binaryPath, baseURL, prompt string) string {
	t.Helper()
	dir := t.TempDir()
	config := `model = "` + codexOAuthWSSmokeModel + `"
model_provider = "fixture"
check_for_update_on_startup = false
[model_providers.fixture]
name = "Loopback fixture"
base_url = "` + baseURL + `/v1"
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 1
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.toml"), []byte(config), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, "exec", "--skip-git-repo-check", "--ephemeral", "--json", "-")
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CODEX_HOME=" + dir, "TMPDIR=" + t.TempDir(), "SUB2API_TEST_TOKEN=sk-test"}
	cmd.Stdin = strings.NewReader(prompt)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	require.NoError(t, err, "CLI stderr:\n%s\nCLI stdout:\n%s", &stderr, stdout)
	return string(stdout)
}

type codexOAuthWSWireConnKey struct{}

type codexOAuthWSWireListener struct{ net.Listener }

func (l codexOAuthWSWireListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &codexOAuthWSWireConn{Conn: conn}, nil
}

type codexOAuthWSWireConn struct {
	net.Conn
	wire              bytes.Buffer
	handshakeConsumed bool
}

func (c *codexOAuthWSWireConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.wire.Write(p[:n])
	return n, err
}

type codexOAuthWSWireFrame struct {
	length int
	opcode byte
	final  bool
}

func (c *codexOAuthWSWireConn) takeDataFrames() []codexOAuthWSWireFrame {
	if !c.handshakeConsumed {
		handshakeEnd := bytes.Index(c.wire.Bytes(), []byte("\r\n\r\n"))
		if handshakeEnd < 0 {
			return nil
		}
		c.wire.Next(handshakeEnd + 4)
		c.handshakeConsumed = true
	}
	var frames []codexOAuthWSWireFrame
	for data := c.wire.Bytes(); len(data) >= 2; data = c.wire.Bytes() {
		header, length := 2, int(data[1]&0x7f)
		switch length {
		case 126:
			if len(data) < 4 {
				return frames
			}
			header, length = 4, int(binary.BigEndian.Uint16(data[2:4]))
		case 127:
			if len(data) < 10 {
				return frames
			}
			header, length = 10, int(binary.BigEndian.Uint64(data[2:10]))
		}
		if data[1]&0x80 != 0 {
			header += 4
		}
		if length > len(data)-header {
			return frames
		}
		opcode := data[0] & 0xf
		if opcode < 8 {
			frames = append(frames, codexOAuthWSWireFrame{length: length, opcode: opcode, final: data[0]&0x80 != 0})
		}
		c.wire.Next(header + length)
	}
	return frames
}
