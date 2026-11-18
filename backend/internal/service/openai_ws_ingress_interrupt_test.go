package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type interruptTestConn struct{ *stagedPassthroughConn }

func (c *interruptTestConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, payload)
}

func newInterruptTestService(t *testing.T, upstream ...openAIWSClientConn) (*OpenAIGatewayService, *Account, *openAIWSQueueDialer) {
	t.Helper()
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 6
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 5
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	account := passthroughLifecycleAccount()
	account.Type = AccountTypeOAuth
	account.Credentials = map[string]any{"access_token": "sk-test"}
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool}
	svc := newPassthroughLifecycleService(cfg, nil)
	dialer := &openAIWSQueueDialer{conns: upstream}
	svc.openaiWSPool = newOpenAIWSConnPool(cfg)
	svc.openaiWSPool.setClientDialerForTest(dialer)
	t.Cleanup(svc.CloseOpenAIWSPool)
	return svc, account, dialer
}

const (
	interruptTestCreated  = `{"type":"response.created","response":{"id":"resp_active","status":"in_progress"}}`
	interruptTestComplete = `{"type":"response.completed","response":{"id":"resp_active","status":"completed","usage":{"input_tokens":11,"output_tokens":2}}}`
	interruptTestAck      = `{"type":"response.interrupt.accepted","response_id":"resp_active"}`
	interruptTestReject   = `{"type":"response.interrupt.failed","response_id":"resp_active"}`
	interruptTestMetadata = `{"type":"codex.response.metadata","headers":{"x-models-etag":"interrupt-smoke"}}`
	interruptTestControl  = `{"type":"response.interrupt","response_id":"resp_active","mode":"discard_partial_items"}`
)

func requireInterruptDownstream(t *testing.T, conn *coderws.Conn, want string) {
	t.Helper()
	payload, err := readPassthroughLifecycleFrame(t, conn, 2*time.Second)
	require.NoError(t, err)
	require.JSONEq(t, want, string(payload))
}

func writeInterruptTestClient(t *testing.T, client *coderws.Conn, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(payload)))
}

func TestOpenAIWSOAuthCtxPoolInterruptStaysOnActiveResponse(t *testing.T) {
	for _, ackFirst := range []bool{true, false} {
		name := "terminal first"
		if ackFirst {
			name = "ack first"
		}
		t.Run(name, func(t *testing.T) {
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, dialer := newInterruptTestService(t, upstream)
			server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
			defer server.Close()
			client := dialPassthroughLifecycleClient(t, server)
			defer func() { _ = client.CloseNow() }()
			create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
			upstream.Send(interruptTestCreated)
			requireInterruptDownstream(t, client, interruptTestCreated)

			writeInterruptTestClient(t, client, `{"type":"response.interrupt","response_id":"resp_other","mode":"discard_partial_items"}`)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_other"}`)
			writeInterruptTestClient(t, client, interruptTestControl+" invalid")
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_active"}`)
			writeInterruptTestClient(t, client, `{"type":"response.interrupt","response_id":123,"mode":"discard_partial_items"}`)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"123"}`)
			writeInterruptTestClient(t, client, `{"type":"response.interrupt","response_id":"resp_active","mode":["discard_partial_items"]}`)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_active"}`)
			writeInterruptTestClient(t, client, `{"type":"response.interrupt","response_id":"resp_active","response_id":"resp_other","mode":"discard_partial_items"}`)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_active"}`)
			writeInterruptTestClient(t, client, interruptTestControl)
			control := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			require.JSONEq(t, interruptTestControl, string(control))
			writeInterruptTestClient(t, client, interruptTestControl)
			writeInterruptTestClient(t, client, `{"type":"response.interrupt","response_id":"resp_other","mode":"discard_partial_items"}`)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_other"}`)

			const secondCreate = `{"type":"response.create","model":"gpt-5.1","stream":true,"input":[{"role":"user","content":"next"}]}`
			if ackFirst {
				upstream.Send(interruptTestAck)
				requireInterruptDownstream(t, client, interruptTestAck)
				upstream.Send(interruptTestComplete)
				requireInterruptDownstream(t, client, interruptTestComplete)
				writeInterruptTestClient(t, client, secondCreate)
			} else {
				upstream.Send(interruptTestComplete)
				requireInterruptDownstream(t, client, interruptTestComplete)
				upstream.Send(interruptTestMetadata)
				requireInterruptDownstream(t, client, interruptTestMetadata)
				writeInterruptTestClient(t, client, secondCreate)
				select {
				case frame := <-upstream.writes:
					t.Fatalf("unexpected upstream frame before ACK: %s", frame)
				case <-time.After(40 * time.Millisecond):
				}
				upstream.Send(interruptTestAck)
				requireInterruptDownstream(t, client, interruptTestAck)
			}
			create = requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
			require.Equal(t, "next", gjson.GetBytes(create, "input.0.content").String())
			const nextCreated = `{"type":"response.created","response":{"id":"resp_next","status":"in_progress"}}`
			upstream.Send(nextCreated)
			requireInterruptDownstream(t, client, nextCreated)
			writeInterruptTestClient(t, client, interruptTestControl)
			requireInterruptDownstream(t, client, `{"type":"response.interrupt.failed","response_id":"resp_active"}`)
			const nextComplete = `{"type":"response.completed","response":{"id":"resp_next","status":"completed"}}`
			upstream.Send(nextComplete)
			requireInterruptDownstream(t, client, nextComplete)
			require.Equal(t, 1, dialer.DialCount())
			require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("client close did not finish the session")
			}
		})
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptRejectedDiscardsLease(t *testing.T) {
	for _, tc := range []struct {
		name         string
		terminalSeen bool
	}{
		{name: "rejection first"},
		{name: "terminal first", terminalSeen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			replacement := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, dialer := newInterruptTestService(t, upstream, replacement)
			billed := make(chan *OpenAIForwardResult, 2)
			server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
					if err == nil {
						billed <- result
					}
				}}
			})
			defer server.Close()
			client := dialPassthroughLifecycleClient(t, server)
			defer func() { _ = client.CloseNow() }()
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			upstream.Send(interruptTestCreated)
			requireInterruptDownstream(t, client, interruptTestCreated)
			writeInterruptTestClient(t, client, interruptTestControl)
			require.JSONEq(t, interruptTestControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))

			if tc.terminalSeen {
				upstream.Send(interruptTestComplete)
				requireInterruptDownstream(t, client, interruptTestComplete)
			}
			upstream.Send(interruptTestReject)
			if !tc.terminalSeen {
				upstream.Send(interruptTestComplete)
			}
			requireInterruptDownstream(t, client, interruptTestReject)
			_, clientErr := readPassthroughLifecycleFrame(t, client, 2*time.Second)
			require.Equal(t, coderws.StatusInternalError, coderws.CloseStatus(clientErr))
			select {
			case err := <-done:
				require.ErrorIs(t, err, errOpenAIWSInterruptRejected)
			case <-time.After(2 * time.Second):
				t.Fatal("upstream rejection did not finish the session")
			}
			if tc.terminalSeen {
				require.Len(t, billed, 1)
				result := <-billed
				require.Equal(t, 11, result.Usage.InputTokens)
				require.Equal(t, 2, result.Usage.OutputTokens)
			} else {
				require.Empty(t, billed)
			}
			select {
			case <-upstream.closed:
			default:
				t.Fatal("rejected cancellation left the old lease open")
			}

			next := dialPassthroughLifecycleClient(t, server)
			defer func() { _ = next.CloseNow() }()
			create := requirePassthroughUpstreamWrite(t, replacement.stagedPassthroughConn, time.Second)
			require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
			require.Equal(t, 2, dialer.DialCount(), "a new session must dial after cancellation was rejected")
			replacement.Send(interruptTestComplete)
			requireInterruptDownstream(t, next, interruptTestComplete)
			require.NoError(t, next.Close(coderws.StatusNormalClosure, ""))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("client close did not finish the replacement session")
			}
		})
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptAcceptedWithoutTerminalDiscardsLease(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = client.CloseNow() }()
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestAck)
	requireInterruptDownstream(t, client, interruptTestAck)

	// Keep the client open so disconnect cleanup cannot release the lease.
	select {
	case <-upstream.closed:
	case <-time.After(2 * openAIWSInterruptTimeout):
		t.Fatal("accepted interrupt fell back to the ordinary upstream read timeout")
	}
	_, clientErr := readPassthroughLifecycleFrame(t, client, 2*time.Second)
	require.Equal(t, coderws.StatusInternalError, coderws.CloseStatus(clientErr))
	select {
	case err := <-done:
		require.ErrorIs(t, err, errOpenAIWSInterruptTimeout)
	case <-time.After(2 * time.Second):
		t.Fatal("interrupt timeout did not finish the session")
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptBackpressureRetainsTerminalUsage(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	billed := make(chan *OpenAIForwardResult, 2)
	server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err == nil {
				billed <- result
			}
		}}
	})
	defer server.Close()
	client := dialInterruptSlowReader(t, server, 4096)
	defer func() { _ = client.CloseNow() }()
	writeInterruptTestClient(t, client, `{"type":"response.create","model":"gpt-5.1","stream":true}`)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)

	// Keep the client open without reading a terminal frame larger than TCP can buffer.
	upstream.Send(strings.TrimSuffix(interruptTestComplete, "}") + `,"padding":"` + strings.Repeat("x", 8<<20) + `"}`)
	select {
	case <-upstream.closed:
	case <-time.After(2 * openAIWSInterruptTimeout):
		t.Fatal("the downstream terminal write held the upstream lease past the interrupt deadline")
	}
	_ = client.CloseNow()
	select {
	case err := <-done:
		require.ErrorIs(t, err, errOpenAIWSInterruptTimeout)
	case <-time.After(2 * time.Second):
		t.Fatal("terminal write timeout did not finish the session")
	}
	require.Len(t, billed, 1)
	result := <-billed
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
}

func TestOpenAIWSOAuthCtxPoolInterruptDrainAllowsSlowDownstream(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, dialer := newInterruptTestService(t, upstream)
	svc.cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 5
	server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
	defer server.Close()
	client := dialInterruptSlowReader(t, server, 128<<10)
	defer func() { _ = client.CloseNow() }()
	client.SetReadLimit(16 << 20)
	writeInterruptTestClient(t, client, interruptDrainTestCreate)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	require.JSONEq(t, interruptTestControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))

	// A steering client may take more than one second to read a terminal
	// before it knows which response ID to continue.
	terminal := strings.TrimSuffix(interruptTestComplete, "}") + `,"padding":"` + strings.Repeat("x", 8<<20) + `"}`
	upstream.Send(terminal)
	select {
	case <-upstream.closed:
		t.Fatal("the gateway applied the cancellation deadline to a Lite steering write")
	case <-time.After(openAIWSInterruptTimeout + 350*time.Millisecond):
	}
	payload, err := readPassthroughLifecycleFrame(t, client, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, len(terminal), len(payload))
	require.Equal(t, "resp_active", gjson.GetBytes(payload, "response.id").String())

	writeInterruptTestClient(t, client, interruptDrainTestContinuation)
	create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	require.Equal(t, "resp_active", gjson.GetBytes(create, "previous_response_id").String())
	require.Equal(t, 1, dialer.DialCount(), "steering must reuse the lease after the slow terminal write")
	upstream.Send(interruptTestAck)
	const nextComplete = `{"type":"response.completed","response":{"id":"resp_next","status":"completed"}}`
	upstream.Send(nextComplete)
	requireInterruptDownstream(t, client, nextComplete)
	require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("client close did not finish the steered session")
	}
}

func dialInterruptSlowReader(t *testing.T, server *httptest.Server, readBuffer int) *coderws.Conn {
	t.Helper()
	dialer := &net.Dialer{}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if err := conn.(*net.TCPConn).SetReadBuffer(readBuffer); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return conn, nil
	}}
	t.Cleanup(transport.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	require.NoError(t, err)
	return client
}

func TestOpenAIWSOAuthCtxPoolInterruptMissingAckDiscardsLease(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	billed := make(chan *OpenAIForwardResult, 2)
	server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err == nil {
				billed <- result
			}
		}}
	})
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = client.CloseNow() }()
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestComplete)
	requireInterruptDownstream(t, client, interruptTestComplete)
	upstream.Fail(io.EOF)
	_, clientErr := readPassthroughLifecycleFrame(t, client, 2*time.Second)
	require.Equal(t, coderws.StatusInternalError, coderws.CloseStatus(clientErr))
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(2 * time.Second):
		t.Fatal("missing ACK did not finish the session")
	}
	require.Len(t, billed, 1)
	result := <-billed
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	select {
	case <-upstream.closed:
	default:
		t.Fatal("incomplete interrupt left the upstream lease reusable")
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptMissingAckCannotStartQueuedTurn(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	billed := make(chan *OpenAIForwardResult, 2)
	server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err == nil {
				billed <- result
			}
		}}
	})
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = client.CloseNow() }()
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestComplete)
	requireInterruptDownstream(t, client, interruptTestComplete)
	writeInterruptTestClient(t, client, `{"type":"response.create","model":"gpt-5.1","stream":true}`)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(2 * openAIWSInterruptTimeout)
waitForTimeout:
	for {
		select {
		case <-ticker.C:
			select {
			case upstream.frames <- stagedPassthroughFrame{messageType: coderws.MessageText, payload: []byte(interruptTestMetadata)}:
			default:
			}
		case <-upstream.closed:
			break waitForTimeout
		case <-deadline:
			t.Fatal("metadata or a queued request extended the original interrupt deadline")
		}
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, errOpenAIWSInterruptTimeout)
	case <-time.After(3 * time.Second):
		t.Fatal("queued request blocked the interrupt deadline")
	}
	require.Len(t, billed, 1)
	require.Equal(t, 11, (<-billed).Usage.InputTokens)
	require.Empty(t, upstream.writes, "queued create must not pass an unacknowledged interrupt")
	select {
	case <-upstream.closed:
	default:
		t.Fatal("timed-out interrupt left the upstream lease reusable")
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptClientDisconnectDrainsThenDiscards(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = client.CloseNow() }()
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("client disconnect did not end the stalled upstream interrupt")
	}
	select {
	case <-upstream.closed:
	default:
		t.Fatal("pending interrupt left the upstream lease reusable")
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptKeepsPreOutputAccountHandoff(t *testing.T) {
	first := &interruptTestConn{newStagedPassthroughConn()}
	second := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, dialer := newInterruptTestService(t, first, second)
	nextAccount := *account
	nextAccount.ID++
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, payload, err := ReadOpenAIWSClientMessage(ctx, conn, time.Second, coderws.StatusPolicyViolation, "missing first create")
		if err != nil {
			done <- err
			return
		}
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ginCtx.Request = r
		err = svc.ProxyResponsesWebSocketFromClient(ctx, ginCtx, conn, account, "sk-test", payload, nil)
		var failoverErr *UpstreamFailoverError
		if !errors.As(err, &failoverErr) || failoverErr.StatusCode != http.StatusTooManyRequests {
			done <- errors.New("first upstream attempt did not preserve rate-limit failover")
			return
		}
		done <- svc.ProxyResponsesWebSocketFromClient(ctx, ginCtx, conn, &nextAccount, "sk-test", payload, nil)
	}))
	defer server.Close()
	client := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = client.CloseNow() }()
	requirePassthroughUpstreamWrite(t, first.stagedPassthroughConn, time.Second)
	first.Send(`{"type":"error","error":{"code":"rate_limit_exceeded","type":"usage_limit_reached","message":"The usage limit has been reached"}}`)
	create := requirePassthroughUpstreamWrite(t, second.stagedPassthroughConn, time.Second)
	require.Equal(t, "response.create", gjson.GetBytes(create, "type").String())
	second.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	control := requirePassthroughUpstreamWrite(t, second.stagedPassthroughConn, time.Second)
	require.JSONEq(t, interruptTestControl, string(control))
	second.Send(interruptTestAck)
	second.Send(interruptTestComplete)
	requireInterruptDownstream(t, client, interruptTestAck)
	requireInterruptDownstream(t, client, interruptTestComplete)
	require.Equal(t, 2, dialer.DialCount())
	require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("client close did not finish the failover session")
	}
}
