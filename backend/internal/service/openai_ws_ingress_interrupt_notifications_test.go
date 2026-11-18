package service

import (
	"context"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSOAuthCtxPoolInterruptTerminalTiming(t *testing.T) {
	const (
		interrupted = `{"type":"response.incomplete","response":{"id":"resp_active","status":"incomplete","incomplete_details":{"reason":"interrupted"},"usage":{"input_tokens":11,"output_tokens":2}}}`
		timing      = `{"type":"responsesapi.websocket_timing","response_id":"resp_active","timing":{"total_ms":73}}`
		nextCreate  = `{"type":"response.create","model":"gpt-5.1","stream":true,"input":[{"role":"user","content":"next turn"}]}`
		nextDone    = `{"type":"response.completed","response":{"id":"resp_next","status":"completed"}}`
	)
	for _, tc := range []struct {
		name     string
		terminal string
		ack      bool
	}{
		{name: "completed then timing then ACK", terminal: interruptTestComplete, ack: true},
		{name: "interrupted then timing then ACK", terminal: interrupted, ack: true},
		{name: "interrupted then timing without ACK", terminal: interrupted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, dialer := newInterruptTestService(t, upstream)
			billed := make(chan *OpenAIForwardResult, 2)
			server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{AfterTurn: func(turn int, result *OpenAIForwardResult, err error) {
					if turn == 1 && err == nil {
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
			started := time.Now()
			writeInterruptTestClient(t, client, interruptTestControl)
			require.JSONEq(t, interruptTestControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))

			upstream.Send(tc.terminal)
			requireInterruptDownstream(t, client, tc.terminal)
			upstream.Send(timing)
			requireInterruptDownstream(t, client, timing)

			// A queued turn must be held behind the cancellation acknowledgement.
			writeInterruptTestClient(t, client, nextCreate)
			if tc.ack {
				upstream.Send(interruptTestAck)
				requireInterruptDownstream(t, client, interruptTestAck)
				create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
				require.Equal(t, "next turn", gjson.GetBytes(create, "input.0.content").String())
				require.Equal(t, 1, dialer.DialCount(), "an acknowledged interrupt permits reuse")
				upstream.Send(nextDone)
				requireInterruptDownstream(t, client, nextDone)
				require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
			} else {
				// If notifications restarted the wait, the lease would stay open
				// past the deadline while timing frames keep arriving.
				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				deadline := time.After(time.Until(started.Add(2 * openAIWSInterruptTimeout)))
			waitForTimeout:
				for {
					select {
					case <-ticker.C:
						select {
						case upstream.frames <- stagedPassthroughFrame{messageType: coderws.MessageText, payload: []byte(timing)}:
						default:
						}
					case <-upstream.closed:
						break waitForTimeout
					case <-deadline:
						t.Fatal("timing notifications extended the interrupt deadline")
					}
				}
				require.GreaterOrEqual(t, time.Since(started), 750*time.Millisecond)
				require.Empty(t, upstream.writes, "a queued turn must not run without an ACK")
				_ = client.CloseNow()
			}
			select {
			case err := <-done:
				if tc.ack {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errOpenAIWSInterruptTimeout)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("interrupt session did not finish")
			}
			require.Len(t, billed, 1)
			result := <-billed
			require.Equal(t, 11, result.Usage.InputTokens)
			require.Equal(t, 2, result.Usage.OutputTokens)
		})
	}
}
