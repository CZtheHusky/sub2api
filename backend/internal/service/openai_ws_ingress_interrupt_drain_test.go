package service

import (
	"context"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	interruptDrainTestCreate       = `{"type":"response.create","model":"gpt-5.1","stream":true,"client_metadata":{"turn_id":"turn_steering","ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`
	interruptDrainTestContinuation = `{"type":"response.create","model":"gpt-5.1","stream":true,"previous_response_id":"resp_active","client_metadata":{"turn_id":"turn_steering","ws_request_header_x_openai_internal_codex_responses_lite":"true"},"input":[{"role":"user","content":"continue"}]}`
)

func TestOpenAIWSOAuthCtxPoolInterruptDrainedAcknowledgementStaysWithLease(t *testing.T) {
	const (
		nextCreated  = `{"type":"response.created","response":{"id":"resp_next","status":"in_progress"}}`
		nextControl  = `{"type":"response.interrupt","response_id":"resp_next","mode":"discard_partial_items"}`
		nextAck      = `{"type":"response.interrupt.accepted","response_id":"resp_next"}`
		nextComplete = `{"type":"response.completed","response":{"id":"resp_next","status":"completed"}}`
	)
	for _, tc := range []struct {
		name          string
		lateAck       string
		cancelNext    bool
		terminalFirst bool
	}{
		{name: "late acceptance while new response is generating", lateAck: interruptTestAck},
		{name: "late acceptance while new cancellation awaits terminal", lateAck: interruptTestAck, cancelNext: true},
		{name: "late rejection while awaiting new ACK after terminal", lateAck: interruptTestReject, cancelNext: true, terminalFirst: true},
		{name: "optional ACK still unresolved when session releases its lease"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			replacement := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, dialer := newInterruptTestService(t, upstream, replacement)
			server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
			defer server.Close()
			client := dialPassthroughLifecycleClientWithPayload(t, server, interruptDrainTestCreate)
			defer func() { _ = client.CloseNow() }()
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			upstream.Send(interruptTestCreated)
			requireInterruptDownstream(t, client, interruptTestCreated)
			writeInterruptTestClient(t, client, interruptTestControl)
			require.JSONEq(t, interruptTestControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))
			upstream.Send(interruptTestComplete)
			requireInterruptDownstream(t, client, interruptTestComplete)

			writeInterruptTestClient(t, client, interruptDrainTestContinuation)
			create := requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			require.Equal(t, "resp_active", gjson.GetBytes(create, "previous_response_id").String())
			upstream.Send(nextCreated)
			requireInterruptDownstream(t, client, nextCreated)
			if tc.cancelNext {
				writeInterruptTestClient(t, client, nextControl)
				require.JSONEq(t, nextControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))
				if tc.terminalFirst {
					upstream.Send(nextComplete)
					requireInterruptDownstream(t, client, nextComplete)
				}
			}
			if tc.lateAck != "" {
				upstream.Send(tc.lateAck)
			}
			if tc.cancelNext {
				upstream.Send(nextAck)
				// A drained response's acknowledgement must neither reach the
				// client nor close the cancellation handshake for the new ID.
				requireInterruptDownstream(t, client, nextAck)
			}
			if !tc.terminalFirst {
				upstream.Send(nextComplete)
				requireInterruptDownstream(t, client, nextComplete)
			}
			require.NoError(t, client.Close(coderws.StatusNormalClosure, ""))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("the client session did not release the lease")
			}

			// Identical first requests make this an eligible pool borrower.
			// An unresolved late ACK requires a fresh upstream connection.
			borrowedUpstream := upstream
			wantDialCount := 1
			if tc.lateAck == "" {
				select {
				case <-upstream.closed:
				default:
					t.Fatal("an unresolved steering acknowledgement was released to the pool")
				}
				borrowedUpstream = replacement
				wantDialCount = 2
			}
			borrower := dialPassthroughLifecycleClientWithPayload(t, server, interruptDrainTestCreate)
			defer func() { _ = borrower.CloseNow() }()
			borrowed := requirePassthroughUpstreamWrite(t, borrowedUpstream.stagedPassthroughConn, time.Second)
			require.Equal(t, "response.create", gjson.GetBytes(borrowed, "type").String())
			require.Equal(t, wantDialCount, dialer.DialCount())
			borrowedUpstream.Send(interruptTestComplete)
			requireInterruptDownstream(t, borrower, interruptTestComplete)
			require.NoError(t, borrower.Close(coderws.StatusNormalClosure, ""))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("the next pool borrower did not release the lease")
			}
		})
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptDrainRejectsUnrelatedCreateWithoutAck(t *testing.T) {
	for _, tc := range []struct {
		path  string
		value string
	}{
		{path: "previous_response_id", value: "resp_other"},
		{path: "client_metadata.turn_id", value: "turn_other"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			queued, err := sjson.Set(interruptDrainTestContinuation, tc.path, tc.value)
			require.NoError(t, err)
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, dialer := newInterruptTestService(t, upstream)
			server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
			defer server.Close()
			client := dialPassthroughLifecycleClientWithPayload(t, server, interruptDrainTestCreate)
			defer func() { _ = client.CloseNow() }()
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			upstream.Send(interruptTestCreated)
			requireInterruptDownstream(t, client, interruptTestCreated)
			started := time.Now()
			writeInterruptTestClient(t, client, interruptTestControl)
			require.JSONEq(t, interruptTestControl, string(requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)))

			upstream.Send(interruptTestComplete)
			requireInterruptDownstream(t, client, interruptTestComplete)
			writeInterruptTestClient(t, client, queued)
			// Leave the client connected and withhold the current ACK. Either
			// mismatched ID must prevent the create from being forwarded.
			select {
			case frame := <-upstream.writes:
				t.Fatalf("unrelated create bypassed the cancellation handshake: %s", frame)
			case err := <-done:
				require.ErrorIs(t, err, errOpenAIWSInterruptTimeout)
			case <-time.After(time.Until(started.Add(2 * openAIWSInterruptTimeout))):
				t.Fatal("the unrelated Lite create left the cancellation lease open")
			}
			require.GreaterOrEqual(t, time.Since(started), 750*time.Millisecond)
			require.Empty(t, upstream.writes)
			require.Equal(t, 1, dialer.DialCount())
			select {
			case <-upstream.closed:
			default:
				t.Fatal("the missing acknowledgement did not retire the lease")
			}
			_, err = readPassthroughLifecycleFrame(t, client, time.Second)
			require.Error(t, err)
			require.NotErrorIs(t, err, context.DeadlineExceeded, "gateway must close the client on timeout")
		})
	}
}

func TestOpenAIWSOAuthCtxPoolInterruptContinuationGetsFreshReadTimeout(t *testing.T) {
	for _, supplyTerminal := range []bool{true, false} {
		name := "missing first frame times out"
		if supplyTerminal {
			name = "first frame within new turn budget"
		}
		t.Run(name, func(t *testing.T) {
			upstream := &interruptTestConn{newStagedPassthroughConn()}
			svc, account, _ := newInterruptTestService(t, upstream)
			svc.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 1
			server, done := startPassthroughLifecycleServer(t, context.Background(), svc, account)
			defer server.Close()
			client := dialPassthroughLifecycleClientWithPayload(t, server, interruptDrainTestCreate)
			defer client.CloseNow()
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			upstream.Send(interruptTestCreated)
			requireInterruptDownstream(t, client, interruptTestCreated)
			writeInterruptTestClient(t, client, interruptTestControl)
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			upstream.Send(interruptTestComplete)
			requireInterruptDownstream(t, client, interruptTestComplete)

			// Spend part of the old read budget before the next create.
			time.Sleep(650 * time.Millisecond)
			writeInterruptTestClient(t, client, interruptDrainTestContinuation)
			requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
			select {
			case <-upstream.closed:
				t.Fatal("the old ACK read deadline cut short the continuation")
			case <-time.After(650 * time.Millisecond):
			}
			if supplyTerminal {
				const next = `{"type":"response.completed","response":{"id":"resp_next","status":"completed"}}`
				upstream.Send(next)
				requireInterruptDownstream(t, client, next)
				return
			}
			// The handoff also has to retain the ordinary first-frame timeout.
			select {
			case <-upstream.closed:
			case <-time.After(time.Second):
				t.Fatal("continuation with no upstream frame did not time out")
			}
			_ = client.CloseNow()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(2 * time.Second):
				t.Fatal("gateway did not finish the timed out turn")
			}
		})
	}
}
