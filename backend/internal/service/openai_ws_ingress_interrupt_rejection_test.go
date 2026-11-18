package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSOAuthCtxPoolInterruptRejectedDrainAdmissionFailureRetiresLease(t *testing.T) {
	upstream := &interruptTestConn{newStagedPassthroughConn()}
	svc, account, _ := newInterruptTestService(t, upstream)
	admissionErr := errors.New("deny the continuation")
	server, done := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{BeforeTurn: func(turn int) error {
			if turn == 2 {
				return admissionErr
			}
			return nil
		}}
	})
	defer server.Close()
	client := dialPassthroughLifecycleClientWithPayload(t, server, interruptDrainTestCreate)
	defer client.CloseNow()

	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestCreated)
	requireInterruptDownstream(t, client, interruptTestCreated)
	writeInterruptTestClient(t, client, interruptTestControl)
	requirePassthroughUpstreamWrite(t, upstream.stagedPassthroughConn, time.Second)
	upstream.Send(interruptTestReject)
	requireInterruptDownstream(t, client, interruptTestReject)
	upstream.Send(interruptTestComplete)
	requireInterruptDownstream(t, client, interruptTestComplete)
	writeInterruptTestClient(t, client, interruptDrainTestContinuation)

	select {
	case err := <-done:
		require.ErrorIs(t, err, admissionErr)
	case <-time.After(time.Second):
		t.Fatal("the denied continuation did not finish the session")
	}
	select {
	case <-upstream.closed:
	default:
		t.Fatal("the denied steering continuation returned a socket with a pending reader")
	}
	require.Empty(t, upstream.writes, "the admission failure must not write the continuation upstream")
}
