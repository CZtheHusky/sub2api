package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

const (
	openAIWSInterruptRequested = "response.interrupt"
	openAIWSInterruptAccepted  = "response.interrupt.accepted"
	openAIWSInterruptFailed    = "response.interrupt.failed"
	openAIWSInterruptMode      = "discard_partial_items"
	// Codex 0.170 allows one second to drain a canceled response.
	openAIWSInterruptTimeout = time.Second
)

var (
	errOpenAIWSInterruptTimeout  = errors.New("upstream websocket interruption timed out")
	errOpenAIWSInterruptRejected = errors.New("upstream websocket interruption was rejected")
)

// openAIWSIngressInterrupt keeps at most one client read outstanding across
// turns. Only the forwarding loop consumes reads and writes control frames.
type openAIWSIngressInterrupt struct {
	ctx           context.Context
	client        *coderws.Conn
	idleTimeout   time.Duration
	idle          atomic.Bool
	idleStart     chan struct{}
	reading       chan openAIWSClientReadResult
	queued        *openAIWSClientReadResult
	drainDeadline time.Time

	// A steering continuation can arrive while the read for the preceding ACK
	// is in flight. The next turn on this lease consumes that very same read.
	pending      *openAIWSInterruptRead
	drainedLease *openAIWSConnLease
	drainedIDs   map[string]bool
}

type openAIWSInterruptRead struct {
	lease  *openAIWSConnLease
	result chan openAIWSClientReadResult
	cancel context.CancelFunc
}

var errOpenAIWSDrainContinued = errors.New("client continued the drained response")

type openAIWSInterruptTurn struct {
	liteTurnID     string
	terminalSeen   bool
	responseID     string
	awaitingAck    bool
	cancelDeadline time.Time
}

func (r *openAIWSIngressInterrupt) startRead() {
	if r.reading != nil || r.queued != nil || !r.drainDeadline.IsZero() {
		return
	}
	r.reading = make(chan openAIWSClientReadResult, 1)
	result := r.reading
	go func() {
		kind, body, err := readOpenAIWSClientMessageWithTimeoutStart(r.ctx, r.client, r.idleTimeout,
			coderws.StatusNormalClosure, "websocket idle timeout", r.idleStart, r.idle.Load)
		result <- openAIWSClientReadResult{messageType: kind, payload: body, err: err}
	}()
}

func (r *openAIWSIngressInterrupt) readClient() openAIWSClientReadResult {
	r.idle.Store(true)
	r.startRead()
	select {
	case r.idleStart <- struct{}{}:
	default:
	}
	var result openAIWSClientReadResult
	if r.queued != nil {
		result = *r.queued
		r.queued = nil
	} else {
		result = <-r.reading
		r.reading = nil
	}
	r.idle.Store(false)
	return result
}

func (t *openAIWSInterruptTurn) continues(queued *openAIWSClientReadResult) bool {
	if t.liteTurnID == "" || queued == nil || queued.err != nil {
		return false
	}
	fields := gjson.GetManyBytes(queued.payload, "type", "previous_response_id", "client_metadata.turn_id")
	return fields[0].Str == "response.create" && fields[1].Str == t.responseID && fields[2].Str == t.liteTurnID
}

// A late acknowledgement belongs to the previous response on this lease,
// never to a cancellation that may already be active on the following turn.
func (r *openAIWSIngressInterrupt) consumeDrainedAck(lease *openAIWSConnLease, kind string, body []byte) bool {
	if r.drainedLease != lease || (kind != openAIWSInterruptAccepted && kind != openAIWSInterruptFailed) {
		return false
	}
	fields, valid := openAIWSInterruptFields(body)
	id := fields["response_id"]
	if !valid || fields["type"] != kind || !r.drainedIDs[id] {
		return false
	}
	delete(r.drainedIDs, id)
	if len(r.drainedIDs) == 0 {
		r.drainedLease = nil
	}
	return true
}

func (r *openAIWSIngressInterrupt) releaseLease(lease *openAIWSConnLease) {
	if r.drainedLease == lease {
		// An optional ACK must never be delivered to another pool borrower.
		lease.abort()
		r.drainedIDs = nil
		r.drainedLease = nil
	}
	if pending := r.pending; pending != nil && pending.lease == lease {
		pending.cancel()
		<-pending.result
		r.pending = nil
	}
}

func (r *openAIWSIngressInterrupt) readFirst(lease *openAIWSConnLease, timeout time.Duration) ([]byte, error) {
	if r.pending != nil && r.pending.lease == lease {
		read := r.pending
		r.pending = nil
		defer read.cancel()
		result := <-read.result
		return result.payload, result.err
	}
	return lease.ReadMessageWithContextTimeout(r.ctx, timeout)
}

func (r *openAIWSIngressInterrupt) readUpstream(lease *openAIWSConnLease, turn *openAIWSInterruptTurn, readTimeout, writeTimeout time.Duration, writeClient func([]byte) error) ([]byte, error) {
	deadline, deadlineErr := r.readDeadline(turn)
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return nil, deadlineErr
	}
	r.startRead()
	readCtx, cancel := context.WithCancel(r.ctx)
	keepRead := false
	defer func() {
		if !keepRead {
			cancel()
		}
	}()
	upstream := make(chan openAIWSClientReadResult, 1)
	go func() {
		body, err := lease.ReadMessageWithContextTimeout(readCtx, readTimeout)
		upstream <- openAIWSClientReadResult{payload: body, err: err}
	}()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var timedOut <-chan time.Time
	resetDeadline := func() {
		deadline, deadlineErr = r.readDeadline(turn)
		if !deadline.IsZero() {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(time.Until(deadline))
			timedOut = timer.C
		}
	}
	resetDeadline()
	endRead := func(err error) ([]byte, error) {
		cancel()
		<-upstream
		return nil, err
	}
	for {
		if turn.terminalSeen && turn.continues(r.queued) {
			keepRead = true
			r.pending = &openAIWSInterruptRead{lease: lease, result: upstream, cancel: cancel}
			r.drainedLease = lease
			if r.drainedIDs == nil {
				r.drainedIDs = make(map[string]bool)
			}
			r.drainedIDs[turn.responseID] = true
			return nil, errOpenAIWSDrainContinued
		}
		select {
		case result := <-upstream:
			return result.payload, result.err
		case <-timedOut:
			return endRead(deadlineErr)
		case result := <-r.reading:
			r.reading = nil
			if !r.drainDeadline.IsZero() {
				continue
			}
			if result.err != nil {
				if !isOpenAIWSClientDisconnectError(result.err) {
					return endRead(result.err)
				}
				r.queued = &result
				if turn.liteTurnID != "" && !turn.cancelDeadline.IsZero() {
					return endRead(context.Canceled)
				}
				r.startDrain()
				resetDeadline()
				continue
			}
			if gjson.GetBytes(result.payload, "type").String() != openAIWSInterruptRequested {
				r.queued = &result
				resetDeadline()
				continue
			}
			id, valid := parseOpenAIWSInterrupt(result.payload)
			var err error
			if !valid || turn.responseID != id {
				err = writeOpenAIWSInterruptFailed(writeClient, id)
			} else if turn.cancelDeadline.IsZero() {
				turn.cancelDeadline = time.Now().Add(openAIWSInterruptTimeout)
				writeCtx, cancelWrite := context.WithDeadline(r.ctx, turn.cancelDeadline)
				err = lease.WriteJSONWithContextTimeout(writeCtx, map[string]string{
					"type": openAIWSInterruptRequested, "response_id": id, "mode": openAIWSInterruptMode,
				}, writeTimeout)
				cancelWrite()
				if err == nil {
					turn.awaitingAck = true
					resetDeadline()
				}
			}
			if err != nil {
				return endRead(fmt.Errorf("write websocket interrupt: %w", err))
			}
			r.startRead()
		}
	}
}

func parseOpenAIWSInterrupt(payload []byte) (string, bool) {
	id := gjson.GetBytes(payload, "response_id").String()
	fields, valid := openAIWSInterruptFields(payload)
	return id, valid && fields["response_id"] != "" && fields["type"] == openAIWSInterruptRequested && fields["mode"] == openAIWSInterruptMode
}

func openAIWSInterruptFields(payload []byte) (map[string]string, bool) {
	if !gjson.ValidBytes(payload) {
		return nil, false
	}
	fields := make(map[string]string, 3)
	valid := true
	gjson.ParseBytes(payload).ForEach(func(key, field gjson.Result) bool {
		name := key.String()
		switch name {
		case "type", "response_id", "mode":
			if _, duplicate := fields[name]; duplicate || field.Type != gjson.String {
				valid = false
				return false
			}
			fields[name] = field.String()
		}
		return true
	})
	return fields, valid
}

func openAIWSHasDuplicateMessageType(payload []byte) bool {
	count := 0
	gjson.ParseBytes(payload).ForEach(func(key, _ gjson.Result) bool {
		if key.String() == "type" {
			count++
		}
		return count < 2
	})
	return count > 1
}

func (r *openAIWSIngressInterrupt) readDeadline(turn *openAIWSInterruptTurn) (time.Time, error) {
	if !r.drainDeadline.IsZero() {
		if !turn.cancelDeadline.IsZero() && turn.cancelDeadline.Before(r.drainDeadline) {
			return turn.cancelDeadline, context.Canceled
		}
		return r.drainDeadline, context.Canceled
	}
	// Responses Lite sends identical control frames for Drain and Cancel. The
	// native client closes the socket on its Cancel deadline. Drain may take
	// longer and continue the same logical turn on the same connection.
	if !turn.cancelDeadline.IsZero() && (turn.liteTurnID == "" || (r.queued != nil && !turn.continues(r.queued))) {
		return turn.cancelDeadline, errOpenAIWSInterruptTimeout
	}
	return time.Time{}, nil
}

func (r *openAIWSIngressInterrupt) startDrain() {
	if r.drainDeadline.IsZero() {
		// Match the passthrough relay's grace period for recording usage.
		r.drainDeadline = time.Now().Add(1200 * time.Millisecond)
	}
}

func (t *openAIWSInterruptTurn) recordAck(kind string, payload []byte) error {
	if kind != openAIWSInterruptAccepted && kind != openAIWSInterruptFailed {
		return nil
	}
	fields, valid := openAIWSInterruptFields(payload)
	if !t.awaitingAck || !valid || fields["type"] != kind || fields["response_id"] != t.responseID {
		return errors.New("unexpected upstream websocket interruption acknowledgement")
	}
	t.awaitingAck = false
	return nil
}

func (r *openAIWSIngressInterrupt) finishTurn(lease *openAIWSConnLease, turn *openAIWSInterruptTurn, readTimeout, writeTimeout time.Duration, writeClient func([]byte) error) error {
	turn.terminalSeen = true
	for {
		payload, err := r.readUpstream(lease, turn, readTimeout, writeTimeout, writeClient)
		if errors.Is(err, errOpenAIWSDrainContinued) {
			turn.awaitingAck = false
			return nil
		}
		if err != nil {
			return err
		}
		if !r.drainDeadline.IsZero() {
			return context.Canceled
		}
		kind := gjson.GetBytes(payload, "type").String()
		if r.consumeDrainedAck(lease, kind, payload) {
			continue
		}
		switch kind {
		case openAIWSInterruptAccepted, openAIWSInterruptFailed:
			if err := turn.recordAck(kind, payload); err != nil {
				return err
			}
			if err := writeClient(payload); err != nil {
				return err
			}
			if kind == openAIWSInterruptFailed {
				return errOpenAIWSInterruptRejected
			}
			return nil
		case "codex.response.metadata", "codex.rate_limits", "responsesapi.websocket_timing":
			if err := writeClient(payload); err != nil {
				return err
			}
		case "error":
			if err := writeClient(payload); err != nil {
				return err
			}
			return errors.New("upstream websocket error while awaiting interruption acknowledgement")
		default:
			return fmt.Errorf("unexpected websocket event %q after terminal while awaiting interruption acknowledgement", kind)
		}
	}
}

func writeOpenAIWSInterruptFailed(write func([]byte) error, responseID string) error {
	payload, _ := json.Marshal(map[string]string{"type": openAIWSInterruptFailed, "response_id": responseID})
	return write(payload)
}

func (r *openAIWSIngressInterrupt) finish(err error) {
	if r.reading == nil {
		return
	}
	status, reason := coderws.StatusNormalClosure, "websocket session finished"
	if err != nil && !errors.Is(err, context.Canceled) {
		status, reason = coderws.StatusInternalError, "upstream websocket proxy failed"
	}
	var closeErr *OpenAIWSClientCloseError
	if errors.As(err, &closeErr) {
		status, reason = closeErr.StatusCode(), closeErr.Reason()
	}
	_ = r.client.Close(status, reason)
	_ = r.client.CloseNow()
	<-r.reading
	r.reading = nil
}
