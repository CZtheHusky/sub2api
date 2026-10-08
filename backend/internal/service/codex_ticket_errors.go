package service

import "errors"

// Codex ticket diagnostic sentinels (ported from sub4api harvest chain;
// harvesting itself is not ported — these are shared by handler error mapping).
var (
	ErrCodexTicketUnavailable = errors.New("codex ticket harvesting unavailable")
	ErrCodexTicketBusy        = errors.New("codex ticket attempt already running")
	ErrCodexTicketNoProxy     = errors.New("no available ticket proxy")
	ErrCodexTicketModel       = errors.New("unsupported ticket model")
	ErrCodexTicketRateLimited = errors.New("account or model is rate limited")
)
