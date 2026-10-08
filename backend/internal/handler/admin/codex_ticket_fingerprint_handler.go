package admin

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func codexTicketAccountID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return 0, false
	}
	return id, true
}

func codexTicketError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrCodexTicketBusy):
		response.ErrorWithDetails(c, http.StatusConflict, "Ticket attempt already running", "CODEX_TICKET_BUSY", nil)
	case errors.Is(err, service.ErrCodexTicketNoProxy):
		response.ErrorWithDetails(c, http.StatusUnprocessableEntity, "No available ticket proxy", "CODEX_TICKET_NO_PROXY", nil)
	case errors.Is(err, service.ErrCodexTicketModel):
		response.ErrorWithDetails(c, http.StatusBadRequest, "Unsupported ticket model", "CODEX_TICKET_MODEL", nil)
	case errors.Is(err, service.ErrCodexTicketRateLimited):
		response.ErrorWithDetails(c, http.StatusConflict, "Account or model is rate limited", "CODEX_TICKET_RATE_LIMITED", nil)
	case errors.Is(err, service.ErrCodexTicketUnavailable):
		response.ErrorWithDetails(c, http.StatusUnprocessableEntity, "Ticket harvesting disabled or account ineligible", "CODEX_TICKET_UNAVAILABLE", nil)
	default:
		response.ErrorFrom(c, err)
	}
}

// GetCodexTicketHistory returns the persisted ModelTrace diagnostic attempts
// for one account (success + failed), newest first.
func (h *AccountHandler) GetCodexTicketHistory(c *gin.Context) {
	id, ok := codexTicketAccountID(c)
	if !ok {
		return
	}
	model := c.Query("model")
	filter := c.DefaultQuery("filter", "all")
	if filter != "all" && filter != "success" {
		response.BadRequest(c, "filter must be all or success")
		return
	}
	page, size := response.ParsePagination(c)
	if size > 100 {
		size = 100
	}
	if h.codexTicketAttempts == nil {
		codexTicketError(c, service.ErrCodexTicketUnavailable)
		return
	}
	rows, total, err := h.codexTicketAttempts.List(c.Request.Context(), id, model, filter == "success", page, size)
	if err != nil {
		codexTicketError(c, err)
		return
	}
	response.Success(c, gin.H{"items": rows, "total": total, "page": page, "page_size": size})
}

// GetCodexFingerprintVersion reports the active ModelTrace bank commit and
// the GPT models the bank can attribute.
func (h *AccountHandler) GetCodexFingerprintVersion(c *gin.Context) {
	models, err := service.ModelTraceGPTModels()
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"commit": service.ModelTraceBankCommit(), "models": models})
}

// RefreshCodexFingerprint pulls the latest ModelTrace bank from upstream.
func (h *AccountHandler) RefreshCodexFingerprint(c *gin.Context) {
	if h.codexTicketSettings == nil {
		codexTicketError(c, service.ErrCodexTicketUnavailable)
		return
	}
	commit, err := h.codexTicketSettings.RefreshModelTraceBank(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	models, err := service.ModelTraceGPTModels()
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"commit": commit, "models": models})
}
