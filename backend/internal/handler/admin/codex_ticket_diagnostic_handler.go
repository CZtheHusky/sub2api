package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// SetCodexTicketDiagnosticRouter wires everything the ModelTrace diagnostic
// flow needs: the gateway router for in-process replays, the admin's API key
// service, the gateway service (probe context + request builder), settings
// (fingerprint bank refresh) and the attempt repository (history).
func (h *AccountHandler) SetCodexTicketDiagnosticRouter(router http.Handler, keys *service.APIKeyService, gateway *service.OpenAIGatewayService, settings *service.SettingService, attempts service.CodexTicketAttemptRepository) {
	h.codexTicketRouter = router
	h.codexTicketAPIKeys = keys
	h.codexTicketGateway = gateway
	h.codexTicketSettings = settings
	h.codexTicketAttempts = attempts
}

type codexDiagnosticItem struct {
	Model            string                      `json:"model"`
	Status           string                      `json:"status"`
	Reason           string                      `json:"reason,omitempty"`
	PredictedModel   string                      `json:"predicted_model,omitempty"`
	Probability      float64                     `json:"probability,omitempty"`
	ParsedCount      int                         `json:"parsed_number_count,omitempty"`
	HTTPStatus       int                         `json:"http_status,omitempty"`
	GatewayErrorCode string                      `json:"gateway_error_code,omitempty"`
	Harvest          *service.CodexTicketAttempt `json:"harvest,omitempty"`
}

func codexDiagnosticGatewayError(raw []byte) string {
	if len(raw) > 16<<10 {
		raw = raw[:16<<10]
	}
	for _, path := range []string{"reason", "error.code"} {
		code := gjson.GetBytes(raw, path).String()
		if code == "" || len(code) > 64 {
			continue
		}
		for _, character := range code {
			if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
				continue
			}
			code = ""
			break
		}
		if code != "" {
			return code
		}
	}
	return ""
}

func codexDiagnosticOutput(raw []byte) (string, bool) {
	completed := false
	streamed := false
	var output strings.Builder
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 2<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			continue
		}
		streamed = true
		switch gjson.Get(payload, "type").String() {
		case "response.output_text.delta":
			output.WriteString(gjson.Get(payload, "delta").String())
		case "response.completed":
			completed = true
			if output.Len() == 0 {
				gjson.Get(payload, "response.output").ForEach(func(_, message gjson.Result) bool {
					message.Get("content").ForEach(func(_, part gjson.Result) bool {
						if part.Get("type").String() == "output_text" {
							output.WriteString(part.Get("text").String())
						}
						return true
					})
					return true
				})
			}
		}
	}
	if streamed {
		return output.String(), completed && scanner.Err() == nil
	}
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil {
		return "", false
	}
	if gjson.GetBytes(raw, "status").String() != "completed" {
		return "", false
	}
	if text := gjson.GetBytes(raw, "output_text").String(); text != "" {
		return text, true
	}
	return gjson.GetBytes(raw, "output.0.content.0.text").String(), true
}

func (h *AccountHandler) DiagnoseCodexModels(c *gin.Context) {
	h.diagnoseCodexModels(c, service.NewModelTraceChallenge)
}

func (h *AccountHandler) diagnoseCodexModels(c *gin.Context, generateChallenge func() (service.ModelTraceChallenge, error)) {
	accountID, ok := codexTicketAccountID(c)
	if !ok {
		return
	}
	if h.codexTicketRouter == nil || h.codexTicketAPIKeys == nil || h.codexTicketGateway == nil {
		codexTicketError(c, service.ErrCodexTicketUnavailable)
		return
	}
	var input struct {
		APIKeyID int64    `json:"api_key_id"`
		Models   []string `json:"models"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.APIKeyID <= 0 || len(input.Models) == 0 || len(input.Models) > 32 {
		response.BadRequest(c, "Select an API key and up to 32 models")
		return
	}
	subject, authorized := middleware.GetAuthSubjectFromContext(c)
	if !authorized || subject.UserID <= 0 {
		response.ErrorWithDetails(c, http.StatusForbidden, "Admin user required", "FORBIDDEN", nil)
		return
	}
	key, err := h.codexTicketAPIKeys.GetByID(c.Request.Context(), input.APIKeyID)
	if err != nil || key == nil || key.UserID != subject.UserID || !key.IsActive() || key.IsExpired() || key.IsQuotaExhausted() || key.Key == "" {
		response.ErrorWithDetails(c, http.StatusForbidden, "API key unavailable or not owned by this admin", "API_KEY_UNAVAILABLE", nil)
		return
	}
	allowed, err := service.ModelTraceGPTModels()
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	modelSet := make(map[string]bool, len(allowed))
	for _, model := range allowed {
		modelSet[model] = true
	}
	seen := make(map[string]bool, len(input.Models))
	for _, model := range input.Models {
		if !modelSet[model] || seen[model] {
			response.BadRequest(c, "Invalid or duplicate GPT model")
			return
		}
		seen[model] = true
	}
	results := make([]codexDiagnosticItem, 0, len(input.Models))
	for _, model := range input.Models {
		if c.Request.Context().Err() != nil {
			break
		}
		item, attempt := h.diagnoseCodexModel(c, accountID, model, key.Key, generateChallenge)
		if attempt != nil && h.codexTicketAttempts != nil {
			// 落库不阻断诊断结果：用脱离请求生命周期的上下文，避免客户端断开导致丢失记录
			ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 5*time.Second)
			if err := h.codexTicketAttempts.Insert(ctx, attempt); err != nil {
				slog.Warn("codex_ticket_attempt_insert_failed", "account_id", accountID, "model", model, "error", err)
			}
			cancel()
		}
		results = append(results, item)
	}
	response.Success(c, gin.H{"items": results, "canceled": c.Request.Context().Err() != nil})
}

// diagnoseCodexModel probes a single model through the in-process gateway.
// The attempt is returned for history persistence only when the probe was
// actually dispatched to the gateway (pre-flight failures are not recorded).
func (h *AccountHandler) diagnoseCodexModel(c *gin.Context, accountID int64, model, apiKey string, generateChallenge func() (service.ModelTraceChallenge, error)) (codexDiagnosticItem, *service.CodexTicketAttempt) {
	item := codexDiagnosticItem{Model: model, Status: "failed"}
	probeCtx, templateErr := h.codexTicketGateway.PrepareCodexProbeContext(c.Request.Context())
	if templateErr != nil {
		item.Reason = "template_unavailable"
		if errors.Is(templateErr, service.ErrCodexProbeTemplateInvalid) {
			item.Reason = "template_invalid"
		}
		return item, nil
	}
	challenge, challengeErr := generateChallenge()
	if challengeErr != nil {
		item.Reason = "challenge_generation_failed"
		return item, nil
	}
	body, probeHeaders, buildErr := h.codexTicketGateway.BuildCodexDiagnosticRequest(probeCtx, accountID, model, challenge)
	if buildErr != nil {
		item.Reason = "request_build_failed"
		if errors.Is(buildErr, service.ErrCodexProbeIdentity) {
			item.Reason = "identity_resolution_failed"
		}
		return item, nil
	}
	// The normal gateway owns ticket policy, scheduling, injection and invalidation.
	// Diagnostics only pin the target account and analyze the model response.
	modelCtx, cancel := context.WithTimeout(service.WithCodexTicketDiagnostic(probeCtx, accountID), 120*time.Second)
	request, requestErr := http.NewRequestWithContext(modelCtx, http.MethodPost, "/v1/responses", bytes.NewReader(body))
	if requestErr != nil {
		cancel()
		item.Reason = "request_build_failed"
		return item, nil
	}
	for name, values := range probeHeaders {
		request.Header[name] = values
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.RemoteAddr = c.Request.RemoteAddr
	request.Host = c.Request.Host
	start := time.Now()
	writer := httptest.NewRecorder()
	h.codexTicketRouter.ServeHTTP(writer, request)
	cancel()

	code := writer.Code
	expectedCount := challenge.ExpectedCount
	attempt := &service.CodexTicketAttempt{
		AccountID:              accountID,
		Model:                  model,
		OccurredAt:             start,
		Outcome:                "error",
		Trigger:                "diagnostic",
		HTTPStatus:             &code,
		DurationMS:             int(time.Since(start).Milliseconds()),
		ReasonCode:             "gateway_request_failed",
		VerificationMethod:     "modeltrace",
		FingerprintCommit:      service.ModelTraceBankCommit(),
		ChallengeExpectedCount: &expectedCount,
	}
	item.HTTPStatus = code
	if code < 200 || code >= 300 {
		item.GatewayErrorCode = codexDiagnosticGatewayError(writer.Body.Bytes())
		return item, attempt
	}
	output, readErr := io.ReadAll(io.LimitReader(writer.Body, 2<<20))
	if readErr != nil {
		item.Reason = "response_read_failed"
		attempt.ReasonCode = item.Reason
		return item, attempt
	}
	diagnosticText, complete := codexDiagnosticOutput(output)
	if !complete {
		item.Reason = "response_incomplete"
		attempt.Outcome = "miss"
		attempt.ReasonCode = item.Reason
		return item, attempt
	}
	prediction, _, predictErr := service.ModelTracePredictCommitted(diagnosticText, challenge.ExpectedCount)
	item.ParsedCount = prediction.ParsedCount
	attempt.ParsedNumberCount = &prediction.ParsedCount
	if predictErr != nil {
		item.Reason = "insufficient_numbers"
		attempt.Outcome = "success"
		attempt.ReasonCode = item.Reason
		return item, attempt
	}
	item.PredictedModel, item.Probability = prediction.Model, prediction.Probability
	attempt.Outcome = "success"
	attempt.ReasonCode = ""
	attempt.FingerprintPredictedModel = prediction.Model
	attempt.FingerprintProbability = &prediction.Probability
	if prediction.Model == model {
		item.Status = "normal"
		matched := true
		attempt.FingerprintMatched = &matched
	} else {
		item.Status = "degraded"
		item.Reason = "fingerprint_mismatch"
		matched := false
		attempt.FingerprintMatched = &matched
		attempt.ReasonCode = item.Reason
	}
	return item, attempt
}
