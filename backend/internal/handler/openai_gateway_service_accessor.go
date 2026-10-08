package handler

import "github.com/Wei-Shaw/sub2api/internal/service"

// Service exposes the OpenAI gateway service for cross-module wiring
// (ModelTrace diagnostic replays route through the live gateway).
func (h *OpenAIGatewayHandler) Service() *service.OpenAIGatewayService {
	return h.gatewayService
}
