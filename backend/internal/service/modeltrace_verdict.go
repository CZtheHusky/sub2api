package service

import "slices"

// CodexDiagnosticVerdict 是一次降智判定。
// Status: normal(正常) / degraded(疑似降智) / uncertain(不确定) / failed(请求失败)。
type CodexDiagnosticVerdict struct {
	Status         string  `json:"status"`
	Reason         string  `json:"reason,omitempty"`
	PredictedModel string  `json:"predicted_model,omitempty"`
	Probability    float64 `json:"probability,omitempty"`
	ParsedCount    int     `json:"parsed_count"`
}

// EvaluateCodexDiagnostic 把探测输出与请求模型比对。
//
// 指纹库是闭集：请求的模型若不在库中（当前只有 gpt-6.1-sol），分类器永远
// 不会返回它的 ID。此时用同系列参考类兜底判定——命中参考类报 uncertain
// （库缺类，无法确认具体版本），命中其它任何 GPT 类才是 degraded。这样
// 健康的 6.1-sol 不会被误判成降智，而降级到 5.6/其它系列仍会被抓到。
func EvaluateCodexDiagnostic(model, output string, challenge ModelTraceChallenge) CodexDiagnosticVerdict {
	prediction, _, err := ModelTracePredictCommitted(output, challenge.ExpectedCount)
	verdict := CodexDiagnosticVerdict{Status: "failed", Reason: "insufficient_numbers", ParsedCount: prediction.ParsedCount}
	if err != nil {
		return verdict
	}
	verdict.PredictedModel = prediction.Model
	verdict.Probability = prediction.Probability
	models, err := ModelTraceDiagnosticModels()
	if err != nil || !slices.Contains(models, model) {
		verdict.Status = "uncertain"
		verdict.Reason = "reference_class_unavailable"
		return verdict
	}
	if prediction.Model == model {
		return CodexDiagnosticVerdict{Status: "normal", PredictedModel: prediction.Model, Probability: prediction.Probability, ParsedCount: prediction.ParsedCount}
	}
	if reference, ok := modelTraceDiagnosticReferenceClass[model]; ok {
		if prediction.Model == reference {
			verdict.Status = "uncertain"
			verdict.Reason = "reference_class_unavailable"
			return verdict
		}
	}
	verdict.Status = "degraded"
	verdict.Reason = "fingerprint_mismatch"
	return verdict
}
