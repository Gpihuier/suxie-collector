package queue

import "time"

// CollectMessage 是采集结果的 MQ 载荷模型。
// 设计为“原始数据 + 请求上下文”组合，便于下游幂等与排错。
type CollectMessage struct {
	TenantID      string         `json:"tenant_id"`
	Platform      string         `json:"platform"`
	JobName       string         `json:"job_name"`
	RequestID     string         `json:"request_id"`
	Page          int            `json:"page"`
	Total         int            `json:"total"`
	Records       []any          `json:"records"`
	Raw           map[string]any `json:"raw"`
	RequestParams map[string]any `json:"request_params"`
	CollectedAt   time.Time      `json:"collected_at"`
}
