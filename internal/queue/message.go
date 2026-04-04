package queue

import "time"

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
