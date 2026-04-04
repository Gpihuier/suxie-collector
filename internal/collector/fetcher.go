package collector

import "context"

// FetchRequest 是平台层统一请求模型。
// 采集引擎只关心“拉数据”，不关心具体平台签名细节。
type FetchRequest struct {
	TenantID  string
	Method    string
	Endpoint  string
	RequestIn string
	Headers   map[string]string
	Params    map[string]any
}

// Fetcher 是平台适配器接口：
// - Platform: 返回平台标识（如 lingxing）
// - Fetch: 执行一次 API 请求并返回原始 JSON 对象
type Fetcher interface {
	Platform() string
	Fetch(ctx context.Context, req FetchRequest) (map[string]any, error)
}
