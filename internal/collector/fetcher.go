package collector

import "context"

// FetchRequest 是采集引擎传给平台适配层的统一请求模型。
// 这个结构体故意不包含任何“领星专属”字段，以便未来接入马帮等平台时复用。
type FetchRequest struct {
	// TenantID 标识当前请求属于哪个租户。
	// 平台客户端可用它查租户级鉴权（例如 appId/appSecret/token）。
	TenantID string
	// Method 是 HTTP 方法（GET/POST...）。
	Method string
	// Endpoint 是平台接口路径或完整 URL。
	Endpoint string
	// RequestIn 指定业务参数放在 query 还是 body。
	// 约定值："query" / "body"。
	RequestIn string
	// Headers 是调用方附加的请求头。
	Headers map[string]string
	// Params 是业务参数。
	// 鉴权相关字段（access_token/sign/timestamp）通常由平台客户端补齐。
	Params map[string]any
}

// Fetcher 抽象了“拉取一页数据”的能力。
// 采集引擎只依赖这个接口，不感知具体平台的签名和 token 细节。
type Fetcher interface {
	// Platform 返回平台标识（例如 lingxing）。
	Platform() string
	// Fetch 执行一次请求并返回解码后的 JSON 对象。
	// 返回 map[string]any 是为了适配不同接口的非统一字段。
	Fetch(ctx context.Context, req FetchRequest) (map[string]any, error)
}
