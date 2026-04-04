package lingxing

import "fmt"

// GlobalErrorCodeDesc 领星全局错误码说明（来自接入文档）。
var GlobalErrorCodeDesc = map[int]string{
	2001001: "app not exist: appId不存在，检查值有效性",
	2001002: "app secret not correct: appSecret不正确，检查值有效性",
	2001003: "access token is missing or expire: token不存在或者已过期，可刷新token重试",
	2001004: "the api not authorized: 请求的api未授权，联系领星确认",
	2001005: "access token not match: access_token不正确，检查值有效性",
	2001006: "api sign not correct: 签名不正确，检查签名生成与urlencode处理",
	2001007: "api sign has expired: 签名已过期，检查timestamp是否在有效期",
	2001008: "refresh token expired: refresh_token过期，请重新获取access token",
	2001009: "refresh token is invalid: refresh_token无效，检查值有效性",
	3001001: "missing query param(access_token,sign,timestamp,app_key): 缺少必传参数",
	3001002: "ip not permit: ip未加入白名单",
	3001008: "requests too frequently: 接口请求过于频繁触发限流（维度: appId + 接口url）",
}

const (
	ErrCodeTokenMissingOrExpired = 2001003
	ErrCodeAccessTokenNotMatch   = 2001005
	ErrCodeRateLimited           = 3001008
)

type APIError struct {
	Code      int
	Message   string
	RequestID string
	Endpoint  string
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if desc, ok := GlobalErrorCodeDesc[e.Code]; ok {
		return fmt.Sprintf("lingxing api error code=%d message=%s request_id=%s endpoint=%s desc=%s", e.Code, e.Message, e.RequestID, e.Endpoint, desc)
	}
	return fmt.Sprintf("lingxing api error code=%d message=%s request_id=%s endpoint=%s", e.Code, e.Message, e.RequestID, e.Endpoint)
}
