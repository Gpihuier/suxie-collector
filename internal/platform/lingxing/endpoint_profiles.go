package lingxing

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type endpointProfile struct {
	Method              string
	TokenBucketCapacity int
	ApplyAndValidate    func(params map[string]any) error
}

// endpointProfiles 存放“接口级协议约束”：
// - 默认参数
// - 参数合法性校验
// - 独立令牌桶容量
var endpointProfiles = map[string]endpointProfile{
	"/bd/profit/report/open/report/msku/list": {
		Method:              "POST",
		TokenBucketCapacity: 10,
		ApplyAndValidate:    applyMSKUProfitParams,
	},
}

func lookupEndpointProfile(target string) (endpointProfile, bool) {
	u, err := url.Parse(target)
	if err != nil {
		return endpointProfile{}, false
	}
	p, ok := endpointProfiles[u.Path]
	return p, ok
}

// applyMSKUProfitParams 对“利润报表-MSKU”应用默认值并做参数校验。
func applyMSKUProfitParams(params map[string]any) error {
	if params == nil {
		return fmt.Errorf("msku list params is nil")
	}

	if _, ok := params["offset"]; !ok {
		params["offset"] = 0
	}
	if _, ok := params["length"]; !ok {
		params["length"] = 1000
	}
	if _, ok := params["monthlyQuery"]; !ok {
		params["monthlyQuery"] = false
	}

	offset, err := asInt(params["offset"])
	if err != nil || offset < 0 {
		return fmt.Errorf("offset must be int and >= 0")
	}
	params["offset"] = offset

	length, err := asInt(params["length"])
	if err != nil || length <= 0 || length > 10000 {
		return fmt.Errorf("length must be int between 1 and 10000")
	}
	params["length"] = length

	monthly, err := asBool(params["monthlyQuery"])
	if err != nil {
		return fmt.Errorf("monthlyQuery must be bool")
	}
	params["monthlyQuery"] = monthly

	startDate, ok := asString(params["startDate"])
	if !ok || strings.TrimSpace(startDate) == "" {
		return fmt.Errorf("startDate is required")
	}
	endDate, ok := asString(params["endDate"])
	if !ok || strings.TrimSpace(endDate) == "" {
		return fmt.Errorf("endDate is required")
	}

	if monthly {
		// 按月查询：开始结束必须同月，格式 Y-m
		start, err := time.Parse("2006-01", startDate)
		if err != nil {
			return fmt.Errorf("monthlyQuery=true startDate format must be Y-m")
		}
		end, err := time.Parse("2006-01", endDate)
		if err != nil {
			return fmt.Errorf("monthlyQuery=true endDate format must be Y-m")
		}
		if start.Year() != end.Year() || start.Month() != end.Month() {
			return fmt.Errorf("monthlyQuery=true requires startDate and endDate in same month")
		}
		return nil
	}

	// 按天查询：双闭区间且跨度不超过 31 天，格式 Y-m-d
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return fmt.Errorf("monthlyQuery=false startDate format must be Y-m-d")
	}
	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return fmt.Errorf("monthlyQuery=false endDate format must be Y-m-d")
	}
	if end.Before(start) {
		return fmt.Errorf("endDate must be >= startDate")
	}
	days := int(end.Sub(start).Hours()/24) + 1
	if days > 31 {
		return fmt.Errorf("monthlyQuery=false date range cannot exceed 31 days")
	}
	return nil
}

// asInt 以宽松类型转换方式读取整数字段。
func asInt(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported int type %T", v)
	}
}

// asBool 以宽松类型转换方式读取布尔字段。
func asBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(x))
		if err != nil {
			return false, err
		}
		return b, nil
	default:
		return false, fmt.Errorf("unsupported bool type %T", v)
	}
}

// asString 读取字符串字段。
func asString(v any) (string, bool) {
	s, ok := v.(string)
	if ok {
		return s, true
	}
	return "", false
}
