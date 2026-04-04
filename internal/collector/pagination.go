package collector

import (
	"fmt"
	"strconv"
	"strings"
)

// ExtractTotal 按候选路径顺序提取 total。
// 典型兼容场景：
// 1) data.total
// 2) Data.Total
// 3) 外层 total/Total
func ExtractTotal(payload map[string]any, candidates []string) (int, bool) {
	// 逐个尝试候选路径，命中即返回。
	for _, path := range candidates {
		// 按路径取值（支持大小写不敏感）。
		v, ok := LookupPath(payload, path)
		if !ok {
			continue
		}
		// 取到值后做类型转换。
		total, ok := toInt(v)
		if ok {
			return total, true
		}
	}
	// 所有路径都未成功解析为 int。
	return 0, false
}

// ExtractRecords 按候选路径提取记录列表。
func ExtractRecords(payload map[string]any, candidates []string) ([]any, bool) {
	for _, path := range candidates {
		v, ok := LookupPath(payload, path)
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case []any:
			// 已是通用切片，直接返回。
			return vv, true
		case []map[string]any:
			// 转成 []any，统一上游处理。
			out := make([]any, 0, len(vv))
			for _, item := range vv {
				out = append(out, item)
			}
			return out, true
		}
	}
	return nil, false
}

// LookupPath 支持 a.b.c 路径访问，并对每一层 key 做大小写不敏感匹配。
func LookupPath(payload map[string]any, path string) (any, bool) {
	// 空路径视为返回整个 payload。
	if path == "" {
		return payload, true
	}
	// 用点号拆分层级路径。
	segments := strings.Split(path, ".")
	var current any = payload
	for _, seg := range segments {
		// 每一层都要求是 map[string]any。
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		// 按大小写不敏感方式取下一层。
		next, found := getMapValueCaseInsensitive(m, seg)
		if !found {
			return nil, false
		}
		current = next
	}
	return current, true
}

// getMapValueCaseInsensitive 先精确匹配，再降级为小写比较匹配。
func getMapValueCaseInsensitive(in map[string]any, key string) (any, bool) {
	if v, ok := in[key]; ok {
		return v, true
	}
	lower := strings.ToLower(key)
	for k, v := range in {
		if strings.ToLower(k) == lower {
			return v, true
		}
	}
	return nil, false
}

// toInt 把常见数值类型统一转为 int。
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case float32:
		return int(n), true
	case string:
		if n == "" {
			return 0, false
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

// BuildPageParams 把分页字段写入请求参数。
func BuildPageParams(base map[string]any, pagination Pagination, page int) map[string]any {
	// 复制一份，避免修改调用方 map。
	params := make(map[string]any, len(base)+2)
	for k, v := range base {
		params[k] = v
	}
	// 开启分页时写入页码与每页大小。
	if pagination.Enabled {
		params[pagination.PageParam] = page
		params[pagination.PageSizeParam] = pagination.PageSize
	}
	return params
}

// NeedNextPage 判断是否继续翻页。
// 规则：
// 1) 当前无记录 -> 结束。
// 2) total 已知 -> page*pageSize < total 才继续。
// 3) total 未知 -> 本页满页才继续（保守策略）。
func NeedNextPage(page, pageSize, recordCount, total int, totalKnown bool) bool {
	if recordCount == 0 {
		return false
	}
	if totalKnown {
		return page*pageSize < total
	}
	return recordCount >= pageSize
}

// EnsurePagination 归一化分页配置并做基础校验。
func EnsurePagination(p Pagination) (Pagination, error) {
	// 未开启分页时直接返回。
	if !p.Enabled {
		return p, nil
	}
	// 起始页默认值。
	if p.StartPage <= 0 {
		p.StartPage = 1
	}
	// 页码参数名默认值。
	if p.PageParam == "" {
		p.PageParam = "page"
	}
	// 每页大小参数名默认值。
	if p.PageSizeParam == "" {
		p.PageSizeParam = "limit"
	}
	// 每页大小必须是正整数。
	if p.PageSize <= 0 {
		return p, fmt.Errorf("invalid page_size: %d", p.PageSize)
	}
	// total 提取路径默认值。
	if len(p.TotalPathCandidates) == 0 {
		p.TotalPathCandidates = []string{"data.total", "total", "Total"}
	}
	return p, nil
}
