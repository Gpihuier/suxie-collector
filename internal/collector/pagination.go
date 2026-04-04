package collector

import (
	"fmt"
	"strconv"
	"strings"
)

func ExtractTotal(payload map[string]any, candidates []string) (int, bool) {
	for _, path := range candidates {
		v, ok := LookupPath(payload, path)
		if !ok {
			continue
		}
		total, ok := toInt(v)
		if ok {
			return total, true
		}
	}
	return 0, false
}

func ExtractRecords(payload map[string]any, candidates []string) ([]any, bool) {
	for _, path := range candidates {
		v, ok := LookupPath(payload, path)
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case []any:
			return vv, true
		case []map[string]any:
			out := make([]any, 0, len(vv))
			for _, item := range vv {
				out = append(out, item)
			}
			return out, true
		}
	}
	return nil, false
}

func LookupPath(payload map[string]any, path string) (any, bool) {
	if path == "" {
		return payload, true
	}
	segments := strings.Split(path, ".")
	var current any = payload
	for _, seg := range segments {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, found := getMapValueCaseInsensitive(m, seg)
		if !found {
			return nil, false
		}
		current = next
	}
	return current, true
}

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

func BuildPageParams(base map[string]any, pagination Pagination, page int) map[string]any {
	params := make(map[string]any, len(base)+2)
	for k, v := range base {
		params[k] = v
	}
	if pagination.Enabled {
		params[pagination.PageParam] = page
		params[pagination.PageSizeParam] = pagination.PageSize
	}
	return params
}

func NeedNextPage(page, pageSize, recordCount, total int, totalKnown bool) bool {
	if recordCount == 0 {
		return false
	}
	if totalKnown {
		return page*pageSize < total
	}
	return recordCount >= pageSize
}

func EnsurePagination(p Pagination) (Pagination, error) {
	if !p.Enabled {
		return p, nil
	}
	if p.StartPage <= 0 {
		p.StartPage = 1
	}
	if p.PageParam == "" {
		p.PageParam = "page"
	}
	if p.PageSizeParam == "" {
		p.PageSizeParam = "limit"
	}
	if p.PageSize <= 0 {
		return p, fmt.Errorf("invalid page_size: %d", p.PageSize)
	}
	if len(p.TotalPathCandidates) == 0 {
		p.TotalPathCandidates = []string{"data.total", "Data.Total", "total", "Total"}
	}
	return p, nil
}
