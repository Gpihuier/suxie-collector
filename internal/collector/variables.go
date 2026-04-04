package collector

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"suxie.com/suxie-collector/internal/config"
	"suxie.com/suxie-collector/internal/storage"
)

var placeholderRegex = regexp.MustCompile(`\$\{([a-zA-Z0-9_\-\.]+)\}`)

type ResolveInput struct {
	TenantID string
	JobName  string
	Timezone string
	Now      time.Time
	Cursor   storage.CursorState
}

// ResolveOutput 分为两类：
// 1) Scalars: 单值变量
// 2) Lists: 列表变量（会参与参数组合展开）
type ResolveOutput struct {
	Scalars            map[string]string
	Lists              map[string][]string
	NextWindowEnd      *time.Time
	CurrentWindowStart *time.Time
	CurrentWindowEnd   *time.Time
}

type VariableProvider interface {
	Type() string
	Resolve(ctx context.Context, input ResolveInput) (ResolveOutput, error)
}

// StaticProvider 提供固定值变量。
type StaticProvider struct {
	Key   string
	Value string
}

func (p StaticProvider) Type() string { return "static" }

func (p StaticProvider) Resolve(_ context.Context, _ ResolveInput) (ResolveOutput, error) {
	return ResolveOutput{Scalars: map[string]string{p.Key: p.Value}}, nil
}

type ListProvider struct {
	Key    string
	Values []string
}

func (p ListProvider) Type() string { return "list" }

func (p ListProvider) Resolve(_ context.Context, _ ResolveInput) (ResolveOutput, error) {
	values := append([]string{}, p.Values...)
	return ResolveOutput{Lists: map[string][]string{p.Key: values}}, nil
}

// DateWindowProvider 提供时间窗口变量（start/end）。
// 常用于按天/按月增量采集。
type DateWindowProvider struct {
	KeyStart  string
	KeyEnd    string
	Format    string
	Window    time.Duration
	StartFrom time.Time
}

func (p DateWindowProvider) Type() string { return "date_window" }

func (p DateWindowProvider) Resolve(_ context.Context, input ResolveInput) (ResolveOutput, error) {
	loc, err := time.LoadLocation(input.Timezone)
	if err != nil {
		loc = time.Local
	}

	now := input.Now.In(loc)
	start := p.StartFrom.In(loc)

	// 若存在游标则从上次窗口结束时间续跑。
	if input.Cursor.LastWindowEnd != "" {
		last, parseErr := time.Parse(time.RFC3339, input.Cursor.LastWindowEnd)
		if parseErr == nil {
			start = last.In(loc)
		}
	}

	if start.IsZero() {
		start = now.Add(-24 * time.Hour)
	}

	// end 不能超过当前时间，保证窗口不会跑到未来。
	end := start.Add(p.Window)
	if end.After(now) {
		end = now
	}
	if !end.After(start) {
		end = start
	}

	scalars := map[string]string{
		p.KeyStart: start.Format(p.layout()),
		p.KeyEnd:   end.Format(p.layout()),
	}

	return ResolveOutput{
		Scalars:            scalars,
		NextWindowEnd:      &end,
		CurrentWindowStart: &start,
		CurrentWindowEnd:   &end,
	}, nil
}

func BuildProviders(configs []config.VariableConfig) ([]VariableProvider, error) {
	// BuildProviders 把配置映射为具体 provider，实现可插拔变量来源。
	providers := make([]VariableProvider, 0, len(configs))
	for _, c := range configs {
		typ := strings.ToLower(strings.TrimSpace(c.Type))
		switch typ {
		case "", "static":
			if c.Key == "" {
				return nil, fmt.Errorf("static variable missing key")
			}
			providers = append(providers, StaticProvider{Key: c.Key, Value: c.Value})
		case "list", "shop_list":
			if c.Key == "" {
				return nil, fmt.Errorf("list variable missing key")
			}
			if len(c.Values) == 0 {
				return nil, fmt.Errorf("list variable %s missing values", c.Key)
			}
			providers = append(providers, ListProvider{Key: c.Key, Values: c.Values})
		case "date_window":
			if c.KeyStart == "" || c.KeyEnd == "" {
				return nil, fmt.Errorf("date_window requires key_start and key_end")
			}
			window, err := time.ParseDuration(c.Window)
			if err != nil || window <= 0 {
				return nil, fmt.Errorf("invalid date window duration: %s", c.Window)
			}
			var startFrom time.Time
			if c.StartFrom != "" {
				parsed, err := time.Parse(time.RFC3339, c.StartFrom)
				if err != nil {
					return nil, fmt.Errorf("invalid start_from for date_window: %w", err)
				}
				startFrom = parsed
			}
			providers = append(providers, DateWindowProvider{
				KeyStart:  c.KeyStart,
				KeyEnd:    c.KeyEnd,
				Format:    c.Format,
				Window:    window,
				StartFrom: startFrom,
			})
		default:
			return nil, fmt.Errorf("unsupported variable type: %s", c.Type)
		}
	}
	return providers, nil
}

func (p DateWindowProvider) layout() string {
	if strings.TrimSpace(p.Format) == "" {
		return time.RFC3339
	}
	return p.Format
}

func ResolveVariables(ctx context.Context, input ResolveInput, providers []VariableProvider) (ResolveOutput, error) {
	// 按 provider 顺序合并，后写入的同名 key 会覆盖前者。
	merged := ResolveOutput{
		Scalars: map[string]string{},
		Lists:   map[string][]string{},
	}
	for _, provider := range providers {
		out, err := provider.Resolve(ctx, input)
		if err != nil {
			return ResolveOutput{}, fmt.Errorf("resolve variable provider=%s: %w", provider.Type(), err)
		}
		for k, v := range out.Scalars {
			merged.Scalars[k] = v
		}
		for k, values := range out.Lists {
			merged.Lists[k] = append([]string{}, values...)
		}
		if out.NextWindowEnd != nil {
			merged.NextWindowEnd = out.NextWindowEnd
		}
		if out.CurrentWindowStart != nil {
			merged.CurrentWindowStart = out.CurrentWindowStart
		}
		if out.CurrentWindowEnd != nil {
			merged.CurrentWindowEnd = out.CurrentWindowEnd
		}
	}
	return merged, nil
}

func RenderParamSets(template map[string]any, variables ResolveOutput) []map[string]any {
	if len(template) == 0 {
		return []map[string]any{{}}
	}

	usedListVars := findUsedListVariables(template, variables.Lists)
	if len(usedListVars) == 0 {
		return []map[string]any{renderSingle(template, variables.Scalars, nil)}
	}

	sort.Strings(usedListVars)
	// 列表变量做笛卡尔积，生成多组请求参数。
	combinations := crossProduct(variables.Lists, usedListVars)
	result := make([]map[string]any, 0, len(combinations))
	for _, combo := range combinations {
		result = append(result, renderSingle(template, variables.Scalars, combo))
	}
	return result
}

func renderSingle(template map[string]any, scalars map[string]string, listValues map[string]string) map[string]any {
	out := make(map[string]any, len(template))
	for k, rawValue := range template {
		tpl, ok := rawValue.(string)
		if !ok {
			out[k] = rawValue
			continue
		}
		v := placeholderRegex.ReplaceAllStringFunc(tpl, func(raw string) string {
			match := placeholderRegex.FindStringSubmatch(raw)
			if len(match) != 2 {
				return raw
			}
			key := match[1]
			if listValues != nil {
				if v, ok := listValues[key]; ok {
					return v
				}
			}
			if v, ok := scalars[key]; ok {
				return v
			}
			return ""
		})
		out[k] = v
	}
	return out
}

func findUsedListVariables(template map[string]any, lists map[string][]string) []string {
	set := map[string]struct{}{}
	for _, raw := range template {
		tpl, ok := raw.(string)
		if !ok {
			continue
		}
		matches := placeholderRegex.FindAllStringSubmatch(tpl, -1)
		for _, m := range matches {
			if len(m) != 2 {
				continue
			}
			if _, ok := lists[m[1]]; ok {
				set[m[1]] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func crossProduct(lists map[string][]string, keys []string) []map[string]string {
	if len(keys) == 0 {
		return []map[string]string{{}}
	}

	result := []map[string]string{{}}
	for _, key := range keys {
		values := lists[key]
		if len(values) == 0 {
			continue
		}
		next := make([]map[string]string, 0, len(result)*len(values))
		for _, base := range result {
			for _, value := range values {
				entry := mapClone(base)
				entry[key] = value
				next = append(next, entry)
			}
		}
		result = next
	}
	return result
}

func mapClone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
