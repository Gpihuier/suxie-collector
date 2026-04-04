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

// placeholderRegex 用于匹配模板占位符，例如 ${window_start}。
var placeholderRegex = regexp.MustCompile(`\$\{([a-zA-Z0-9_\-\.]+)\}`)

// ResolveInput 是变量解析输入。
type ResolveInput struct {
	// TenantID 当前租户。
	TenantID string
	// JobName 当前任务名。
	JobName string
	// Timezone 变量计算时区。
	Timezone string
	// Now 当前时间（可注入，便于测试）。
	Now time.Time
	// Cursor 上次运行游标（用于增量窗口续跑）。
	Cursor storage.CursorState
}

// ResolveOutput 是变量解析输出。
// 分两类：
// 1) Scalars: 标量变量（单值）
// 2) Lists: 列表变量（用于笛卡尔积参数展开）
type ResolveOutput struct {
	// Scalars 保存 key->value 的字符串变量。
	Scalars map[string]string
	// Lists 保存 key->[]value 的列表变量。
	Lists map[string][]string
	// NextWindowEnd 表示本次窗口结束点，执行完成后会写回游标。
	NextWindowEnd *time.Time
	// CurrentWindowStart 记录当前窗口开始点（用于日志）。
	CurrentWindowStart *time.Time
	// CurrentWindowEnd 记录当前窗口结束点（用于日志）。
	CurrentWindowEnd *time.Time
}

// VariableProvider 是变量来源扩展点。
// 新增变量来源（数据库、HTTP、文件）只需实现该接口。
type VariableProvider interface {
	// Type 返回 provider 类型标识。
	Type() string
	// Resolve 输出本 provider 提供的变量。
	Resolve(ctx context.Context, input ResolveInput) (ResolveOutput, error)
}

// StaticProvider 返回固定标量变量。
type StaticProvider struct {
	// Key 变量名。
	Key string
	// Value 变量值。
	Value string
}

// Type 返回 provider 类型。
func (p StaticProvider) Type() string { return "static" }

// Resolve 输出固定变量。
func (p StaticProvider) Resolve(_ context.Context, _ ResolveInput) (ResolveOutput, error) {
	return ResolveOutput{Scalars: map[string]string{p.Key: p.Value}}, nil
}

// ListProvider 返回固定列表变量。
type ListProvider struct {
	// Key 变量名。
	Key string
	// Values 列表值。
	Values []string
}

// Type 返回 provider 类型。
func (p ListProvider) Type() string { return "list" }

// Resolve 输出列表变量。
func (p ListProvider) Resolve(_ context.Context, _ ResolveInput) (ResolveOutput, error) {
	// 做一次切片复制，避免外部修改原始配置数据。
	values := append([]string{}, p.Values...)
	return ResolveOutput{Lists: map[string][]string{p.Key: values}}, nil
}

// DateWindowProvider 负责生成时间窗口变量。
// 常见用途：startDate/endDate 增量拉取。
type DateWindowProvider struct {
	// KeyStart 开始时间变量名。
	KeyStart string
	// KeyEnd 结束时间变量名。
	KeyEnd string
	// Format 输出格式（Go layout）。
	Format string
	// Window 窗口跨度（例如 24h）。
	Window time.Duration
	// StartFrom 首次运行起点。
	StartFrom time.Time
}

// Type 返回 provider 类型。
func (p DateWindowProvider) Type() string { return "date_window" }

// Resolve 计算当前窗口并输出 start/end 变量。
func (p DateWindowProvider) Resolve(_ context.Context, input ResolveInput) (ResolveOutput, error) {
	// 加载配置时区，失败时回退到本地时区。
	loc, err := time.LoadLocation(input.Timezone)
	if err != nil {
		loc = time.Local
	}

	// now 统一转成业务时区。
	now := input.Now.In(loc)
	// 默认从 StartFrom 起跑。
	start := p.StartFrom.In(loc)

	// 如果存在游标窗口结束时间，优先从游标续跑。
	if input.Cursor.LastWindowEnd != "" {
		last, parseErr := time.Parse(time.RFC3339, input.Cursor.LastWindowEnd)
		if parseErr == nil {
			start = last.In(loc)
		}
	}

	// StartFrom 和游标都为空时，默认取 now-24h。
	if start.IsZero() {
		start = now.Add(-24 * time.Hour)
	}

	// end = start + window，但不能超过 now。
	end := start.Add(p.Window)
	if end.After(now) {
		end = now
	}
	// 极端情况下确保 end >= start。
	if !end.After(start) {
		end = start
	}

	// 输出变量采用配置格式（默认 RFC3339）。
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

// BuildProviders 把配置层变量定义编译成 provider 实例。
func BuildProviders(configs []config.VariableConfig) ([]VariableProvider, error) {
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

// layout 返回日期格式；未配置时默认 RFC3339。
func (p DateWindowProvider) layout() string {
	if strings.TrimSpace(p.Format) == "" {
		return time.RFC3339
	}
	return p.Format
}

// ResolveVariables 按 provider 顺序合并变量。
// 后面的 provider 同名 key 会覆盖前面的值。
func ResolveVariables(ctx context.Context, input ResolveInput, providers []VariableProvider) (ResolveOutput, error) {
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

// RenderParamSets 把模板参数渲染成可执行参数集合。
func RenderParamSets(template map[string]any, variables ResolveOutput) []map[string]any {
	// 空模板仍然返回一个空参数集合，保证任务会执行一次。
	if len(template) == 0 {
		return []map[string]any{{}}
	}

	// 查找模板中真正使用到的列表变量。
	usedListVars := findUsedListVariables(template, variables.Lists)
	if len(usedListVars) == 0 {
		// 无列表变量时只渲染一个参数集。
		return []map[string]any{renderSingle(template, variables.Scalars, nil)}
	}

	// 固定顺序保证结果稳定。
	sort.Strings(usedListVars)
	// 对列表变量做笛卡尔积。
	combinations := crossProduct(variables.Lists, usedListVars)
	result := make([]map[string]any, 0, len(combinations))
	for _, combo := range combinations {
		result = append(result, renderSingle(template, variables.Scalars, combo))
	}
	return result
}

// renderSingle 渲染单组参数。
func renderSingle(template map[string]any, scalars map[string]string, listValues map[string]string) map[string]any {
	out := make(map[string]any, len(template))
	for k, rawValue := range template {
		tpl, ok := rawValue.(string)
		if !ok {
			// 非字符串值不做模板替换，直接透传。
			out[k] = rawValue
			continue
		}
		// 针对字符串做 ${var} 替换。
		v := placeholderRegex.ReplaceAllStringFunc(tpl, func(raw string) string {
			match := placeholderRegex.FindStringSubmatch(raw)
			if len(match) != 2 {
				return raw
			}
			key := match[1]
			// 优先用列表变量组合值。
			if listValues != nil {
				if v, ok := listValues[key]; ok {
					return v
				}
			}
			// 其次用标量变量。
			if v, ok := scalars[key]; ok {
				return v
			}
			// 未匹配变量时替换为空字符串。
			return ""
		})
		out[k] = v
	}
	return out
}

// findUsedListVariables 找出模板里用到的列表变量名。
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

// crossProduct 计算列表变量的笛卡尔积。
func crossProduct(lists map[string][]string, keys []string) []map[string]string {
	if len(keys) == 0 {
		return []map[string]string{{}}
	}

	// result 从一个空组合开始逐步扩展。
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

// mapClone 复制 map[string]string。
func mapClone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
