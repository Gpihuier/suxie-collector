package collector

import "time"

// BaseCollectTask 是采集任务的基础结构体。
// 设计目标：
// 1) 用 WithXXX 链式配置，减少构建样板代码。
// 2) 让 runner 可以把配置层对象统一编译成可执行任务。
// 3) 为未来动态任务（非 YAML）预留统一入口。
type BaseCollectTask struct {
	// TenantID 表示任务所属租户。
	TenantID string
	// Platform 表示平台名（例如 lingxing）。
	Platform string
	// JobName 表示任务名，用于日志与指标标签。
	JobName string
	// Method 表示请求方法（GET/POST...）。
	Method string
	// Endpoint 表示请求路径（或完整 URL）。
	Endpoint string
	// RequestIn 表示参数位置（query/body）。
	RequestIn string
	// Headers 表示固定请求头模板。
	Headers map[string]string
	// ParamTemplate 表示参数模板，支持 `${var}` 占位符。
	ParamTemplate map[string]any

	// Pagination 保存分页策略。
	Pagination Pagination
	// DataPaths 是记录列表提取路径候选。
	DataPaths []string
	// Parallelism 表示同一任务内参数组合并发度。
	Parallelism int

	// CursorPrefix 用于自定义游标 key 前缀。
	CursorPrefix string
	// WindowStart/WindowEnd 可选记录当前采集窗口。
	WindowStart *time.Time
	WindowEnd   *time.Time
}

// Pagination 描述分页行为。
type Pagination struct {
	// Enabled 控制是否启用分页。
	Enabled bool
	// StartPage 表示起始页码。
	StartPage int
	// PageParam 表示页码参数名（例如 page/offset）。
	PageParam string
	// PageSizeParam 表示每页大小参数名（例如 limit/length）。
	PageSizeParam string
	// PageSize 表示每页大小。
	PageSize int
	// TotalPathCandidates 表示 total 的候选提取路径。
	TotalPathCandidates []string
}

// NewBaseCollectTask 创建带默认值的任务对象。
func NewBaseCollectTask() *BaseCollectTask {
	return &BaseCollectTask{
		// 默认 GET，可由 WithMethod 覆盖。
		Method: "GET",
		// 默认 query，可由 WithRequestIn 覆盖。
		RequestIn: "query",
		// 初始化空 map，避免后续 nil map 写入 panic。
		Headers: make(map[string]string),
		// 初始化空 map，供参数模板写入。
		ParamTemplate: make(map[string]any),
		// 默认分页配置：page + limit。
		Pagination: Pagination{
			Enabled:       true,
			StartPage:     1,
			PageParam:     "page",
			PageSizeParam: "limit",
			PageSize:      50,
			// 兼容 total 在 data/Data/外层多种情况。
			TotalPathCandidates: []string{"data.total", "Data.Total", "total", "Total"},
		},
		// 兼容 list 在常见字段路径中的不同命名。
		DataPaths: []string{"data.list", "data.data", "Data.List", "Data.Data", "list", "data"},
		// 默认单协程处理参数组合。
		Parallelism: 1,
	}
}

// WithTenantID 设置租户 ID。
func (t *BaseCollectTask) WithTenantID(v string) *BaseCollectTask {
	t.TenantID = v
	return t
}

// WithPlatform 设置平台。
func (t *BaseCollectTask) WithPlatform(v string) *BaseCollectTask {
	t.Platform = v
	return t
}

// WithJobName 设置任务名。
func (t *BaseCollectTask) WithJobName(v string) *BaseCollectTask {
	t.JobName = v
	return t
}

// WithMethod 设置 HTTP 方法；空值不覆盖默认值。
func (t *BaseCollectTask) WithMethod(v string) *BaseCollectTask {
	if v != "" {
		t.Method = v
	}
	return t
}

// WithEndpoint 设置接口路径。
func (t *BaseCollectTask) WithEndpoint(v string) *BaseCollectTask {
	t.Endpoint = v
	return t
}

// WithRequestIn 设置参数位置；空值不覆盖默认值。
func (t *BaseCollectTask) WithRequestIn(v string) *BaseCollectTask {
	if v != "" {
		t.RequestIn = v
	}
	return t
}

// WithHeader 设置单个请求头。
func (t *BaseCollectTask) WithHeader(k, v string) *BaseCollectTask {
	if t.Headers == nil {
		t.Headers = make(map[string]string)
	}
	t.Headers[k] = v
	return t
}

// WithHeaders 合并一组请求头。
func (t *BaseCollectTask) WithHeaders(v map[string]string) *BaseCollectTask {
	if t.Headers == nil {
		t.Headers = make(map[string]string)
	}
	for k, val := range v {
		t.Headers[k] = val
	}
	return t
}

// WithParam 设置单个模板参数。
func (t *BaseCollectTask) WithParam(k string, v any) *BaseCollectTask {
	if t.ParamTemplate == nil {
		t.ParamTemplate = make(map[string]any)
	}
	t.ParamTemplate[k] = v
	return t
}

// WithParams 合并一组模板参数。
func (t *BaseCollectTask) WithParams(v map[string]any) *BaseCollectTask {
	if t.ParamTemplate == nil {
		t.ParamTemplate = make(map[string]any)
	}
	for k, val := range v {
		t.ParamTemplate[k] = val
	}
	return t
}

// WithPagination 设置分页策略。
func (t *BaseCollectTask) WithPagination(v Pagination) *BaseCollectTask {
	t.Pagination = v
	return t
}

// WithDataPaths 设置记录提取路径候选。
func (t *BaseCollectTask) WithDataPaths(v []string) *BaseCollectTask {
	if len(v) > 0 {
		t.DataPaths = append([]string{}, v...)
	}
	return t
}

// WithParallelism 设置参数组合并发度。
func (t *BaseCollectTask) WithParallelism(v int) *BaseCollectTask {
	if v > 0 {
		t.Parallelism = v
	}
	return t
}

// WithCursorPrefix 设置游标 key 前缀。
func (t *BaseCollectTask) WithCursorPrefix(v string) *BaseCollectTask {
	t.CursorPrefix = v
	return t
}

// WithWindow 设置当前窗口。
func (t *BaseCollectTask) WithWindow(start, end time.Time) *BaseCollectTask {
	t.WindowStart = &start
	t.WindowEnd = &end
	return t
}

// Clone 返回任务副本，避免并发执行时共享 map/slice 引发数据竞争。
func (t *BaseCollectTask) Clone() *BaseCollectTask {
	// 先做浅拷贝。
	cp := *t
	// map 需要深拷贝。
	cp.Headers = mapCloneString(t.Headers)
	cp.ParamTemplate = mapCloneAny(t.ParamTemplate)
	// slice 需要深拷贝。
	cp.DataPaths = append([]string{}, t.DataPaths...)
	cp.Pagination.TotalPathCandidates = append([]string{}, t.Pagination.TotalPathCandidates...)
	return &cp
}

// mapCloneString 复制 map[string]string。
func mapCloneString(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}

// mapCloneAny 复制 map[string]any。
func mapCloneAny(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}
