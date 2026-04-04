package collector

import "time"

// BaseCollectTask 是采集任务的基础结构体，使用 WithXXX 链式构建。
type BaseCollectTask struct {
	TenantID      string
	Platform      string
	JobName       string
	Method        string
	Endpoint      string
	RequestIn     string
	Headers       map[string]string
	ParamTemplate map[string]any

	Pagination  Pagination
	DataPaths   []string
	Parallelism int

	CursorPrefix string
	WindowStart  *time.Time
	WindowEnd    *time.Time
}

type Pagination struct {
	Enabled             bool
	StartPage           int
	PageParam           string
	PageSizeParam       string
	PageSize            int
	TotalPathCandidates []string
}

func NewBaseCollectTask() *BaseCollectTask {
	return &BaseCollectTask{
		Method:        "GET",
		RequestIn:     "query",
		Headers:       make(map[string]string),
		ParamTemplate: make(map[string]any),
		Pagination: Pagination{
			Enabled:             true,
			StartPage:           1,
			PageParam:           "page",
			PageSizeParam:       "limit",
			PageSize:            50,
			TotalPathCandidates: []string{"data.total", "Data.Total", "total", "Total"},
		},
		DataPaths:   []string{"data.list", "data.data", "Data.List", "Data.Data", "list", "data"},
		Parallelism: 1,
	}
}

func (t *BaseCollectTask) WithTenantID(v string) *BaseCollectTask {
	t.TenantID = v
	return t
}

func (t *BaseCollectTask) WithPlatform(v string) *BaseCollectTask {
	t.Platform = v
	return t
}

func (t *BaseCollectTask) WithJobName(v string) *BaseCollectTask {
	t.JobName = v
	return t
}

func (t *BaseCollectTask) WithMethod(v string) *BaseCollectTask {
	if v != "" {
		t.Method = v
	}
	return t
}

func (t *BaseCollectTask) WithEndpoint(v string) *BaseCollectTask {
	t.Endpoint = v
	return t
}

func (t *BaseCollectTask) WithRequestIn(v string) *BaseCollectTask {
	if v != "" {
		t.RequestIn = v
	}
	return t
}

func (t *BaseCollectTask) WithHeader(k, v string) *BaseCollectTask {
	if t.Headers == nil {
		t.Headers = make(map[string]string)
	}
	t.Headers[k] = v
	return t
}

func (t *BaseCollectTask) WithHeaders(v map[string]string) *BaseCollectTask {
	if t.Headers == nil {
		t.Headers = make(map[string]string)
	}
	for k, val := range v {
		t.Headers[k] = val
	}
	return t
}

func (t *BaseCollectTask) WithParam(k string, v any) *BaseCollectTask {
	if t.ParamTemplate == nil {
		t.ParamTemplate = make(map[string]any)
	}
	t.ParamTemplate[k] = v
	return t
}

func (t *BaseCollectTask) WithParams(v map[string]any) *BaseCollectTask {
	if t.ParamTemplate == nil {
		t.ParamTemplate = make(map[string]any)
	}
	for k, val := range v {
		t.ParamTemplate[k] = val
	}
	return t
}

func (t *BaseCollectTask) WithPagination(v Pagination) *BaseCollectTask {
	t.Pagination = v
	return t
}

func (t *BaseCollectTask) WithDataPaths(v []string) *BaseCollectTask {
	if len(v) > 0 {
		t.DataPaths = append([]string{}, v...)
	}
	return t
}

func (t *BaseCollectTask) WithParallelism(v int) *BaseCollectTask {
	if v > 0 {
		t.Parallelism = v
	}
	return t
}

func (t *BaseCollectTask) WithCursorPrefix(v string) *BaseCollectTask {
	t.CursorPrefix = v
	return t
}

func (t *BaseCollectTask) WithWindow(start, end time.Time) *BaseCollectTask {
	t.WindowStart = &start
	t.WindowEnd = &end
	return t
}

func (t *BaseCollectTask) Clone() *BaseCollectTask {
	cp := *t
	cp.Headers = mapCloneString(t.Headers)
	cp.ParamTemplate = mapCloneAny(t.ParamTemplate)
	cp.DataPaths = append([]string{}, t.DataPaths...)
	cp.Pagination.TotalPathCandidates = append([]string{}, t.Pagination.TotalPathCandidates...)
	return &cp
}

func mapCloneString(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}

func mapCloneAny(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}
