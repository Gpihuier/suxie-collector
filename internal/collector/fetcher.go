package collector

import "context"

type FetchRequest struct {
	TenantID  string
	Method    string
	Endpoint  string
	RequestIn string
	Headers   map[string]string
	Params    map[string]any
}

type Fetcher interface {
	Platform() string
	Fetch(ctx context.Context, req FetchRequest) (map[string]any, error)
}
