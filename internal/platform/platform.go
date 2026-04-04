package platform

import (
	"fmt"
	"strings"

	"suxie.com/suxie-collector/internal/collector"
)

// Registry 保存“平台名 -> Fetcher”的映射关系。
type Registry struct {
	fetchers map[string]collector.Fetcher
}

func NewRegistry() *Registry {
	return &Registry{fetchers: map[string]collector.Fetcher{}}
}

// Register 注册平台实现，名称按小写存储。
func (r *Registry) Register(name string, f collector.Fetcher) {
	if r.fetchers == nil {
		r.fetchers = map[string]collector.Fetcher{}
	}
	r.fetchers[strings.ToLower(strings.TrimSpace(name))] = f
}

// Fetcher 按平台名获取实现。
func (r *Registry) Fetcher(name string) (collector.Fetcher, error) {
	f, ok := r.fetchers[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return nil, fmt.Errorf("platform fetcher not found: %s", name)
	}
	return f, nil
}
