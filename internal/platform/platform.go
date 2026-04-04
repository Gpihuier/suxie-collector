package platform

import (
	"fmt"
	"strings"

	"suxie.com/suxie-collector/internal/collector"
)

type Registry struct {
	fetchers map[string]collector.Fetcher
}

func NewRegistry() *Registry {
	return &Registry{fetchers: map[string]collector.Fetcher{}}
}

func (r *Registry) Register(name string, f collector.Fetcher) {
	if r.fetchers == nil {
		r.fetchers = map[string]collector.Fetcher{}
	}
	r.fetchers[strings.ToLower(strings.TrimSpace(name))] = f
}

func (r *Registry) Fetcher(name string) (collector.Fetcher, error) {
	f, ok := r.fetchers[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return nil, fmt.Errorf("platform fetcher not found: %s", name)
	}
	return f, nil
}
