package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CollectorMetrics 聚合采集服务关键指标，便于统一注入和调用。
type CollectorMetrics struct {
	CollectRequests *prometheus.CounterVec
	CollectRecords  *prometheus.CounterVec
	CollectDuration *prometheus.HistogramVec
	PublishTotal    *prometheus.CounterVec
	ErrorTotal      *prometheus.CounterVec
	RateLimited     *prometheus.CounterVec
}

// NewCollectorMetrics 初始化业务指标。
func NewCollectorMetrics(reg prometheus.Registerer) *CollectorMetrics {
	factory := promauto.With(reg)

	return &CollectorMetrics{
		CollectRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "suxie",
			Subsystem: "collector",
			Name:      "requests_total",
			Help:      "Total number of collection requests",
		}, []string{"tenant", "job", "status"}),
		CollectRecords: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "suxie",
			Subsystem: "collector",
			Name:      "records_total",
			Help:      "Total number of collected records",
		}, []string{"tenant", "job"}),
		CollectDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "suxie",
			Subsystem: "collector",
			Name:      "duration_seconds",
			Help:      "Collection latency in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"tenant", "job"}),
		PublishTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "suxie",
			Subsystem: "queue",
			Name:      "publish_total",
			Help:      "Total number of messages published to queue",
		}, []string{"tenant", "job", "status"}),
		ErrorTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "suxie",
			Subsystem: "collector",
			Name:      "errors_total",
			Help:      "Total number of internal errors by component",
		}, []string{"component"}),
		RateLimited: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "suxie",
			Subsystem: "collector",
			Name:      "rate_limited_total",
			Help:      "Times a task waited due to rate limit",
		}, []string{"tenant", "job"}),
	}
}

// NewRegistry 创建 Prometheus Registry 并注册 Go/Process 基础指标。
func NewRegistry() (*prometheus.Registry, *CollectorMetrics) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return reg, NewCollectorMetrics(reg)
}

// NewHTTPHandler 返回 /metrics HTTP handler。
func NewHTTPHandler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// ObserveDuration 是简化的耗时观测辅助函数。
func ObserveDuration(start time.Time, fn func(seconds float64)) {
	fn(time.Since(start).Seconds())
}
