package collector

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"suxie.com/suxie-collector/internal/metrics"
	"suxie.com/suxie-collector/internal/queue"
	"suxie.com/suxie-collector/internal/storage"
)

type MessagePublisher interface {
	Publish(ctx context.Context, msg queue.CollectMessage) error
}

type ExecuteInput struct {
	Task      *BaseCollectTask
	Timezone  string
	Providers []VariableProvider
	Fetcher   Fetcher
	Limiter   *rate.Limiter
}

// Engine 负责编排“变量展开 -> 分页采集 -> 游标持久化 -> MQ投递”完整链路。
type Engine struct {
	logger    *slog.Logger
	metrics   *metrics.CollectorMetrics
	cursors   storage.CursorStore
	publisher MessagePublisher
}

func NewEngine(logger *slog.Logger, m *metrics.CollectorMetrics, cursors storage.CursorStore, publisher MessagePublisher) *Engine {
	return &Engine{
		logger:    logger,
		metrics:   m,
		cursors:   cursors,
		publisher: publisher,
	}
}

func (e *Engine) Execute(ctx context.Context, in ExecuteInput) error {
	if in.Task == nil {
		return errors.New("task is nil")
	}
	if in.Fetcher == nil {
		return errors.New("fetcher is nil")
	}

	pagination, err := EnsurePagination(in.Task.Pagination)
	if err != nil {
		return err
	}
	in.Task.Pagination = pagination

	baseCursorKey := in.Task.CursorPrefix
	if baseCursorKey == "" {
		baseCursorKey = fmt.Sprintf("collector:cursor:%s:%s:%s", in.Task.Platform, in.Task.TenantID, in.Task.JobName)
	}

	baseCursorState, err := e.cursors.Get(ctx, baseCursorKey)
	if err != nil && !errors.Is(err, storage.ErrCursorNotFound) {
		return fmt.Errorf("load base cursor: %w", err)
	}

	vars, err := ResolveVariables(ctx, ResolveInput{
		TenantID: in.Task.TenantID,
		JobName:  in.Task.JobName,
		Timezone: in.Timezone,
		Now:      time.Now(),
		Cursor:   baseCursorState,
	}, in.Providers)
	if err != nil {
		return err
	}
	paramSets := RenderParamSets(in.Task.ParamTemplate, vars)
	if len(paramSets) == 0 {
		paramSets = []map[string]any{{}}
	}

	workerCount := in.Task.Parallelism
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(paramSets) {
		workerCount = len(paramSets)
	}
	if workerCount == 0 {
		workerCount = 1
	}

	// jobs 用于分发不同参数组合（例如不同店铺/站点）的采集任务。
	jobs := make(chan map[string]any)
	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < workerCount; i++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case params, ok := <-jobs:
					if !ok {
						return nil
					}
					if err := e.collectByParams(gctx, in, params, vars, baseCursorKey); err != nil {
						return err
					}
				}
			}
		})
	}

	// 生产者协程：把参数集推入 jobs，由 worker 并发消费。
	g.Go(func() error {
		defer close(jobs)
		for _, params := range paramSets {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case jobs <- params:
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if vars.NextWindowEnd != nil {
		baseCursorState.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
		baseCursorState.LastSuccessAt = time.Now().UTC()
		baseCursorState.NextPage = in.Task.Pagination.StartPage
		if err := e.cursors.Set(ctx, baseCursorKey, baseCursorState); err != nil {
			e.logger.Error("persist base cursor failed", "cursor_key", baseCursorKey, "err", err)
		}
	}

	return nil
}

func (e *Engine) collectByParams(
	ctx context.Context,
	in ExecuteInput,
	params map[string]any,
	vars ResolveOutput,
	baseCursorKey string,
) error {
	pageCursorKey := buildPageCursorKey(baseCursorKey, params, in.Task.Pagination)
	pageCursor, err := e.cursors.Get(ctx, pageCursorKey)
	if err != nil && !errors.Is(err, storage.ErrCursorNotFound) {
		return fmt.Errorf("load page cursor: %w", err)
	}

	page := in.Task.Pagination.StartPage
	if pageCursor.NextPage > 0 {
		page = pageCursor.NextPage
	}

	requestID := uuid.NewString()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 每页请求动态注入分页参数，兼容 page/limit 等不同字段名。
		requestParams := BuildPageParams(params, in.Task.Pagination, page)
		if in.Limiter != nil {
			e.metrics.RateLimited.WithLabelValues(in.Task.TenantID, in.Task.JobName).Inc()
			if err := in.Limiter.Wait(ctx); err != nil {
				return fmt.Errorf("wait rate limiter: %w", err)
			}
		}

		start := time.Now()
		resp, err := in.Fetcher.Fetch(ctx, FetchRequest{
			TenantID:  in.Task.TenantID,
			Method:    in.Task.Method,
			Endpoint:  in.Task.Endpoint,
			RequestIn: in.Task.RequestIn,
			Headers:   in.Task.Headers,
			Params:    requestParams,
		})
		e.metrics.CollectDuration.WithLabelValues(in.Task.TenantID, in.Task.JobName).Observe(time.Since(start).Seconds())
		if err != nil {
			e.metrics.CollectRequests.WithLabelValues(in.Task.TenantID, in.Task.JobName, "error").Inc()
			return fmt.Errorf("fetch page=%d: %w", page, err)
		}
		e.metrics.CollectRequests.WithLabelValues(in.Task.TenantID, in.Task.JobName, "success").Inc()

		// 兼容 Data/data/外层的 total 与 records 返回结构差异。
		records, _ := ExtractRecords(resp, in.Task.DataPaths)
		total, totalKnown := ExtractTotal(resp, in.Task.Pagination.TotalPathCandidates)

		msg := queue.CollectMessage{
			TenantID:      in.Task.TenantID,
			Platform:      in.Task.Platform,
			JobName:       in.Task.JobName,
			RequestID:     requestID,
			Page:          page,
			Total:         total,
			Records:       records,
			Raw:           resp,
			RequestParams: requestParams,
			CollectedAt:   time.Now().UTC(),
		}

		if err := e.publisher.Publish(ctx, msg); err != nil {
			e.metrics.PublishTotal.WithLabelValues(in.Task.TenantID, in.Task.JobName, "error").Inc()
			return fmt.Errorf("publish message: %w", err)
		}
		e.metrics.PublishTotal.WithLabelValues(in.Task.TenantID, in.Task.JobName, "success").Inc()
		e.metrics.CollectRecords.WithLabelValues(in.Task.TenantID, in.Task.JobName).Add(float64(len(records)))

		pageCursor.NextPage = page + 1
		pageCursor.LastSuccessAt = time.Now().UTC()
		if vars.NextWindowEnd != nil {
			pageCursor.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
		}
		if err := e.cursors.Set(ctx, pageCursorKey, pageCursor); err != nil {
			e.logger.Warn("set page cursor failed", "cursor_key", pageCursorKey, "err", err)
		}

		if !in.Task.Pagination.Enabled {
			break
		}
		// total 已知按 total 判断；未知时按“是否满页”继续翻页。
		if !NeedNextPage(page, in.Task.Pagination.PageSize, len(records), total, totalKnown) {
			break
		}
		page++
	}

	pageCursor.NextPage = in.Task.Pagination.StartPage
	pageCursor.LastSuccessAt = time.Now().UTC()
	if vars.NextWindowEnd != nil {
		pageCursor.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
	}
	if err := e.cursors.Set(ctx, pageCursorKey, pageCursor); err != nil {
		e.logger.Warn("reset page cursor failed", "cursor_key", pageCursorKey, "err", err)
	}

	if vars.CurrentWindowStart != nil && vars.CurrentWindowEnd != nil {
		e.logger.Info("collection window finished",
			"tenant", in.Task.TenantID,
			"job", in.Task.JobName,
			"window_start", vars.CurrentWindowStart.Format(time.RFC3339),
			"window_end", vars.CurrentWindowEnd.Format(time.RFC3339),
		)
	}

	return nil
}

func buildPageCursorKey(base string, params map[string]any, p Pagination) string {
	data := make(map[string]any, len(params))
	for k, v := range params {
		kLower := strings.ToLower(k)
		if k == p.PageParam || k == p.PageSizeParam {
			continue
		}
		// 时间窗口参数不参与游标哈希，避免每个时间片都产生新游标键。
		if strings.Contains(kLower, "date") || strings.Contains(kLower, "time") || strings.Contains(kLower, "start") || strings.Contains(kLower, "end") {
			continue
		}
		data[k] = v
	}
	if len(data) == 0 {
		return base + ":default"
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ordered := make(map[string]any, len(data))
	for _, k := range keys {
		ordered[k] = data[k]
	}

	body, _ := json.Marshal(ordered)
	hash := sha1.Sum(body)
	return base + ":" + hex.EncodeToString(hash[:8])
}
