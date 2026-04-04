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

// MessagePublisher 抽象消息发布能力。
// 引擎不依赖具体 MQ 实现，便于后续替换成 Kafka 等。
type MessagePublisher interface {
	// Publish 把采集结果发布到消息系统。
	Publish(ctx context.Context, msg queue.CollectMessage) error
}

// ExecuteInput 是每次执行任务的输入上下文。
type ExecuteInput struct {
	// Task 是执行任务定义。
	Task *BaseCollectTask
	// Timezone 用于变量解析（例如 date_window）。
	Timezone string
	// Providers 是动态变量来源集合。
	Providers []VariableProvider
	// Fetcher 是平台请求适配器。
	Fetcher Fetcher
	// Limiter 是任务级限流器（可为空）。
	Limiter *rate.Limiter
}

// Engine 负责单个任务的完整执行链路：
// 1) 变量解析
// 2) 参数展开
// 3) 分页请求
// 4) 游标持久化
// 5) MQ 投递
type Engine struct {
	// logger 用于输出运行日志。
	logger *slog.Logger
	// metrics 用于记录观测指标。
	metrics *metrics.CollectorMetrics
	// cursors 用于持久化断点。
	cursors storage.CursorStore
	// publisher 用于输出采集结果。
	publisher MessagePublisher
}

// NewEngine 创建采集引擎实例。
func NewEngine(logger *slog.Logger, m *metrics.CollectorMetrics, cursors storage.CursorStore, publisher MessagePublisher) *Engine {
	return &Engine{
		logger:    logger,
		metrics:   m,
		cursors:   cursors,
		publisher: publisher,
	}
}

// Execute 执行一个采集任务。
func (e *Engine) Execute(ctx context.Context, in ExecuteInput) error {
	// 基础参数校验：任务不能为空。
	if in.Task == nil {
		return errors.New("task is nil")
	}
	// 基础参数校验：平台拉取器不能为空。
	if in.Fetcher == nil {
		return errors.New("fetcher is nil")
	}

	// 归一化分页配置并做合法性校验。
	pagination, err := EnsurePagination(in.Task.Pagination)
	if err != nil {
		return err
	}
	// 将归一化结果回写到任务对象。
	in.Task.Pagination = pagination

	// 生成“基础游标 key”，用于记录窗口进度。
	baseCursorKey := in.Task.CursorPrefix
	if baseCursorKey == "" {
		baseCursorKey = fmt.Sprintf("collector:cursor:%s:%s:%s", in.Task.Platform, in.Task.TenantID, in.Task.JobName)
	}

	// 读取基础游标（不存在是正常情况）。
	baseCursorState, err := e.cursors.Get(ctx, baseCursorKey)
	if err != nil && !errors.Is(err, storage.ErrCursorNotFound) {
		return fmt.Errorf("load base cursor: %w", err)
	}

	// 解析变量（静态值、列表、时间窗口等）。
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

	// 用变量渲染参数模板，得到参数组合。
	paramSets := RenderParamSets(in.Task.ParamTemplate, vars)
	if len(paramSets) == 0 {
		// 没有参数模板时至少执行一次空参数请求。
		paramSets = []map[string]any{{}}
	}

	// 计算参数组合并发度。
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

	// jobs 负责分发不同参数组合到 worker。
	jobs := make(chan map[string]any)
	g, gctx := errgroup.WithContext(ctx)

	// 启动参数组合 worker。
	for i := 0; i < workerCount; i++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					// 上下文结束时退出。
					return gctx.Err()
				case params, ok := <-jobs:
					if !ok {
						// jobs 关闭说明生产结束。
						return nil
					}
					// 对单组参数执行完整采集（含分页）。
					if err := e.collectByParams(gctx, in, params, vars, baseCursorKey); err != nil {
						return err
					}
				}
			}
		})
	}

	// 生产者协程：把所有参数组合推送到 jobs。
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

	// 等待所有参数组合执行完成。
	if err := g.Wait(); err != nil {
		return err
	}

	// 如果本次执行包含时间窗口，则更新基础窗口游标。
	if vars.NextWindowEnd != nil {
		baseCursorState.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
		baseCursorState.LastSuccessAt = time.Now().UTC()
		// 窗口级游标完成后，将页码游标重置为起始页。
		baseCursorState.NextPage = in.Task.Pagination.StartPage
		if err := e.cursors.Set(ctx, baseCursorKey, baseCursorState); err != nil {
			e.logger.Error("persist base cursor failed", "cursor_key", baseCursorKey, "err", err)
		}
	}

	return nil
}

// collectByParams 处理一组“已渲染参数”的分页采集流程。
func (e *Engine) collectByParams(
	ctx context.Context,
	in ExecuteInput,
	params map[string]any,
	vars ResolveOutput,
	baseCursorKey string,
) error {
	// 基于参数构造分页游标 key（忽略分页和时间字段）。
	pageCursorKey := buildPageCursorKey(baseCursorKey, params, in.Task.Pagination)
	// 读取分页游标。
	pageCursor, err := e.cursors.Get(ctx, pageCursorKey)
	if err != nil && !errors.Is(err, storage.ErrCursorNotFound) {
		return fmt.Errorf("load page cursor: %w", err)
	}

	// 计算起始页：优先用上次断点。
	page := in.Task.Pagination.StartPage
	if pageCursor.NextPage > 0 {
		page = pageCursor.NextPage
	}

	// 同一参数组内复用一个 requestID，便于链路追踪。
	requestID := uuid.NewString()
	for {
		// 每一页开始前检查 context。
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 注入分页参数（兼容 page/limit/offset/length 等字段名）。
		requestParams := BuildPageParams(params, in.Task.Pagination, page)
		if in.Limiter != nil {
			// 记录一次“即将等待限流”的计数。
			e.metrics.RateLimited.WithLabelValues(in.Task.TenantID, in.Task.JobName).Inc()
			// 按令牌桶节奏等待。
			if err := in.Limiter.Wait(ctx); err != nil {
				return fmt.Errorf("wait rate limiter: %w", err)
			}
		}

		// 记录请求耗时。
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
			// 请求失败打 error 指标并返回。
			e.metrics.CollectRequests.WithLabelValues(in.Task.TenantID, in.Task.JobName, "error").Inc()
			return fmt.Errorf("fetch page=%d: %w", page, err)
		}
		// 请求成功打 success 指标。
		e.metrics.CollectRequests.WithLabelValues(in.Task.TenantID, in.Task.JobName, "success").Inc()

		// 提取 records 与 total（total 可能在不同层级）。
		records, _ := ExtractRecords(resp, in.Task.DataPaths)
		total, totalKnown := ExtractTotal(resp, in.Task.Pagination.TotalPathCandidates)

		// 组装要投递给 MQ 的标准消息。
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

		// 发布消息到 MQ。
		if err := e.publisher.Publish(ctx, msg); err != nil {
			e.metrics.PublishTotal.WithLabelValues(in.Task.TenantID, in.Task.JobName, "error").Inc()
			return fmt.Errorf("publish message: %w", err)
		}
		e.metrics.PublishTotal.WithLabelValues(in.Task.TenantID, in.Task.JobName, "success").Inc()
		e.metrics.CollectRecords.WithLabelValues(in.Task.TenantID, in.Task.JobName).Add(float64(len(records)))

		// 写入分页游标：下一页 + 最近成功时间。
		pageCursor.NextPage = page + 1
		pageCursor.LastSuccessAt = time.Now().UTC()
		if vars.NextWindowEnd != nil {
			pageCursor.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
		}
		if err := e.cursors.Set(ctx, pageCursorKey, pageCursor); err != nil {
			e.logger.Warn("set page cursor failed", "cursor_key", pageCursorKey, "err", err)
		}

		// 未开启分页则单次请求后结束。
		if !in.Task.Pagination.Enabled {
			break
		}
		// 开启分页时决定是否继续翻页。
		if !NeedNextPage(page, in.Task.Pagination.PageSize, len(records), total, totalKnown) {
			break
		}
		// 进入下一页。
		page++
	}

	// 参数组执行完成后，重置 NextPage，确保下轮从起始页开始。
	pageCursor.NextPage = in.Task.Pagination.StartPage
	pageCursor.LastSuccessAt = time.Now().UTC()
	if vars.NextWindowEnd != nil {
		pageCursor.LastWindowEnd = vars.NextWindowEnd.UTC().Format(time.RFC3339)
	}
	if err := e.cursors.Set(ctx, pageCursorKey, pageCursor); err != nil {
		e.logger.Warn("reset page cursor failed", "cursor_key", pageCursorKey, "err", err)
	}

	// 打一条窗口完成日志，方便观察增量推进。
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

// buildPageCursorKey 构造参数级分页游标 key。
// 设计要点：
// 1) 忽略分页参数，避免每一页生成不同 key。
// 2) 忽略时间参数，避免每个时间窗口生成不同 key。
// 3) 其余参数做稳定排序后哈希，保证 key 长度可控。
func buildPageCursorKey(base string, params map[string]any, p Pagination) string {
	// 先过滤出参与哈希的参数。
	data := make(map[string]any, len(params))
	for k, v := range params {
		kLower := strings.ToLower(k)
		if k == p.PageParam || k == p.PageSizeParam {
			continue
		}
		if strings.Contains(kLower, "date") || strings.Contains(kLower, "time") || strings.Contains(kLower, "start") || strings.Contains(kLower, "end") {
			continue
		}
		data[k] = v
	}
	// 过滤后为空时，使用 default 后缀。
	if len(data) == 0 {
		return base + ":default"
	}

	// 对 key 做排序，确保哈希稳定。
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 按排序结果构造有序 map 再序列化。
	ordered := make(map[string]any, len(data))
	for _, k := range keys {
		ordered[k] = data[k]
	}

	// 计算短哈希并拼接到 base key。
	body, _ := json.Marshal(ordered)
	hash := sha1.Sum(body)
	return base + ":" + hex.EncodeToString(hash[:8])
}
