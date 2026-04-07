package runner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"suxie.com/suxie-collector/internal/collector"
	"suxie.com/suxie-collector/internal/config"
	"suxie.com/suxie-collector/internal/platform"
)

type Notifier interface {
	SendMarkdown(ctx context.Context, title, text string) error
}

type compiledJob struct {
	Task      *collector.BaseCollectTask
	Timezone  string
	Providers []collector.VariableProvider
	Fetcher   collector.Fetcher
	Limiter   *rate.Limiter
	Every     time.Duration
}

// Runner 负责“任务编排 + 定时分发 + worker 并发执行”。
type Runner struct {
	logger      *slog.Logger
	workerCount int
	queueSize   int

	engine   *collector.Engine
	tasks    config.TasksConfig
	platform *platform.Registry
	notifier Notifier
}

func New(logger *slog.Logger, workerCount, queueSize int, engine *collector.Engine, tasks config.TasksConfig, platformRegistry *platform.Registry, notifier Notifier) *Runner {
	if workerCount <= 0 {
		workerCount = 8
	}
	if queueSize <= 0 {
		queueSize = 128
	}
	return &Runner{
		logger:      logger,
		workerCount: workerCount,
		queueSize:   queueSize,
		engine:      engine,
		tasks:       tasks,
		platform:    platformRegistry,
		notifier:    notifier,
	}
}

// Start 启动两类协程：
// 1) dispatcher：按周期把任务投递到执行队列
// 2) worker：消费执行队列并调用采集引擎
func (r *Runner) Start(ctx context.Context) error {
	jobs, err := r.compileJobs()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		r.logger.Warn("no enabled jobs found")
		return nil
	}

	// execQueue 作为调度层与执行层之间的解耦缓冲。
	execQueue := make(chan compiledJob, r.queueSize)
	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < r.workerCount; i++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case job, ok := <-execQueue:
					if !ok {
						return nil
					}
					// 执行任务
					// 每次执行都 clone 任务，避免并发写共享对象。
					if err := r.engine.Execute(gctx, collector.ExecuteInput{
						Task:      job.Task.Clone(),
						Timezone:  job.Timezone,
						Providers: job.Providers,
						Fetcher:   job.Fetcher,
						Limiter:   job.Limiter,
					}); err != nil {
						r.logger.Error("execute collect job failed",
							"tenant", job.Task.TenantID,
							"platform", job.Task.Platform,
							"job", job.Task.JobName,
							"err", err,
						)
						if r.notifier != nil {
							_ = r.notifier.SendMarkdown(gctx,
								"采集任务失败",
								fmt.Sprintf("租户: %s\n\n任务: %s\n\n错误: %v", job.Task.TenantID, job.Task.JobName, err),
							)
						}
					}
				}
			}
		})
	}

	for _, compiled := range jobs {
		compiled := compiled
		g.Go(func() error {
			// 每个编译后的任务有自己独立的 ticker。
			ticker := time.NewTicker(compiled.Every)
			defer ticker.Stop()

			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case execQueue <- compiled:
					// 开始执行任务说明
					r.logger.Info("dispatch collect task",
						"tenant", compiled.Task.TenantID,
						"platform", compiled.Task.Platform,
						"job", compiled.Task.JobName,
					)
				}

				select {
				case <-gctx.Done():
					return gctx.Err()
				case <-ticker.C:
				}
			}
		})
	}

	err = g.Wait()
	close(execQueue)
	return err
}

func (r *Runner) compileJobs() ([]compiledJob, error) {
	// compileJobs 把“配置”转换为“可执行任务对象”，集中做校验与默认值处理。
	compiled := make([]compiledJob, 0)

	for _, tenant := range r.tasks.Tenants {
		if !tenant.Enabled {
			continue
		}
		fetcher, err := r.platform.Fetcher(tenant.Platform)
		if err != nil {
			return nil, err
		}

		for _, job := range tenant.Jobs {
			if !job.Enabled {
				continue
			}

			every, err := time.ParseDuration(job.Schedule.Every)
			if err != nil || every <= 0 {
				return nil, fmt.Errorf("invalid job schedule every tenant=%s job=%s: %s", tenant.TenantID, job.Name, job.Schedule.Every)
			}

			providers, err := collector.BuildProviders(append(append([]config.VariableConfig{}, tenant.Variables...), job.Variables...))
			if err != nil {
				return nil, fmt.Errorf("build variables tenant=%s job=%s: %w", tenant.TenantID, job.Name, err)
			}

			limiterCfg := tenant.RateLimit
			if job.RateLimit != nil {
				limiterCfg = *job.RateLimit
			}
			// 限流优先级：job.rate_limit > tenant.rate_limit。
			limiter := rate.NewLimiter(rate.Limit(limiterCfg.RPS), limiterCfg.Burst)

			task := collector.NewBaseCollectTask().
				WithTenantID(tenant.TenantID).
				WithPlatform(strings.ToLower(strings.TrimSpace(tenant.Platform))).
				WithJobName(job.Name).
				WithMethod(job.Method).
				WithEndpoint(job.Endpoint).
				WithRequestIn(job.RequestIn).
				WithHeaders(job.Headers).
				WithParams(job.Params).
				WithDataPaths(job.DataPathCandidates).
				WithParallelism(job.Parallelism)
			task.WithPagination(collector.Pagination{
				Enabled:             job.Pagination.Enabled,
				StartPage:           job.Pagination.StartPage,
				PageParam:           job.Pagination.PageParam,
				PageSizeParam:       job.Pagination.PageSizeParam,
				PageSize:            job.Pagination.PageSize,
				TotalPathCandidates: job.Pagination.TotalPathCandidates,
			})

			compiled = append(compiled, compiledJob{
				Task:      task,
				Timezone:  tenant.Timezone,
				Providers: providers,
				Fetcher:   fetcher,
				Limiter:   limiter,
				Every:     every,
			})
		}
	}

	return compiled, nil
}
