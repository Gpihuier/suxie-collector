package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"suxie.com/suxie-collector/internal/collector"
	"suxie.com/suxie-collector/internal/config"
	"suxie.com/suxie-collector/internal/logging"
	"suxie.com/suxie-collector/internal/metrics"
	"suxie.com/suxie-collector/internal/notifier/dingtalk"
	"suxie.com/suxie-collector/internal/platform"
	"suxie.com/suxie-collector/internal/platform/lingxing"
	"suxie.com/suxie-collector/internal/queue"
	"suxie.com/suxie-collector/internal/runner"
	"suxie.com/suxie-collector/internal/storage"
)

type Application struct {
	cfg      config.AppConfig
	tasksCfg config.TasksConfig
	logger   *slog.Logger
}

func New(configPath, tasksPath string) (*Application, error) {
	appCfg, err := config.LoadAppConfig(configPath)
	if err != nil {
		return nil, err
	}

	if tasksPath == "" {
		tasksPath = appCfg.App.TasksFile
	}
	if tasksPath == "" {
		return nil, fmt.Errorf("tasks config path is empty")
	}

	tasksCfg, err := config.LoadTasksConfig(tasksPath)
	if err != nil {
		return nil, err
	}

	logger := logging.NewLogger(appCfg.Logging)
	slog.SetDefault(logger)

	return &Application{
		cfg:      appCfg,
		tasksCfg: tasksCfg,
		logger:   logger,
	}, nil
}

func (a *Application) Run(ctx context.Context) error {
	reg, collectorMetrics := metrics.NewRegistry()

	if a.cfg.Metrics.Enable {
		mux := http.NewServeMux()
		mux.Handle(a.cfg.Metrics.Path, metrics.NewHTTPHandler(reg))
		server := &http.Server{Addr: a.cfg.Metrics.Addr, Handler: mux}

		go func() {
			a.logger.Info("metrics server started", "addr", a.cfg.Metrics.Addr, "path", a.cfg.Metrics.Path)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				a.logger.Error("metrics server failed", "err", err)
			}
		}()

		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
	}

	redisClient, err := storage.NewRedisClient(a.cfg.Redis)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	cursorStore := storage.NewRedisCursorStore(redisClient)

	producer, err := queue.NewProducer(a.cfg.RabbitMQ)
	if err != nil {
		return err
	}
	defer producer.Close()
	publisher := queue.NewMQPublisher(producer, a.cfg.RabbitMQ.RoutingKey)

	platformRegistry := platform.NewRegistry()
	platformRegistry.Register("lingxing", lingxing.NewClient(a.cfg.Lingxing, a.cfg.TimeoutDuration(), a.logger))

	notifier := dingtalk.NewClient(
		a.cfg.Notification.DingTalk.Enable,
		a.cfg.Notification.DingTalk.Webhook,
		a.cfg.Notification.DingTalk.Secret,
		a.logger,
	)

	engine := collector.NewEngine(a.logger, collectorMetrics, cursorStore, publisher)
	runnerSvc := runner.New(a.logger, a.cfg.Runner.WorkerCount, a.cfg.Runner.QueueSize, engine, a.tasksCfg, platformRegistry, notifier)

	a.logger.Info("collector service started", "id", a.cfg.App.ID, "name", a.cfg.App.Name, "env", a.cfg.App.Env)
	err = runnerSvc.Start(ctx)
	if err != nil && err != context.Canceled {
		return err
	}
	return nil
}
