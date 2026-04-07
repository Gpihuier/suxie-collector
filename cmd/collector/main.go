package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"suxie.com/suxie-collector/internal/app"
)

// main 负责参数解析与进程级生命周期控制。
func main() {
	var (
		configPath string
		tasksPath  string
	)
	flag.StringVar(&configPath, "config", "configs/app.yaml", "path to app config yaml")
	flag.StringVar(&tasksPath, "tasks", "", "path to tasks config yaml (optional, override app config)")
	flag.Parse()

	if err := runWithGracefulReload(configPath, tasksPath); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "collector exit with error: %v\n", err)
		return
	}
}

// runWithGracefulReload 支持两类信号：
// 1) SIGINT/SIGTERM: 优雅停机
// 2) SIGHUP: 优雅重载（停止当前实例并重建）
func runWithGracefulReload(configPath, tasksPath string) error {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reloadSig := make(chan os.Signal, 1)
	signal.Notify(reloadSig, syscall.SIGHUP)
	defer signal.Stop(reloadSig)

	for {
		instance, err := app.New(configPath, tasksPath)
		if err != nil {
			return fmt.Errorf("init app failed: %w", err)
		}

		runCtx, cancel := context.WithCancel(rootCtx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- instance.Run(runCtx)
		}()

		select {
		case <-rootCtx.Done():
			cancel()
			err = <-errCh
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		case <-reloadSig:
			cancel()
			err = <-errCh
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			continue
		case err = <-errCh:
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			if rootCtx.Err() != nil {
				return nil
			}
			return nil
		}
	}
}
