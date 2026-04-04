package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"

	"suxie.com/suxie-collector/internal/config"
)

// NewLogger 初始化 JSON slog，并支持 stdout + 文件滚动双写。
func NewLogger(cfg config.LoggingConfig) *slog.Logger {
	level := parseLevel(cfg.Level)

	writers := []io.Writer{os.Stdout}
	if cfg.FilePath != "" {
		writers = append(writers, &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
			Compress:   cfg.Compress,
		})
	}

	handler := slog.NewJSONHandler(io.MultiWriter(writers...), &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})
	return slog.New(handler)
}

// parseLevel 将字符串日志级别转换为 slog.Level。
func parseLevel(v string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
