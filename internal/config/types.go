package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// AppConfig 是程序运行的全局配置。
type AppConfig struct {
	App          AppSection         `yaml:"app"`
	Logging      LoggingConfig      `yaml:"logging"`
	Redis        RedisConfig        `yaml:"redis"`
	RabbitMQ     RabbitMQConfig     `yaml:"rabbitmq"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Notification NotificationConfig `yaml:"notification"`
	Lingxing     LingxingConfig     `yaml:"lingxing"`
	Runner       RunnerConfig       `yaml:"runner"`
}

type AppSection struct {
	ID        string `yaml:"id"`
	Name      string `yaml:"name"`
	Env       string `yaml:"env"`
	TasksFile string `yaml:"tasks_file"`
}

// LoggingConfig 定义日志级别与文件滚动策略。
type LoggingConfig struct {
	Level      string `yaml:"level"`
	FilePath   string `yaml:"file_path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`
	Compress   bool   `yaml:"compress"`
}

// RedisConfig 用于游标状态存储连接配置。
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// RabbitMQConfig 用于采集结果投递配置。
type RabbitMQConfig struct {
	URL          string `yaml:"url"`
	Exchange     string `yaml:"exchange"`
	ExchangeType string `yaml:"exchange_type"`
	RoutingKey   string `yaml:"routing_key"`
	Mandatory    bool   `yaml:"mandatory"`
}

// MetricsConfig 用于 Prometheus 暴露配置。
type MetricsConfig struct {
	Enable bool   `yaml:"enable"`
	Addr   string `yaml:"addr"`
	Path   string `yaml:"path"`
}

type NotificationConfig struct {
	DingTalk DingTalkConfig `yaml:"dingtalk"`
}

type DingTalkConfig struct {
	Enable  bool   `yaml:"enable"`
	Webhook string `yaml:"webhook"`
	Secret  string `yaml:"secret"`
}

// LingxingConfig 为领星平台接入配置。
type LingxingConfig struct {
	BaseURL             string                            `yaml:"base_url"`
	TokenPath           string                            `yaml:"token_path"`
	Timeout             string                            `yaml:"timeout"`
	TokenReclaimTimeout string                            `yaml:"token_reclaim_timeout"`
	TokenBucketCapacity int                               `yaml:"token_bucket_capacity"`
	DefaultHeader       map[string]string                 `yaml:"default_header"`
	Tenants             map[string]LingxingTenantAuthConf `yaml:"tenants"`
}

type LingxingTenantAuthConf struct {
	ID        string `yaml:"id"`
	Name      string `yaml:"name"`
	AppID     string `yaml:"app_id"`
	AppSecret string `yaml:"app_secret"`
}

// EffectiveAppID 优先使用 app_id，兼容历史字段 app_key。
func (c LingxingTenantAuthConf) EffectiveAppID() string {
	return c.AppID
}

type RunnerConfig struct {
	WorkerCount         int    `yaml:"worker_count"`
	QueueSize           int    `yaml:"queue_size"`
	ShutdownGracePeriod string `yaml:"shutdown_grace_period"`
}

// TimeoutDuration 解析领星客户端超时时间，异常时给默认值。
func (c AppConfig) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(c.Lingxing.Timeout)
	if err != nil || d <= 0 {
		return 15 * time.Second
	}
	return d
}

// ShutdownGraceDuration 解析优雅停机缓冲时间，异常时给默认值。
func (c AppConfig) ShutdownGraceDuration() time.Duration {
	d, err := time.ParseDuration(c.Runner.ShutdownGracePeriod)
	if err != nil || d <= 0 {
		return 15 * time.Second
	}
	return d
}

// LingxingTokenReclaimDuration 解析本地令牌桶超时回收周期。
func (c AppConfig) LingxingTokenReclaimDuration() time.Duration {
	d, err := time.ParseDuration(c.Lingxing.TokenReclaimTimeout)
	if err != nil || d <= 0 {
		return 2 * time.Minute
	}
	return d
}

// LoadAppConfig 从 yaml 读取应用配置并补齐默认值。
func LoadAppConfig(path string) (AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return AppConfig{}, fmt.Errorf("read app config %s: %w", path, err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return AppConfig{}, fmt.Errorf("unmarshal app config: %w", err)
	}

	cfg.setDefaults()
	return cfg, nil
}

// setDefaults 统一维护全局配置默认值，减少运行期 nil/空值判断。
func (c *AppConfig) setDefaults() {
	if c.App.Name == "" {
		c.App.Name = "suxie-collector"
	}
	if c.App.ID == "" {
		c.App.ID = c.App.Name
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.MaxSizeMB <= 0 {
		c.Logging.MaxSizeMB = 100
	}
	if c.Logging.MaxBackups <= 0 {
		c.Logging.MaxBackups = 10
	}
	if c.Logging.MaxAgeDays <= 0 {
		c.Logging.MaxAgeDays = 7
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}
	if c.Metrics.Addr == "" {
		c.Metrics.Addr = ":2112"
	}
	if c.RabbitMQ.Exchange == "" {
		c.RabbitMQ.Exchange = "collector.exchange"
	}
	if c.RabbitMQ.ExchangeType == "" {
		c.RabbitMQ.ExchangeType = "topic"
	}
	if c.RabbitMQ.RoutingKey == "" {
		c.RabbitMQ.RoutingKey = "collector.lingxing.raw"
	}
	if c.Runner.WorkerCount <= 0 {
		c.Runner.WorkerCount = 8
	}
	if c.Runner.QueueSize <= 0 {
		c.Runner.QueueSize = 128
	}
	if c.Lingxing.BaseURL == "" {
		c.Lingxing.BaseURL = "https://openapi.lingxing.com"
	}
	if c.Lingxing.TokenPath == "" {
		c.Lingxing.TokenPath = "/api/auth-server/oauth/access-token"
	}
	if c.Lingxing.TokenBucketCapacity <= 0 {
		c.Lingxing.TokenBucketCapacity = 10
	}
	if c.Lingxing.TokenReclaimTimeout == "" {
		c.Lingxing.TokenReclaimTimeout = "2m"
	}
}
