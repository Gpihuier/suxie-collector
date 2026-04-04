package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// TasksConfig 定义可快速扩展的采集任务清单。
type TasksConfig struct {
	Version string         `yaml:"version"`
	Tenants []TenantConfig `yaml:"tenants"`
}

type TenantConfig struct {
	ID        string           `yaml:"id"`
	TenantID  string           `yaml:"tenant_id"`
	Platform  string           `yaml:"platform"`
	Enabled   bool             `yaml:"enabled"`
	Timezone  string           `yaml:"timezone"`
	RateLimit RateLimitConfig  `yaml:"rate_limit"`
	Variables []VariableConfig `yaml:"variables"`
	Jobs      []JobConfig      `yaml:"jobs"`
}

// RateLimitConfig 表示令牌桶参数（RPS + Burst）。
type RateLimitConfig struct {
	RPS   float64 `yaml:"rps"`
	Burst int     `yaml:"burst"`
}

type JobConfig struct {
	Name               string            `yaml:"name"`
	Enabled            bool              `yaml:"enabled"`
	Method             string            `yaml:"method"`
	Endpoint           string            `yaml:"endpoint"`
	RequestIn          string            `yaml:"request_in"`
	Schedule           ScheduleConfig    `yaml:"schedule"`
	Pagination         PaginationConfig  `yaml:"pagination"`
	Params             map[string]any    `yaml:"params"`
	Headers            map[string]string `yaml:"headers"`
	DataPathCandidates []string          `yaml:"data_path_candidates"`
	RateLimit          *RateLimitConfig  `yaml:"rate_limit"`
	Variables          []VariableConfig  `yaml:"variables"`
	Parallelism        int               `yaml:"parallelism"`
}

type ScheduleConfig struct {
	Mode  string `yaml:"mode"`
	Every string `yaml:"every"`
}

type PaginationConfig struct {
	Enabled             bool     `yaml:"enabled"`
	StartPage           int      `yaml:"start_page"`
	PageParam           string   `yaml:"page_param"`
	PageSizeParam       string   `yaml:"page_size_param"`
	PageSize            int      `yaml:"page_size"`
	TotalPathCandidates []string `yaml:"total_path_candidates"`
}

type VariableConfig struct {
	Type      string   `yaml:"type"`
	Key       string   `yaml:"key"`
	Value     string   `yaml:"value"`
	Values    []string `yaml:"values"`
	KeyStart  string   `yaml:"key_start"`
	KeyEnd    string   `yaml:"key_end"`
	Format    string   `yaml:"format"`
	Window    string   `yaml:"window"`
	StartFrom string   `yaml:"start_from"`
}

// LoadTasksConfig 支持两种输入：
// 1) 单文件：传统 tasks.yaml
// 2) 目录：一租户一文件，自动合并
func LoadTasksConfig(path string) (TasksConfig, error) {
	info, err := os.Stat(path)
	if err != nil {
		return TasksConfig{}, fmt.Errorf("stat tasks config %s: %w", path, err)
	}
	if info.IsDir() {
		return loadTasksConfigFromDir(path)
	}

	return loadTasksConfigFromFile(path)
}

// loadTasksConfigFromFile 读取单个任务文件。
func loadTasksConfigFromFile(path string) (TasksConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return TasksConfig{}, fmt.Errorf("read tasks config %s: %w", path, err)
	}

	var cfg TasksConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return TasksConfig{}, fmt.Errorf("unmarshal tasks config: %w", err)
	}

	cfg.setDefaults()
	return cfg, nil
}

// loadTasksConfigFromDir 扫描任务目录并合并所有 yaml/yml。
// 文件名会作为 tenant 的默认 id（未显式提供时）。
func loadTasksConfigFromDir(dir string) (TasksConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return TasksConfig{}, fmt.Errorf("read tasks dir %s: %w", dir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	merged := TasksConfig{Version: "1"}
	seen := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		filePath := filepath.Join(dir, name)
		cfg, err := loadTasksConfigFromFile(filePath)
		if err != nil {
			return TasksConfig{}, err
		}

		// 文件名即租户默认 id，满足“一租户一文件”约定。
		fileID := strings.TrimSuffix(name, filepath.Ext(name))
		for i := range cfg.Tenants {
			t := &cfg.Tenants[i]
			if t.ID == "" {
				t.ID = fileID
			}
			if t.TenantID == "" {
				t.TenantID = t.ID
			}
			if existing, ok := seen[t.TenantID]; ok {
				return TasksConfig{}, fmt.Errorf("duplicate tenant_id=%s in %s and %s", t.TenantID, existing, filePath)
			}
			seen[t.TenantID] = filePath
		}
		merged.Tenants = append(merged.Tenants, cfg.Tenants...)
	}

	merged.setDefaults()
	if len(merged.Tenants) == 0 {
		return TasksConfig{}, fmt.Errorf("no task yaml found in dir %s", dir)
	}
	return merged, nil
}

// setDefaults 统一补齐任务默认值，避免运行期出现空字段判断分散在各模块。
func (c *TasksConfig) setDefaults() {
	if c.Version == "" {
		c.Version = "1"
	}
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if t.ID == "" {
			t.ID = t.TenantID
		}
		if t.TenantID == "" {
			t.TenantID = t.ID
		}
		if t.Platform == "" {
			t.Platform = "lingxing"
		}
		if t.Timezone == "" {
			t.Timezone = "Asia/Shanghai"
		}
		if t.RateLimit.RPS <= 0 {
			t.RateLimit.RPS = 2
		}
		if t.RateLimit.Burst <= 0 {
			t.RateLimit.Burst = 4
		}
		for j := range t.Jobs {
			job := &t.Jobs[j]
			if job.Method == "" {
				job.Method = "GET"
			}
			if job.RequestIn == "" {
				job.RequestIn = "query"
			}
			if job.Schedule.Mode == "" {
				job.Schedule.Mode = "interval"
			}
			if job.Schedule.Every == "" {
				job.Schedule.Every = "5m"
			}
			if job.Pagination.StartPage <= 0 {
				job.Pagination.StartPage = 1
			}
			if job.Pagination.PageParam == "" {
				job.Pagination.PageParam = "page"
			}
			if job.Pagination.PageSizeParam == "" {
				job.Pagination.PageSizeParam = "limit"
			}
			if job.Pagination.PageSize <= 0 {
				job.Pagination.PageSize = 50
			}
			if len(job.Pagination.TotalPathCandidates) == 0 {
				job.Pagination.TotalPathCandidates = []string{"data.total", "Data.Total", "total", "Total"}
			}
			if len(job.DataPathCandidates) == 0 {
				job.DataPathCandidates = []string{"data.list", "data.data", "Data.List", "Data.Data", "list", "data"}
			}
			if job.Parallelism <= 0 {
				job.Parallelism = 1
			}
		}
	}
}
