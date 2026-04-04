# suxie-collector

一个面向跨境电商（亚马逊）数据采集场景的 Go 采集框架，当前优先支持领星 API，并为后续扩展马帮等平台预留统一接口。

## 目标能力

- 多租户采集：租户认证隔离、任务隔离。
- 多级限流：租户默认限流 + 任务级覆盖限流。
- 灵活分页：支持 `page/limit` 等可配置分页参数，兼容 `Total` 在 `data/Data/外层` 等不同位置。
- 动态参数：支持 `WithXXX` 任务构建 + `${var}` 模板变量替换。
- 时间窗口增量：可通过 Redis 游标从某个时间点持续追到当前。
- 采集与投递解耦：采集结果统一写入 RabbitMQ，消费端可独立开发。
- 可观测性：预留 Prometheus 指标，Grafana 可直接接入。
- 告警预留：集成钉钉机器人通知入口。

## 技术栈

- 语言: Go 1.24+
- 日志: `log/slog` + `lumberjack`（文件滚动）
- 缓存/游标: Redis (`go-redis/v9`)
- 队列: RabbitMQ (`amqp091-go`)
- 限流: `golang.org/x/time/rate`
- 并发: goroutine + channel + select + `errgroup`
- 监控: Prometheus (`client_golang`)
- 配置: YAML (`gopkg.in/yaml.v3`)

## 目录结构

```text
cmd/collector/main.go                # 程序入口
configs/app.example.yaml             # 全局配置样例
configs/tasks.example.yaml           # 任务配置样例（类似 docker-compose 的任务编排）
internal/app                         # 依赖装配与应用启动
internal/config                      # 配置结构体与加载
internal/logging                     # slog 初始化
internal/metrics                     # Prometheus 指标定义
internal/notifier/dingtalk           # 钉钉通知
internal/platform                    # 平台注册中心
internal/platform/lingxing           # 领星 API 客户端（token + 请求）
internal/collector                   # 采集核心（WithXXX、变量、分页、执行引擎）
internal/queue                       # RabbitMQ 生产者与消息模型
internal/storage                     # Redis 客户端与游标存储
internal/runner                      # 任务编排、调度和并发执行
```

## 快速开始

1. 安装依赖服务：Redis、RabbitMQ。
2. 复制并修改配置：
   - `configs/app.example.yaml`
   - `configs/tasks/*.yaml`（一个租户一个任务文件）
3. 启动服务：

```bash
go run ./cmd/collector -config configs/app.example.yaml -tasks configs/tasks
```

也可以让 `app.tasks_file` 指向目录（例如 `configs/tasks`），程序会自动加载目录下所有 `*.yaml/*.yml` 文件并合并。

4. 查看指标（若开启）：

```text
http://127.0.0.1:2112/metrics
```

5. 信号控制：

- `SIGINT/SIGTERM`: 优雅停机（等待 context 取消和任务退出）。
- `SIGHUP`: 优雅重载（停止当前实例并按配置重新拉起）。

## 任务配置说明（核心）

- `tenants[].rate_limit`: 租户默认限流。
- `tenants[].jobs[].rate_limit`: 覆盖该任务限流。
- `pagination`: 按接口定义分页参数名与页大小。
- `total_path_candidates`: Total 多路径兜底匹配。
- `data_path_candidates`: 列表数据多路径兜底匹配。
- `params`: 请求参数模板，支持 `${变量名}`。
- `variables`:
  - `static`: 固定值
  - `list/shop_list`: 列表值（会展开为多组请求参数）
  - `date_window`: 按时间窗口增量采集（结合 Redis 游标）

## WithXXX 任务构建

`internal/collector/task.go` 中 `BaseCollectTask` 提供链式构建方式，如：

- `WithTenantID`
- `WithJobName`
- `WithEndpoint`
- `WithParams`
- `WithPagination`
- `WithParallelism`

方便未来通过代码动态生成任务，而不仅限 YAML。

## 可观测性与告警

- 指标维度包含 `tenant/job/status`，便于 Grafana 按租户和任务拆分看板。
- 钉钉告警默认关闭，开启后任务失败会推送告警。

## 扩展到其他平台（如马帮）

实现 `collector.Fetcher` 接口并在 `internal/platform` 注册即可，无需改动采集引擎：

- `Platform() string`
- `Fetch(ctx, req)`

## 领星对接规则（已内置）

- 域名默认 `https://openapi.lingxing.com`
- `access_token` 获取路径默认 `/api/auth-server/oauth/access-token`
- token 请求方式采用 `multipart/form-data`，字段 `appId`、`appSecret`
- 请求签名参数包含 `access_token`、`timestamp`、`app_key`、`sign`
- 限流维度采用 `appId + 接口url` 的本地令牌桶，令牌在请求完成/异常后归还，并带 2 分钟超时自动回收
- 全局错误码（如 `2001003`、`2001006`、`3001008`）已在代码中内置中文说明，便于排障

## 新增领星接口指南

1. 在任务配置中新增一个 job（`endpoint`、`method`、`request_in`、`params`）。
2. 若参数有固定默认值/格式约束/时间区间限制，在 `internal/platform/lingxing/endpoint_profiles.go` 增加该接口 profile：
   - `Method`
   - `TokenBucketCapacity`
   - `ApplyAndValidate(params)`
3. 若该接口需特殊签名字段处理，可在 `internal/platform/lingxing/client.go` 的 `fetchWithRetry` 中按 path 做扩展。
4. 若只是普通接口，通常仅改 YAML 即可，不需要改 Go 代码。

项目里已给你放了这个接口的示例任务（默认关闭）：
- `profit-report-msku`（`/bd/profit/report/open/report/msku/list`）

## 建议的后续增强

- 引入重试与退避（指数退避 + 熔断）。
- 增加幂等键与重复数据去重策略。
- 任务配置热更新。
- MQ 发布确认（publisher confirm）与失败补偿。
- 增加 OpenTelemetry Trace。
