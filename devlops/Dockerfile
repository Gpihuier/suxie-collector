# syntax=docker/dockerfile:1

# -----------------------------
# 1) Builder: 编译 Go 二进制
# -----------------------------
FROM golang:1.24-alpine AS builder

WORKDIR /src

# 安装 git 以支持 go mod 拉取部分依赖（如果需要）
RUN apk add --no-cache git

# 先拷贝 go mod/sum，利用 Docker 层缓存依赖下载
COPY go.mod go.sum ./
RUN go mod download

# 再拷贝项目源码
COPY . .

# 编译静态二进制（linux/amd64）
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/collector ./cmd/collector

# ---------------------------------------
# 2) Runtime: 仅保留运行所需最小环境
# ---------------------------------------
FROM alpine:3.20

WORKDIR /app

# 时区与证书（HTTPS 调用领星 API 需要 CA 证书）
RUN apk add --no-cache ca-certificates tzdata

# 拷贝二进制与配置目录（你也可以用 volume 覆盖）
COPY --from=builder /out/collector /app/collector
COPY configs /app/configs

# 采集日志目录（与 app.example 里的 /tmp 日志路径不冲突）
RUN mkdir -p /app/logs

# 默认暴露 metrics 端口
EXPOSE 2112

# 默认按示例配置启动
ENTRYPOINT ["/app/collector"]
CMD ["-config", "/app/configs/app.example.yaml", "-tasks", "/app/configs/tasks"]

