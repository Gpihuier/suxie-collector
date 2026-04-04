package dingtalk

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	enabled    bool
	webhookURL string
	secret     string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewClient 创建钉钉通知客户端；enable=false 时发送会短路为 no-op。
func NewClient(enabled bool, webhookURL, secret string, logger *slog.Logger) *Client {
	return &Client{
		enabled:    enabled,
		webhookURL: webhookURL,
		secret:     secret,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		logger:     logger,
	}
}

// SendMarkdown 发送 markdown 消息。
// 当配置了 secret 时自动生成 timestamp + sign。
func (c *Client) SendMarkdown(ctx context.Context, title, text string) error {
	if !c.enabled || c.webhookURL == "" {
		return nil
	}

	target := c.webhookURL
	if c.secret != "" {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		signature := signDingTalk(timestamp, c.secret)
		u, err := url.Parse(target)
		if err != nil {
			return fmt.Errorf("parse dingtalk url: %w", err)
		}
		q := u.Query()
		q.Set("timestamp", timestamp)
		q.Set("sign", signature)
		u.RawQuery = q.Encode()
		target = u.String()
	}

	payload := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": title,
			"text":  text,
		},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create dingtalk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send dingtalk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("dingtalk status=%d", resp.StatusCode)
	}

	if c.logger != nil {
		c.logger.Info("dingtalk notification sent", "title", title)
	}
	return nil
}

// signDingTalk 实现钉钉签名：HMAC-SHA256 + Base64 + URL Encode。
func signDingTalk(timestamp, secret string) string {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write([]byte(stringToSign))
	return url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
}
