package lingxing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"suxie.com/suxie-collector/internal/collector"
	"suxie.com/suxie-collector/internal/config"
)

type tokenState struct {
	Value     string
	ExpiresAt time.Time
}

type Client struct {
	cfg              config.LingxingConfig
	httpClient       *http.Client
	logger           *slog.Logger
	tokenBucketSize  int
	tokenReclaimTime time.Duration

	mu      sync.RWMutex
	tokens  map[string]tokenState
	buckets map[string]*tokenBucket
}

func NewClient(cfg config.LingxingConfig, timeout time.Duration, logger *slog.Logger) *Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	reclaim := 2 * time.Minute
	if d, err := time.ParseDuration(cfg.TokenReclaimTimeout); err == nil && d > 0 {
		reclaim = d
	}
	if cfg.TokenBucketCapacity <= 0 {
		cfg.TokenBucketCapacity = 10
	}

	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger:           logger,
		tokenBucketSize:  cfg.TokenBucketCapacity,
		tokenReclaimTime: reclaim,
		tokens:           map[string]tokenState{},
		buckets:          map[string]*tokenBucket{},
	}
}

func (c *Client) Platform() string {
	return "lingxing"
}

func (c *Client) Fetch(ctx context.Context, req collector.FetchRequest) (map[string]any, error) {
	return c.fetchWithRetry(ctx, req, true)
}

func (c *Client) fetchWithRetry(ctx context.Context, req collector.FetchRequest, retryOnTokenErr bool) (map[string]any, error) {
	tenantAuth, err := c.tenantAuth(req.TenantID)
	if err != nil {
		return nil, err
	}
	appID := tenantAuth.EffectiveAppID()

	target, err := c.joinURL(req.Endpoint)
	if err != nil {
		return nil, err
	}

	endpointParams := cloneMapAny(req.Params)
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if profile, ok := lookupEndpointProfile(target); ok {
		if method == "" {
			method = profile.Method
		}
		if profile.ApplyAndValidate != nil {
			if err := profile.ApplyAndValidate(endpointParams); err != nil {
				return nil, fmt.Errorf("validate endpoint params %s: %w", req.Endpoint, err)
			}
		}
	}
	if method == "" {
		method = http.MethodGet
	}

	release, ok := c.acquireRequestToken(appID, target, endpointBucketCapacity(target))
	if !ok {
		return nil, &APIError{Code: ErrCodeRateLimited, Message: "requests too frequently. please request later.", Endpoint: target}
	}
	defer release()

	token, err := c.getToken(ctx, req.TenantID)
	if err != nil {
		return nil, err
	}

	apiParams := cloneMapAny(endpointParams)
	authParams := map[string]any{
		"access_token": token,
		"timestamp":    time.Now().Unix(),
		"app_key":      appID,
	}
	for k, v := range authParams {
		apiParams[k] = v
	}

	sign, err := GenerateSign(appID, apiParams)
	if err != nil {
		return nil, fmt.Errorf("generate sign: %w", err)
	}

	queryParams := map[string]any{
		"access_token": authParams["access_token"],
		"timestamp":    authParams["timestamp"],
		"app_key":      authParams["app_key"],
		"sign":         sign,
	}

	var body io.Reader
	if strings.EqualFold(req.RequestIn, "body") {
		payload, err := json.Marshal(endpointParams)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		body = bytes.NewReader(payload)
	} else {
		for k, v := range endpointParams {
			queryParams[k] = v
		}
	}
	target = appendQuery(target, queryParams)

	httpReq, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range c.cfg.DefaultHeader {
		httpReq.Header.Set(k, v)
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	respBody, statusCode, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("lingxing http status=%d body=%s", statusCode, string(respBody))
	}

	payload, apiErr := parseEnvelope(respBody, target)
	if apiErr != nil {
		if retryOnTokenErr && (apiErr.Code == ErrCodeTokenMissingOrExpired || apiErr.Code == ErrCodeAccessTokenNotMatch) {
			c.clearToken(req.TenantID)
			return c.fetchWithRetry(ctx, req, false)
		}
		return nil, apiErr
	}
	return payload, nil
}

func (c *Client) getToken(ctx context.Context, tenantID string) (string, error) {
	tenantAuth, err := c.tenantAuth(tenantID)
	if err != nil {
		return "", err
	}
	if tenantAuth.StaticToken != "" {
		return tenantAuth.StaticToken, nil
	}
	appID := tenantAuth.EffectiveAppID()
	if appID == "" || tenantAuth.AppSecret == "" {
		return "", fmt.Errorf("tenant=%s missing app_id/app_secret", tenantID)
	}

	c.mu.RLock()
	cached, ok := c.tokens[tenantID]
	c.mu.RUnlock()
	if ok && cached.Value != "" && time.Until(cached.ExpiresAt) > 30*time.Second {
		return cached.Value, nil
	}

	target, err := c.joinURL(c.cfg.TokenPath)
	if err != nil {
		return "", err
	}

	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	if err := writer.WriteField("appId", appID); err != nil {
		return "", fmt.Errorf("write appId field: %w", err)
	}
	if err := writer.WriteField("appSecret", tenantAuth.AppSecret); err != nil {
		return "", fmt.Errorf("write appSecret field: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, buf)
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for k, v := range c.cfg.DefaultHeader {
		req.Header.Set(k, v)
	}

	respBody, statusCode, err := c.doRequest(req)
	if err != nil {
		return "", err
	}
	if statusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("token http status=%d body=%s", statusCode, string(respBody))
	}

	payload, apiErr := parseEnvelope(respBody, target)
	if apiErr != nil {
		return "", apiErr
	}

	token, ok := pickString(payload,
		"access_token",
		"token",
		"data.access_token",
		"data.token",
		"Data.AccessToken",
		"Data.Token",
	)
	if !ok || token == "" {
		return "", fmt.Errorf("token not found in response")
	}

	expiresInSec, ok := pickInt(payload,
		"expires_in",
		"data.expires_in",
		"Data.ExpiresIn",
	)
	if !ok || expiresInSec <= 0 {
		expiresInSec = 3600
	}

	state := tokenState{Value: token, ExpiresAt: time.Now().Add(time.Duration(expiresInSec) * time.Second)}
	c.mu.Lock()
	c.tokens[tenantID] = state
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Debug("refresh lingxing token", "tenant", tenantID, "expires_in", expiresInSec)
	}
	return state.Value, nil
}

func (c *Client) tenantAuth(tenantID string) (config.LingxingTenantAuthConf, error) {
	tenantAuth, ok := c.cfg.Tenants[tenantID]
	if !ok {
		return config.LingxingTenantAuthConf{}, fmt.Errorf("lingxing tenant auth not found: %s", tenantID)
	}
	if tenantAuth.EffectiveAppID() == "" && tenantAuth.StaticToken == "" {
		return config.LingxingTenantAuthConf{}, fmt.Errorf("lingxing tenant auth missing app_id/app_key: %s", tenantID)
	}
	return tenantAuth, nil
}

func (c *Client) clearToken(tenantID string) {
	c.mu.Lock()
	delete(c.tokens, tenantID)
	c.mu.Unlock()
}

func (c *Client) acquireRequestToken(appID, target string, capacity int) (func(), bool) {
	key := appID + ":" + requestBucketRouteKey(target)

	c.mu.Lock()
	bucket, ok := c.buckets[key]
	if !ok {
		if capacity <= 0 {
			capacity = c.tokenBucketSize
		}
		bucket = newTokenBucket(capacity, c.tokenReclaimTime)
		c.buckets[key] = bucket
	}
	c.mu.Unlock()

	return bucket.acquire()
}

func requestBucketRouteKey(target string) string {
	u, err := url.Parse(target)
	if err != nil {
		return target
	}
	return u.Path
}

func endpointBucketCapacity(target string) int {
	if profile, ok := lookupEndpointProfile(target); ok && profile.TokenBucketCapacity > 0 {
		return profile.TokenBucketCapacity
	}
	return 0
}

func parseEnvelope(body []byte, endpoint string) (map[string]any, *APIError) {
	if !json.Valid(body) {
		return nil, &APIError{Code: -1, Message: "response body is not valid json", Endpoint: endpoint}
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &APIError{Code: -1, Message: err.Error(), Endpoint: endpoint}
	}

	code, hasCode := pickInt(payload, "code", "Code")
	message, _ := pickString(payload, "message", "Message")
	requestID, _ := pickString(payload, "request_id", "requestId", "RequestId")

	if hasCode && code != 0 && code != 200 {
		if message == "" {
			message = "unknown error"
		}
		return nil, &APIError{Code: code, Message: message, RequestID: requestID, Endpoint: endpoint}
	}
	return payload, nil
}

func (c *Client) doRequest(req *http.Request) ([]byte, int, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request lingxing api: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response body: %w", err)
	}
	return respBody, resp.StatusCode, nil
}

func (c *Client) joinURL(endpoint string) (string, error) {
	base, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return "", fmt.Errorf("parse base url: %w", err)
	}
	ep, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse endpoint: %w", err)
	}
	if ep.IsAbs() {
		return ep.String(), nil
	}
	base.Path = path.Join(base.Path, ep.Path)
	return base.String(), nil
}

func appendQuery(target string, params map[string]any) string {
	u, err := url.Parse(target)
	if err != nil {
		return target
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, fmt.Sprintf("%v", v))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func pickString(payload map[string]any, paths ...string) (string, bool) {
	for _, p := range paths {
		v, ok := collector.LookupPath(payload, p)
		if !ok {
			continue
		}
		s, ok := v.(string)
		if ok && strings.TrimSpace(s) != "" {
			return s, true
		}
	}
	return "", false
}

func pickInt(payload map[string]any, paths ...string) (int, bool) {
	for _, p := range paths {
		v, ok := collector.LookupPath(payload, p)
		if !ok {
			continue
		}
		switch n := v.(type) {
		case int:
			return n, true
		case int64:
			return int(n), true
		case float64:
			return int(n), true
		case json.Number:
			if i, err := n.Int64(); err == nil {
				return int(i), true
			}
		case string:
			if n == "" {
				continue
			}
			if parsed, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func cloneMapAny(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
