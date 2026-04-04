package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrCursorNotFound = errors.New("cursor not found")

// CursorState 描述采集任务的断点状态：
// - NextPage: 下次从哪一页继续
// - LastWindowEnd: 上次采集窗口结束时间（增量采集关键）
// - LastSuccessAt: 最近一次成功采集时间
type CursorState struct {
	NextPage       int       `json:"next_page"`
	LastWindowEnd  string    `json:"last_window_end"`
	LastSuccessAt  time.Time `json:"last_success_at"`
	LastRecordHash string    `json:"last_record_hash"`
}

type CursorStore interface {
	Get(ctx context.Context, key string) (CursorState, error)
	Set(ctx context.Context, key string, state CursorState) error
	Delete(ctx context.Context, key string) error
}

// RedisCursorStore 是 CursorStore 的 Redis 实现。
type RedisCursorStore struct {
	client *redis.Client
}

func NewRedisCursorStore(client *redis.Client) *RedisCursorStore {
	return &RedisCursorStore{client: client}
}

// Get 读取游标，不存在时返回 ErrCursorNotFound。
func (s *RedisCursorStore) Get(ctx context.Context, key string) (CursorState, error) {
	val, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return CursorState{}, ErrCursorNotFound
	}
	if err != nil {
		return CursorState{}, fmt.Errorf("redis get cursor key=%s: %w", key, err)
	}

	var state CursorState
	if err := json.Unmarshal([]byte(val), &state); err != nil {
		return CursorState{}, fmt.Errorf("unmarshal cursor key=%s: %w", key, err)
	}
	return state, nil
}

// Set 持久化游标状态。
func (s *RedisCursorStore) Set(ctx context.Context, key string, state CursorState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal cursor key=%s: %w", key, err)
	}
	if err := s.client.Set(ctx, key, body, 0).Err(); err != nil {
		return fmt.Errorf("redis set cursor key=%s: %w", key, err)
	}
	return nil
}

// Delete 删除游标。
func (s *RedisCursorStore) Delete(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis del cursor key=%s: %w", key, err)
	}
	return nil
}
