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

type RedisCursorStore struct {
	client *redis.Client
}

func NewRedisCursorStore(client *redis.Client) *RedisCursorStore {
	return &RedisCursorStore{client: client}
}

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

func (s *RedisCursorStore) Delete(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis del cursor key=%s: %w", key, err)
	}
	return nil
}
