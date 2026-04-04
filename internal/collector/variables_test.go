package collector

import (
	"context"
	"testing"
	"time"

	"suxie.com/suxie-collector/internal/storage"
)

// TestRenderParamSets 验证列表变量展开为多组参数。
func TestRenderParamSets(t *testing.T) {
	vars := ResolveOutput{
		Scalars: map[string]string{"start": "2026-01-01", "end": "2026-01-02"},
		Lists: map[string][]string{
			"shop_id": {"s1", "s2"},
		},
	}
	template := map[string]any{
		"shop_id":    "${shop_id}",
		"start_time": "${start}",
		"end_time":   "${end}",
	}

	paramSets := RenderParamSets(template, vars)
	if len(paramSets) != 2 {
		t.Fatalf("expect 2 param sets, got %d", len(paramSets))
	}
}

// TestDateWindowProvider 验证 date_window 可基于游标续跑。
func TestDateWindowProvider(t *testing.T) {
	provider := DateWindowProvider{
		KeyStart:  "window_start",
		KeyEnd:    "window_end",
		Window:    24 * time.Hour,
		StartFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	out, err := provider.Resolve(context.Background(), ResolveInput{
		TenantID: "t1",
		JobName:  "j1",
		Timezone: "UTC",
		Now:      time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		Cursor: storage.CursorState{
			LastWindowEnd: "2026-01-02T00:00:00Z",
		},
	})
	if err != nil {
		t.Fatalf("resolve date window failed: %v", err)
	}
	if out.Scalars["window_start"] != "2026-01-02T00:00:00Z" {
		t.Fatalf("unexpected start: %s", out.Scalars["window_start"])
	}
	if out.Scalars["window_end"] != "2026-01-03T00:00:00Z" {
		t.Fatalf("unexpected end: %s", out.Scalars["window_end"])
	}
}
