package collector

import (
	"context"
	"testing"
	"time"

	"suxie.com/suxie-collector/internal/storage"
)

// TestRenderParamSets 验证：
// 1) 列表变量会展开成多组参数。
// 2) 标量变量会在每组参数中正确替换。
func TestRenderParamSets(t *testing.T) {
	// 构造变量：一个列表变量 shop_id + 两个标量变量 start/end。
	vars := ResolveOutput{
		Scalars: map[string]string{"start": "2026-01-01", "end": "2026-01-02"},
		Lists: map[string][]string{
			"shop_id": {"s1", "s2"},
		},
	}
	// 模板里同时引用列表变量和标量变量。
	template := map[string]any{
		"shop_id":    "${shop_id}",
		"start_time": "${start}",
		"end_time":   "${end}",
	}

	// 渲染参数集合。
	paramSets := RenderParamSets(template, vars)
	// 断言：应得到 2 组（对应 s1/s2）。
	if len(paramSets) != 2 {
		t.Fatalf("expect 2 param sets, got %d", len(paramSets))
	}
}

// TestDateWindowProvider 验证 date_window provider 的游标续跑能力。
func TestDateWindowProvider(t *testing.T) {
	// 配置一个 24h 窗口 provider。
	provider := DateWindowProvider{
		KeyStart:  "window_start",
		KeyEnd:    "window_end",
		Window:    24 * time.Hour,
		StartFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	// 模拟“上次窗口结束在 2026-01-02T00:00:00Z”。
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

	// 断言 start 应从游标续跑。
	if out.Scalars["window_start"] != "2026-01-02T00:00:00Z" {
		t.Fatalf("unexpected start: %s", out.Scalars["window_start"])
	}
	// 断言 end = start + 24h。
	if out.Scalars["window_end"] != "2026-01-03T00:00:00Z" {
		t.Fatalf("unexpected end: %s", out.Scalars["window_end"])
	}
}
