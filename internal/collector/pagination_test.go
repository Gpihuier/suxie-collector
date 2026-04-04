package collector

import "testing"

// TestExtractTotalAndRecords 验证：
// 1) total 可从不同大小写路径提取。
// 2) records 可从不同层级路径提取。
func TestExtractTotalAndRecords(t *testing.T) {
	// 构造模拟响应：total 和 list 放在 Data 下。
	payload := map[string]any{
		"Data": map[string]any{
			"Total": 120,
			"List":  []any{map[string]any{"id": 1}, map[string]any{"id": 2}},
		},
	}

	// 候选路径同时给出小写和大写形式，验证大小写兼容。
	total, ok := ExtractTotal(payload, []string{"data.total", "Data.Total", "total"})
	if !ok || total != 120 {
		t.Fatalf("unexpected total: ok=%v total=%d", ok, total)
	}

	// records 同样验证路径候选机制。
	records, ok := ExtractRecords(payload, []string{"data.list", "Data.List"})
	if !ok || len(records) != 2 {
		t.Fatalf("unexpected records: ok=%v len=%d", ok, len(records))
	}
}

// TestNeedNextPage 验证翻页判定规则：
// 1) total 未知 -> 满页继续。
// 2) total 已知 -> 依据 page*size 与 total 比较。
func TestNeedNextPage(t *testing.T) {
	// total 未知，且本页满页，应该继续。
	if !NeedNextPage(1, 50, 50, 0, false) {
		t.Fatalf("expected next page when total unknown and recordCount == pageSize")
	}
	// total 未知，但本页不满页，应该停止。
	if NeedNextPage(2, 50, 20, 0, false) {
		t.Fatalf("did not expect next page when less than page size")
	}
	// total 已知且尚未达到 total，应该继续。
	if !NeedNextPage(1, 50, 50, 120, true) {
		t.Fatalf("expected next page when total known not reached")
	}
	// total 已知且已达到 total，应该停止。
	if NeedNextPage(3, 50, 20, 120, true) {
		t.Fatalf("did not expect next page when total reached")
	}
}
