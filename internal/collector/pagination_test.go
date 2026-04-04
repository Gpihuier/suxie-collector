package collector

import "testing"

func TestExtractTotalAndRecords(t *testing.T) {
	payload := map[string]any{
		"Data": map[string]any{
			"Total": 120,
			"List":  []any{map[string]any{"id": 1}, map[string]any{"id": 2}},
		},
	}

	total, ok := ExtractTotal(payload, []string{"data.total", "Data.Total", "total"})
	if !ok || total != 120 {
		t.Fatalf("unexpected total: ok=%v total=%d", ok, total)
	}

	records, ok := ExtractRecords(payload, []string{"data.list", "Data.List"})
	if !ok || len(records) != 2 {
		t.Fatalf("unexpected records: ok=%v len=%d", ok, len(records))
	}
}

func TestNeedNextPage(t *testing.T) {
	if !NeedNextPage(1, 50, 50, 0, false) {
		t.Fatalf("expected next page when total unknown and recordCount == pageSize")
	}
	if NeedNextPage(2, 50, 20, 0, false) {
		t.Fatalf("did not expect next page when less than page size")
	}
	if !NeedNextPage(1, 50, 50, 120, true) {
		t.Fatalf("expected next page when total known not reached")
	}
	if NeedNextPage(3, 50, 20, 120, true) {
		t.Fatalf("did not expect next page when total reached")
	}
}
