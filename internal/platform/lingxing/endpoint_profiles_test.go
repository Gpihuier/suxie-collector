package lingxing

import "testing"

func TestApplyMSKUProfitParams_Daily(t *testing.T) {
	params := map[string]any{
		"startDate": "2026-04-01",
		"endDate":   "2026-04-30",
	}
	if err := applyMSKUProfitParams(params); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if params["offset"].(int) != 0 {
		t.Fatalf("offset default not applied")
	}
	if params["length"].(int) != 1000 {
		t.Fatalf("length default not applied")
	}
}

func TestApplyMSKUProfitParams_Monthly(t *testing.T) {
	params := map[string]any{
		"monthlyQuery": true,
		"startDate":    "2026-04",
		"endDate":      "2026-04",
	}
	if err := applyMSKUProfitParams(params); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyMSKUProfitParams_InvalidRange(t *testing.T) {
	params := map[string]any{
		"monthlyQuery": false,
		"startDate":    "2026-04-01",
		"endDate":      "2026-05-10",
	}
	if err := applyMSKUProfitParams(params); err == nil {
		t.Fatalf("expect range validation error")
	}
}
