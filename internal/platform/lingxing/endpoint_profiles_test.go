package lingxing

import "testing"

// TestApplyMSKUProfitParams_Daily 验证按天查询默认值和参数合法性。
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

// TestApplyMSKUProfitParams_Monthly 验证按月查询同月约束。
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

// TestApplyMSKUProfitParams_InvalidRange 验证按天查询跨度上限校验。
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
