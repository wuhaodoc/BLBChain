package committee

import "testing"

func TestLBChainAllocationReducesLoad(t *testing.T) {
	weights := map[string]float64{"hot": 6, "other": 4, "cold": 1}
	owner := map[string]uint64{"hot": 0, "other": 0, "cold": 1}
	moves := LBChainAccountAllocation(weights, owner, 2, 1, 1.3)
	if len(moves) != 1 || moves["hot"] != 1 {
		t.Fatalf("unexpected allocation: %v", moves)
	}
	if owner["hot"] != 0 {
		t.Fatal("mutated input ownership")
	}
}
func TestLBChainBalancedAllocationIsEmpty(t *testing.T) {
	moves := LBChainAccountAllocation(map[string]float64{"a": 5, "b": 5}, map[string]uint64{"a": 0, "b": 1}, 2, 5, 1.3)
	if len(moves) != 0 {
		t.Fatal("balanced shards must not move accounts")
	}
}
