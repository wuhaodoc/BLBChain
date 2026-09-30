package committee

import "testing"

func TestStressMovesReduceVariance(t *testing.T) {
	w := map[string]float64{"a": 5, "b": 3, "c": 2, "d": 1}
	o := map[string]uint64{"a": 0, "b": 0, "c": 0, "d": 1}
	p, b, a := selectStressMoves(w, o, []float64{10, 20}, 2)
	if len(p) == 0 || len(p) > 2 || a >= b {
		t.Fatalf("invalid plan %v %g %g", p, b, a)
	}
	for _, s := range p {
		if s != 1 {
			t.Fatal("wrong migration direction")
		}
	}
}
func TestBalancedStressDoesNotMigrate(t *testing.T) {
	p, _, _ := selectStressMoves(map[string]float64{"a": 10, "b": 20}, map[string]uint64{"a": 0, "b": 1}, []float64{10, 20}, 10)
	if len(p) != 0 {
		t.Fatal("balanced workload moved")
	}
}
