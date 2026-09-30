package params

import "testing"

func TestNamedSchemes(t *testing.T) {
	for name, id := range map[string]int{"BLBChain": 4, "BLBChain-Capacity": 4, "ContribChain": 4, "LB-Chain": 5, "Presto": 3, "CLPA": 1, "Broker": 2} {
		c := globalConfig{Scheme: name}
		c.Overhead.Migration = true
		if err := applyScheme(&c); err != nil {
			t.Fatal(err)
		}
		if c.ConsensusMethod != id {
			t.Fatalf("%s: wrong method", name)
		}
		if name == "BLBChain-Capacity" && c.Overhead.Migration {
			t.Fatal("capacity ablation must not migrate")
		}
	}
	c := globalConfig{Scheme: "invalid"}
	if applyScheme(&c) == nil {
		t.Fatal("unknown scheme accepted")
	}
}
