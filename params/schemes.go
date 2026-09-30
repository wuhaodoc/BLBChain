package params

import "fmt"

// Scheme names are the public configuration interface. Numeric method IDs remain
// unchanged for compatibility with existing data and low-level protocol code.
func applyScheme(c *globalConfig) error {
	methods := map[string]int{"CLPA-Broker": 0, "CLPA": 1, "Broker": 2, "Presto": 3, "ContribChain": 4, "LB-Chain": 5, "BLBChain": 4, "BLBChain-Capacity": 4}
	id, ok := methods[c.Scheme]
	if !ok {
		return fmt.Errorf("unknown Scheme %q", c.Scheme)
	}
	c.ConsensusMethod = id
	if c.Scheme == "BLBChain" || c.Scheme == "BLBChain-Capacity" {
		c.Overhead.Enabled = true
		c.Overhead.Lightweight = true
	}
	if c.Scheme == "BLBChain-Capacity" {
		c.Overhead.Migration = false
	}
	return nil
}
