// LB-Chain allocation adapter: move active accounts from high-load to low-load
// shards using pending work and a history-based estimate of upcoming work.
package committee

import "sort"

// LBChainAccountAllocation uses transaction load, not capacity-normalized stress
// or community detection. It returns accepted changes only. The study adapter
// uses a simple history-rate predictor; it does not implement a learned predictor.
func LBChainAccountAllocation(weights map[string]float64, owner map[string]uint64, shards, limit int, threshold float64) map[string]uint64 {
	plan := map[string]uint64{}
	if shards < 2 || limit <= 0 {
		return plan
	}
	load := make([]float64, shards)
	total := 0.0
	for a, w := range weights {
		if int(owner[a]) >= shards || w < 0 {
			panic("invalid LB-Chain workload")
		}
		load[owner[a]] += w
		total += w
	}
	if total == 0 {
		return plan
	}
	for iteration := 0; iteration < 10 && len(plan) < limit; iteration++ {
		high, low := 0, 0
		for i := 1; i < shards; i++ {
			if load[i] > load[high] {
				high = i
			}
			if load[i] < load[low] {
				low = i
			}
		}
		if high == low || load[high] <= threshold*total/float64(shards) {
			break
		}
		candidates := []string{}
		for a, w := range weights {
			if _, moved := plan[a]; !moved && w > 0 && int(owner[a]) == high {
				candidates = append(candidates, a)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if weights[candidates[i]] == weights[candidates[j]] {
				return candidates[i] < candidates[j]
			}
			return weights[candidates[i]] > weights[candidates[j]]
		})
		moved := false
		for _, a := range candidates {
			w := weights[a]
			before := load[high]*load[high] + load[low]*load[low]
			after := (load[high]-w)*(load[high]-w) + (load[low]+w)*(load[low]+w)
			if after >= before {
				continue
			}
			plan[a] = uint64(low)
			load[high] -= w
			load[low] += w
			moved = true
			break
		}
		if !moved {
			break
		}
	}
	return plan
}
