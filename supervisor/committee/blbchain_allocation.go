// BLBChain allocation: estimate shard capacity and accept variance-reducing account moves.
package committee

import (
	"blockEmulator/experiment"
	"blockEmulator/message"
	"blockEmulator/params"
	"math"
	"sort"
	"time"
)

// Pilot instrumentation: capacity is an empirical round-budget estimate, not
// an externally assigned capacity or a guarantee of a protocol capacity bound.
type migrationStress struct {
	start        time.Time
	pending      map[string]int
	capacity     []float64
	reports      []int
	lastDecision int
}

func newMigrationStress(start time.Time) *migrationStress {
	return &migrationStress{start: start, pending: map[string]int{}, capacity: make([]float64, params.ShardNum), reports: make([]int, params.ShardNum)}
}
func (c *MigrationExperimentCommittee) observeCapacity(b *message.BlockInfoMsg) {
	d := b.CommitTime.Sub(b.ProposeTime).Seconds()
	if d <= 0 || b.BlockBodyLength <= 0 {
		return
	}
	sid := int(b.SenderShardID)
	cap := math.Min(float64(params.MaxBlockSize_global), float64(b.BlockBodyLength)*float64(params.Block_Interval)/1000/d)
	if cap <= 0 {
		return
	}
	c.stress.capacity[sid] = cap
	c.stress.reports[sid]++
	experiment.Record("capacity_samples", []string{"unix_ns", "shard", "block_transactions", "consensus_s", "estimated_capacity"}, time.Now().UnixNano(), sid, b.BlockBodyLength, d, cap)
}
func stressVariance(load, capacity []float64) float64 {
	mean := 0.0
	for i := range load {
		mean += load[i] / capacity[i]
	}
	mean /= float64(len(load))
	v := 0.0
	for i := range load {
		x := load[i]/capacity[i] - mean
		v += x * x
	}
	return v / float64(len(load))
}

// Take only moves from a higher-stress shard to the current lowest-stress shard
// which strictly lower the global variance. The account budget is an upper bound.
func selectStressMoves(weights map[string]float64, owner map[string]uint64, capacity []float64, limit int) (map[string]uint64, float64, float64) {
	load := make([]float64, len(capacity))
	for a, w := range weights {
		load[owner[a]] += w
	}
	initial := stressVariance(load, capacity)
	current := initial
	plan := map[string]uint64{}
	for len(plan) < limit {
		shards := make([]int, len(capacity))
		for i := range shards {
			shards[i] = i
		}
		sort.SliceStable(shards, func(i, j int) bool { return load[shards[i]]/capacity[shards[i]] > load[shards[j]]/capacity[shards[j]] })
		low := shards[len(shards)-1]
		found := false
		for _, high := range shards[:len(shards)-1] {
			accounts := []string{}
			for a, w := range weights {
				if _, moved := plan[a]; !moved && w > 0 && int(owner[a]) == high {
					accounts = append(accounts, a)
				}
			}
			sort.Slice(accounts, func(i, j int) bool {
				if weights[accounts[i]] == weights[accounts[j]] {
					return accounts[i] < accounts[j]
				}
				return weights[accounts[i]] > weights[accounts[j]]
			})
			for _, a := range accounts {
				w := weights[a]
				load[high] -= w
				load[low] += w
				next := stressVariance(load, capacity)
				if next < current-1e-12 {
					plan[a] = uint64(low)
					current = next
					found = true
					break
				}
				load[high] += w
				load[low] -= w
			}
			if found {
				break
			}
		}
		if !found {
			break
		}
	}
	return plan, initial, current
}
func (c *MigrationExperimentCommittee) stressPlan() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stress
	minReports := s.reports[0]
	for _, n := range s.reports {
		if n < minReports {
			minReports = n
		}
	}
	// Require two observed nonempty rounds per shard; never use a wall-clock trigger.
	if minReports < 2 || minReports <= s.lastDecision {
		return nil
	}
	s.lastDecision = minReports
	if c.baseline != nil {
		return c.baseline.plan(c, params.Overhead.MigrationAccounts)
	}
	elapsed := time.Since(s.start).Seconds()
	weights := map[string]float64{}
	owner := map[string]uint64{}
	for a, n := range c.frequency {
		// Arrival-rate estimate from past data only, plus outstanding transactions.
		weights[a] = float64(s.pending[a]) + float64(n)/elapsed*float64(params.Block_Interval)/1000
		owner[a] = c.shard(a)
	}
	plan, before, after := selectStressMoves(weights, owner, s.capacity, params.Overhead.MigrationAccounts)
	experiment.Record("stress_decisions", []string{"unix_ns", "account_limit", "actual_accounts", "variance_before", "variance_after"}, time.Now().UnixNano(), params.Overhead.MigrationAccounts, len(plan), before, after)
	return plan
}
