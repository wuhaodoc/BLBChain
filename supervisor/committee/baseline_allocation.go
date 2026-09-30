// Per-baseline allocation adapters for the common migration execution harness.
package committee

import (
	"blockEmulator/core"
	"blockEmulator/experiment"
	"blockEmulator/message"
	"blockEmulator/params"
	"math/big"
	"sort"
	"time"
)

type migrationBaseline struct {
	scheme string
	txs    []Transaction
	seen   map[string]bool
}

func newMigrationBaseline() *migrationBaseline {
	name := params.MigrationBaseline()
	if name == "" {
		return nil
	}
	return &migrationBaseline{scheme: name, seen: map[string]bool{}}
}

// observe records historical transactions once, without looking ahead in the trace.
func (b *migrationBaseline) observe(m *message.BlockInfoMsg) {
	for _, group := range [][]*core.Transaction{m.InnerShardTxs, m.Relay1Txs, m.Relay2Txs} {
		for _, t := range group {
			k := string(t.TxHash)
			if b.seen[k] {
				continue
			}
			b.seen[k] = true
			v, _ := new(big.Float).SetInt(t.Value).Float64()
			b.txs = append(b.txs, Transaction{Sender: t.Sender, Recipient: t.Recipient, Value: v, payload: t})
		}
	}
}
func (b *migrationBaseline) plan(c *MigrationExperimentCommittee, limit int) map[string]uint64 {
	begin := time.Now()
	candidate := map[string]uint64{}
	switch b.scheme {
	case "ContribChain":
		graph := map[string]map[string]float64{}
		for _, t := range b.txs {
			if graph[t.Sender] == nil {
				graph[t.Sender] = map[string]float64{}
			}
			if graph[t.Recipient] == nil {
				graph[t.Recipient] = map[string]float64{}
			}
			graph[t.Sender][t.Recipient] += t.Value
			graph[t.Recipient][t.Sender] += t.Value
		}
		candidate = ContribChainAccountAllocation(graph, b.txs, params.ShardNum)
	case "LB-Chain":
		weights := map[string]float64{}
		owner := map[string]uint64{}
		elapsed := time.Since(c.stress.start).Seconds()
		for a, n := range c.frequency {
			weights[a] = float64(c.stress.pending[a]) + float64(n)/elapsed*float64(params.Block_Interval)/1000
			owner[a] = c.shard(a)
		}
		candidate = LBChainAccountAllocation(weights, owner, params.ShardNum, limit, 1.3)
	default:
		panic("unknown migration baseline")
	}
	// The account cap is a control for this study. Never invent extra migrations
	// when the selected baseline returns fewer useful candidates.
	keys := []string{}
	for a, to := range candidate {
		if c.shard(a) != to {
			keys = append(keys, a)
		}
	}
	sort.Strings(keys)
	plan := map[string]uint64{}
	for _, a := range keys {
		if len(plan) >= limit {
			break
		}
		plan[a] = candidate[a]
	}
	experiment.Record("baseline_decisions", []string{"unix_ns", "scheme", "requested_accounts", "candidate_accounts", "actual_accounts", "selection_ms"}, time.Now().UnixNano(), b.scheme, limit, len(keys), len(plan), float64(time.Since(begin).Nanoseconds())/1e6)
	return plan
}
