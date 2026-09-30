package committee

import (
	"blockEmulator/experiment"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"math/big"
	"os"
	"time"
)

// initializeHistoricalPartition reuses the repository's allocation heuristic.
// This is static historical prepartitioning, not adaptive BLB stress migration
// and not a claim of exact reproduction of the published P-Louvain algorithm.
func (c *MigrationExperimentCommittee) initializeHistoricalPartition(path string) {
	start := time.Now()
	rows, err := loadOverheadCSV(path, 20000)
	if err != nil {
		panic(err)
	}
	graph := make(map[string]map[string]float64)
	txs := make([]Transaction, 0, len(rows))
	for _, r := range rows {
		value, _ := new(big.Float).SetInt(r.value).Float64()
		txs = append(txs, Transaction{Sender: r.sender, Recipient: r.recipient, Value: value})
		if graph[r.sender] == nil {
			graph[r.sender] = make(map[string]float64)
		}
		if graph[r.recipient] == nil {
			graph[r.recipient] = make(map[string]float64)
		}
		graph[r.sender][r.recipient] += value
		graph[r.recipient][r.sender] += value
	}
	var mapping map[string]uint64
	method := "repository ContribChainAccountAllocation; static historical placement"
	if supplied := os.Getenv("BLB_PARTITION_MAP"); supplied != "" {
		raw, e := os.ReadFile(supplied)
		if e != nil {
			panic(e)
		}
		if e = json.Unmarshal(raw, &mapping); e != nil {
			panic(e)
		}
		for a, s := range mapping {
			if graph[a] == nil || s >= uint64(params.ShardNum) {
				panic("invalid historical partition entry")
			}
		}
		if len(mapping) != len(graph) {
			panic("historical partition coverage mismatch")
		}
		method = "frequency-weighted communities with relay-aware refinement; static historical placement"
	} else {
		mapping = ContribChainAccountAllocation(graph, txs, params.ShardNum)
	}
	encoded, err := json.MarshalIndent(mapping, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile("initial-partition.json", encoded, 0600); err != nil {
		panic(err)
	}
	for a, s := range mapping {
		experiment.Record("migration_plan", []string{"account", "source", "destination", "observed_transactions"}, a, c.shard(a), s, 0)
	}
	b, err := json.Marshal(message.PartitionModifiedMap{PartitionModified: mapping})
	if err != nil {
		panic(err)
	}
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		if err := networks.TcpDial(message.MergeMessage(message.CPartitionMsg, b), c.ips[sid][0]); err != nil {
			panic(err)
		}
	}
	deadline := time.Now().Add(120 * time.Second)
	for {
		c.mu.Lock()
		ready := len(c.acks) == params.ShardNum*params.NodesInShard
		c.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			panic("initial partition synchronization timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.placement = mapping
	meta, _ := json.Marshal(map[string]interface{}{"history": path, "history_count": len(rows), "setup_duration_s": time.Since(start).Seconds(), "method": method})
	if err := os.WriteFile("partition-setup.json", meta, 0600); err != nil {
		panic(err)
	}
}
