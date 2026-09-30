// Shared scheduled workload and migration state-transfer harness with preserved arrival timestamps.
package committee

import (
	"blockEmulator/core"
	"blockEmulator/experiment"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MigrationExperimentCommittee runs scheduled arrivals and one controlled migration.
// The configured policy selects candidates; a shared CLPA barrier transfers state.
// Scheduled arrival timestamps are preserved across the barrier to include waiting cost.
type MigrationExperimentCommittee struct {
	ips       map[uint64]map[uint64]string
	mu        sync.Mutex
	completed map[string]bool
	acks      map[string]bool
	placement map[string]uint64
	frequency map[string]int
	stress    *migrationStress
	baseline  *migrationBaseline
}

func NewMigrationExperimentCommittee(ips map[uint64]map[uint64]string) *MigrationExperimentCommittee {
	return &MigrationExperimentCommittee{ips: ips, completed: map[string]bool{}, acks: map[string]bool{}, placement: map[string]uint64{}, frequency: map[string]int{}}
}

type overheadRow struct {
	sender, recipient string
	value             *big.Int
}

func loadOverheadCSV(path string, limit int) ([]overheadRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	if _, err = r.Read(); err != nil {
		return nil, err
	}
	rows := make([]overheadRow, 0, limit)
	for len(rows) < limit {
		v, e := r.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if len(v) < 5 {
			return nil, fmt.Errorf("row %d: need at least five columns", len(rows)+2)
		}
		for _, addr := range v[2:4] {
			if len(addr) != 40 {
				return nil, fmt.Errorf("row %d: addresses must be 40 hex characters without 0x", len(rows)+2)
			}
			if _, e := hex.DecodeString(addr); e != nil {
				return nil, e
			}
		}
		value, ok := new(big.Int).SetString(v[4], 10)
		if !ok || value.Sign() < 0 {
			return nil, fmt.Errorf("invalid integer value at row %d", len(rows)+2)
		}
		rows = append(rows, overheadRow{strings.ToLower(v[2]), strings.ToLower(v[3]), value})
	}
	if len(rows) != limit {
		return nil, fmt.Errorf("dataset has %d rows; expected %d", len(rows), limit)
	}
	return rows, nil
}

func (c *MigrationExperimentCommittee) shard(addr string) uint64 {
	if s, ok := c.placement[addr]; ok {
		return s
	}
	return uint64(utils.Addr2Shard(addr))
}

func (c *MigrationExperimentCommittee) MsgSendingControl() {
	rows, err := loadOverheadCSV(params.DatasetFile, params.TotalDataSize)
	if err != nil {
		panic(err)
	}
	// Optional historical placement experiment. Never train on the evaluation rows.
	if history := os.Getenv("BLB_PARTITION_HISTORY"); history != "" {
		c.initializeHistoricalPartition(history)
	}
	start := time.Now()
	if params.StressMigrationEnabled() {
		c.stress = newMigrationStress(start)
		c.baseline = newMigrationBaseline()
	}
	deadline := start.Add(time.Duration(params.Overhead.TimeoutSeconds) * time.Second)
	experiment.Event("workload_start", 0, 0, 0, len(rows))
	migrated := false
	for pos := 0; pos < len(rows); {
		if time.Now().After(deadline) {
			panic("workload timeout")
		}
		if params.Overhead.Migration && !migrated && time.Since(start) >= time.Duration(params.Overhead.MigrationAfterSeconds)*time.Second {
			if c.stress == nil {
				c.migrate(deadline)
				migrated = true
			}
		}
		if params.Overhead.Migration && !migrated && c.stress != nil {
			if plan := c.stressPlan(); len(plan) > 0 {
				c.applyStressMigration(plan, deadline)
				migrated = true
			}
		}
		end := pos + params.TxBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		// Wait until the last scheduled arrival in this batch. Earlier transactions
		// include batching delay, network queuing and migration wait in latency.
		arrival := start.Add(time.Duration(float64(end-1) / float64(params.InjectSpeed) * float64(time.Second)))
		if wait := time.Until(arrival); wait > 0 {
			time.Sleep(wait)
		}
		byShard := map[uint64][]*core.Transaction{}
		for i := pos; i < end; i++ {
			row := rows[i]
			at := start.Add(time.Duration(float64(i) / float64(params.InjectSpeed) * float64(time.Second)))
			tx := core.NewTransaction(row.sender, row.recipient, row.value, uint64(i), at)
			byShard[c.shard(row.sender)] = append(byShard[c.shard(row.sender)], tx)
			c.frequency[row.sender]++
			if c.stress != nil {
				c.mu.Lock()
				c.stress.pending[row.sender]++
				c.mu.Unlock()
			}
		}
		for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
			if len(byShard[sid]) == 0 {
				continue
			}
			b, _ := json.Marshal(message.InjectTxs{Txs: byShard[sid], ToShardID: sid})
			if err := networks.TcpDial(message.MergeMessage(message.CInject, b), c.ips[sid][0]); err != nil {
				panic(err)
			}
		}
		pos = end
	}
	if params.Overhead.Migration && !migrated {
		if c.stress == nil {
			panic("migration was not triggered")
		}
		experiment.Event("migration_not_triggered", 0, 0, 0, 0)
	}
	experiment.Event("injection_complete", 0, 0, 0, len(rows))
	for {
		c.mu.Lock()
		n := len(c.completed)
		c.mu.Unlock()
		if n == len(rows) {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("incomplete run: confirmed %d of %d", n, len(rows)))
		}
		time.Sleep(100 * time.Millisecond)
	}
	experiment.Event("workload_complete", 0, 0, 0, len(rows))
}

func (c *MigrationExperimentCommittee) migrate(deadline time.Time) {
	if params.ShardNum < 2 {
		panic("migration requires at least two shards")
	}
	accounts := make([]string, 0, len(c.frequency))
	for a := range c.frequency {
		accounts = append(accounts, a)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if c.frequency[accounts[i]] == c.frequency[accounts[j]] {
			return accounts[i] < accounts[j]
		}
		return c.frequency[accounts[i]] > c.frequency[accounts[j]]
	})
	if len(accounts) > params.Overhead.MigrationAccounts {
		accounts = accounts[:params.Overhead.MigrationAccounts]
	}
	if len(accounts) == 0 {
		panic("no accounts available for controlled migration")
	}
	mapping := map[string]uint64{}
	for _, a := range accounts {
		mapping[a] = (c.shard(a) + 1) % uint64(params.ShardNum)
	}
	experiment.Event("migration_requested", 0, 0, 1, len(mapping))
	for _, a := range accounts {
		experiment.Record("migration_plan", []string{"account", "source", "destination", "observed_transactions"}, a, c.shard(a), mapping[a], c.frequency[a])
	}
	b, _ := json.Marshal(message.PartitionModifiedMap{PartitionModified: mapping})
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		if err := networks.TcpDial(message.MergeMessage(message.CPartitionMsg, b), c.ips[sid][0]); err != nil {
			panic(err)
		}
	}
	for {
		c.mu.Lock()
		n := len(c.acks)
		c.mu.Unlock()
		if n == params.ShardNum*params.NodesInShard {
			break
		}
		if time.Now().After(deadline) {
			panic("migration timed out before all replicas applied state")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for a, s := range mapping {
		c.placement[a] = s
	}
	experiment.Event("migration_complete", 0, 0, 1, len(mapping))
}

func (c *MigrationExperimentCommittee) HandleBlockInfo(b *message.BlockInfoMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stress != nil {
		c.observeCapacity(b)
	}
	if c.baseline != nil {
		c.baseline.observe(b)
	}
	final := append(append([]*core.Transaction{}, b.InnerShardTxs...), b.Relay2Txs...)
	for _, tx := range final {
		key := hex.EncodeToString(tx.TxHash)
		if c.completed[key] {
			continue
		}
		c.completed[key] = true
		if c.stress != nil && c.stress.pending[tx.Sender] > 0 {
			c.stress.pending[tx.Sender]--
		}
		experiment.Record("transactions", []string{"hash", "nonce", "arrival_ns", "commit_ns", "latency_ms", "shard", "epoch"}, key, tx.Nonce, tx.Time.UnixNano(), b.CommitTime.UnixNano(), float64(b.CommitTime.Sub(tx.Time).Nanoseconds())/1e6, b.SenderShardID, b.Epoch)
	}
}

func (c *MigrationExperimentCommittee) HandleOtherMessage(msg []byte) {
	t, b := message.SplitMessage(msg)
	if t != "OHMigrationDone" {
		return
	}
	var ack struct{ Shard, Node, Epoch uint64 }
	if err := json.Unmarshal(b, &ack); err != nil {
		panic(err)
	}
	if ack.Epoch != 1 || ack.Shard >= uint64(params.ShardNum) || ack.Node >= uint64(params.NodesInShard) {
		panic("invalid migration acknowledgment")
	}
	c.mu.Lock()
	c.acks[strconv.FormatUint(ack.Shard, 10)+"/"+strconv.FormatUint(ack.Node, 10)] = true
	c.mu.Unlock()
}

func (c *MigrationExperimentCommittee) applyStressMigration(mapping map[string]uint64, deadline time.Time) {
	experiment.Event("migration_requested", 0, 0, 1, len(mapping))
	for a := range mapping {
		experiment.Record("migration_plan", []string{"account", "source", "destination", "observed_transactions"}, a, c.shard(a), mapping[a], c.frequency[a])
	}
	b, _ := json.Marshal(message.PartitionModifiedMap{PartitionModified: mapping})
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		if err := networks.TcpDial(message.MergeMessage(message.CPartitionMsg, b), c.ips[sid][0]); err != nil {
			panic(err)
		}
	}
	for {
		c.mu.Lock()
		n := len(c.acks)
		c.mu.Unlock()
		if n == params.ShardNum*params.NodesInShard {
			break
		}
		if time.Now().After(deadline) {
			panic("migration timed out before all replicas applied state")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for a, s := range mapping {
		c.placement[a] = s
	}
	experiment.Event("migration_complete", 0, 0, 1, len(mapping))
}
