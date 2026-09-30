// BLBChain experiment network path: streaming, compact proposals, recovery and audit hooks.
package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/experiment"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// Cached bodies are immutable encodings. Relay changes must not mutate cached txs.
type overheadState struct {
	mu     sync.Mutex
	bodies map[string][]byte
	stream chan [][]byte
}

type compactProposal struct {
	Proposal message.PrePrepare
	Header   []byte
	Hash     []byte
	Keys     []string
	Leader   string
}
type missingBodies struct {
	Keys  []string
	Reply string
}

func bodyKey(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// All experiment processes run on one host. These timestamps include queueing
// and scheduling, not just wire transmission. Cross-host use needs clock sync.
func (p *PbftConsensusNode) timing(kind, key string, at time.Time, count int) {
	if !params.Overhead.Enabled {
		return
	}
	experiment.Record("timing", []string{"unix_ns", "event", "shard", "node", "key", "count"}, at.UnixNano(), kind, p.ShardID, p.NodeID, key, count)
}

func (p *PbftConsensusNode) bodyTiming(kind string, bodies [][]byte, at time.Time) {
	keys := make([]string, len(bodies))
	for i, b := range bodies {
		keys[i] = bodyKey(b)
	}
	encoded, _ := json.Marshal(keys)
	p.timing(kind, string(encoded), at, len(keys))
}

func (p *PbftConsensusNode) initOverhead() {
	if !params.Overhead.Enabled {
		return
	}
	p.overhead = &overheadState{bodies: map[string][]byte{}, stream: make(chan [][]byte, 128)}
	go func() {
		for bodies := range p.overhead.stream {
			b, _ := json.Marshal(bodies)
			for n := uint64(0); n < p.node_nums; n++ {
				if n == p.NodeID || int(n) == params.Overhead.StreamSkipNode {
					continue
				}
				if err := networks.TcpDial(message.MergeMessage("OHStream", b), p.ip_nodeTable[p.ShardID][n]); err != nil {
					panic(err)
				}
			}
		}
	}()
}

func (p *PbftConsensusNode) cacheBodies(bodies [][]byte) {
	p.overhead.mu.Lock()
	defer p.overhead.mu.Unlock()
	for _, b := range bodies {
		p.overhead.bodies[bodyKey(b)] = b
	}
}

func (p *PbftConsensusNode) streamTransactions(txs []*core.Transaction) {
	if !params.Overhead.Enabled || !params.Overhead.Lightweight || len(txs) == 0 {
		return
	}
	started := time.Now()
	bodies := make([][]byte, len(txs))
	for i, tx := range txs {
		bodies[i] = tx.Encode()
	}
	p.cacheBodies(bodies)
	p.bodyTiming("stream_start", bodies, started)
	// Bounded queue provides backpressure; the stream worker competes with PBFT
	// and migration through the same process-wide upload limiter.
	p.overhead.stream <- bodies
}

func (p *PbftConsensusNode) encodeProposal(pp message.PrePrepare) []byte {
	if params.Overhead.Enabled && pp.RequestMsg.RequestType == message.BlockRequest {
		started := time.Now()
		p.timing("proposal_start", hex.EncodeToString(pp.Digest), started, len(core.DecodeB(pp.RequestMsg.Msg.Content).Body))
	}
	if params.Overhead.Enabled && pp.RequestMsg.RequestType == message.PartitionReq {
		networks.MarkMigrationDigest(pp.Digest)
	}
	if params.Overhead.Enabled && params.Overhead.Lightweight && pp.RequestMsg.RequestType == message.BlockRequest {
		block := core.DecodeB(pp.RequestMsg.Msg.Content)
		cp := compactProposal{Proposal: pp, Header: block.Header.Encode(), Hash: block.Hash, Keys: make([]string, len(block.Body)), Leader: p.RunningNode.IPaddr}
		r := *pp.RequestMsg
		r.Msg.Content = nil
		cp.Proposal.RequestMsg = &r
		bodies := make([][]byte, len(block.Body))
		for i, tx := range block.Body {
			bodies[i] = tx.Encode()
			cp.Keys[i] = bodyKey(bodies[i])
		}
		p.cacheBodies(bodies)
		b, err := json.Marshal(cp)
		if err != nil {
			panic(err)
		}
		return message.MergeMessage("OHCompact", b)
	}
	b, err := json.Marshal(pp)
	if err != nil {
		panic(err)
	}
	return message.MergeMessage(message.CPrePrepare, b)
}

func (p *PbftConsensusNode) handleOverhead(t message.MessageType, b []byte) bool {
	if !params.Overhead.Enabled {
		return false
	}
	switch t {
	case "OHStream", "OHBodies":
		var bodies [][]byte
		if err := json.Unmarshal(b, &bodies); err != nil {
			panic(err)
		}
		p.cacheBodies(bodies)
		if t == "OHStream" {
			p.bodyTiming("stream_received", bodies, time.Now())
		}
	case "OHMissing":
		go func() {
			var req missingBodies
			if err := json.Unmarshal(b, &req); err != nil {
				panic(err)
			}
			bodies := make([][]byte, 0, len(req.Keys))
			p.overhead.mu.Lock()
			for _, k := range req.Keys {
				v, ok := p.overhead.bodies[k]
				if !ok {
					p.overhead.mu.Unlock()
					panic("proposal body missing at leader")
				}
				bodies = append(bodies, v)
			}
			p.overhead.mu.Unlock()
			payload, _ := json.Marshal(bodies)
			if err := networks.TcpDial(message.MergeMessage("OHBodies", payload), req.Reply); err != nil {
				panic(err)
			}
		}()
	case "OHCompact":
		go p.handleCompactProposal(b)
	default:
		return false
	}
	return true
}

func (p *PbftConsensusNode) handleCompactProposal(b []byte) {
	received := time.Now()
	var cp compactProposal
	if err := json.Unmarshal(b, &cp); err != nil {
		panic(err)
	}
	p.timing("proposal_received", hex.EncodeToString(cp.Proposal.Digest), received, len(cp.Keys))
	missing := []string{}
	p.overhead.mu.Lock()
	for _, k := range cp.Keys {
		if _, ok := p.overhead.bodies[k]; !ok {
			missing = append(missing, k)
		}
	}
	p.overhead.mu.Unlock()
	if len(missing) > 0 {
		r, _ := json.Marshal(missingBodies{missing, p.RunningNode.IPaddr})
		if err := networks.TcpDial(message.MergeMessage("OHMissing", r), cp.Leader); err != nil {
			panic(err)
		}
	}
	deadline := time.Now().Add(time.Duration(params.Overhead.TimeoutSeconds) * time.Second)
	body := make([]*core.Transaction, len(cp.Keys))
	for i, k := range cp.Keys {
		for {
			p.overhead.mu.Lock()
			encoded, ok := p.overhead.bodies[k]
			p.overhead.mu.Unlock()
			if ok {
				body[i] = core.DecodeTx(encoded)
				break
			}
			if time.Now().After(deadline) {
				panic("compact proposal recovery timed out")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	block := core.NewBlock(core.DecodeBH(cp.Header), body)
	block.Hash = cp.Hash
	cp.Proposal.RequestMsg.Msg.Content = block.Encode()
	if string(getDigest(cp.Proposal.RequestMsg)) != string(cp.Proposal.Digest) {
		panic("compact reconstruction digest mismatch")
	}
	encoded, _ := json.Marshal(cp.Proposal)
	p.handlePrePrepare(encoded)
}

func (p *PbftConsensusNode) migrationDone(epoch uint64, count int) {
	if !params.Overhead.Enabled {
		return
	}
	experiment.Event("migration_applied", p.ShardID, p.NodeID, epoch, count)
	b, _ := json.Marshal(struct{ Shard, Node, Epoch uint64 }{p.ShardID, p.NodeID, epoch})
	if err := networks.TcpDial(message.MergeMessage("OHMigrationDone", b), params.SupervisorAddr); err != nil {
		panic(fmt.Errorf("migration ack: %w", err))
	}
}

// Export owned account states after workload completion. The runner compares
// every replica and independently replays balance deltas from the input CSV.
func (p *PbftConsensusNode) auditOverheadState() {
	if !params.Overhead.Enabled {
		return
	}
	f, err := os.Open(params.DatasetFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	if _, err = r.Read(); err != nil {
		panic(err)
	}
	set := map[string]bool{}
	for i := 0; i < params.TotalDataSize; i++ {
		row, e := r.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(e)
		}
		for _, a := range row[2:4] {
			if p.CurChain.Get_PartitionMap(a) == p.ShardID {
				set[a] = true
			}
		}
	}
	addresses := make([]string, 0, len(set))
	for a := range set {
		addresses = append(addresses, a)
	}
	sort.Strings(addresses)
	states := p.CurChain.FetchAccounts(addresses)
	for i, a := range addresses {
		experiment.Record("account_audit", []string{"account", "balance", "initial_balance", "shard", "node"}, a, states[i].Balance.String(), params.Init_Balance.String(), p.ShardID, p.NodeID)
	}
}
