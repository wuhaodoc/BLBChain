// ExClique consensus implementation
package exclique

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/shard"
	"bytes"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// ExCliqueNode represents an ExClique consensus node
type ExCliqueNode struct {
	// Basic node info
	RunningNode *shard.Node
	ShardID     uint64
	NodeID      uint64

	// Blockchain
	CurChain *chain.BlockChain
	db       ethdb.Database

	// ExClique specific
	nodeNums    uint64
	inTurnIndex uint64 // Current in-turn node index

	// Timing parameters
	blockInterval time.Duration
	lastBlockTime time.Time
	delayManager  *DelayManager

	// Transaction pool
	txPool     []*core.Transaction
	txPoolLock sync.Mutex

	// PCB (Proactive Compact Block) support
	cbf         *CountingBloomFilter
	knownTxs    map[string]bool
	knownTxLock sync.RWMutex

	// Order management
	orderManager *OrderManager

	// Network
	ip_nodeTable map[uint64]map[uint64]string
	tcpln        net.Listener

	// Control
	stopSignal atomic.Bool
	pStop      chan uint64

	// Metrics
	broadcastTime atomic.Int64
	verifyTime    atomic.Int64
	blockCount    atomic.Int64
	forkCount     atomic.Int64

	// Random number generator
	rng *rand.Rand
}

// NewExCliqueNode creates a new ExClique consensus node
func NewExCliqueNode(shardID, nodeID uint64, pcc *params.ChainConfig) *ExCliqueNode {
	node := &ExCliqueNode{
		ShardID:       shardID,
		NodeID:        nodeID,
		nodeNums:      pcc.Nodes_perShard,
		inTurnIndex:   0,
		blockInterval: time.Duration(pcc.BlockInterval) * time.Millisecond,
		txPool:        make([]*core.Transaction, 0),
		knownTxs:      make(map[string]bool),
		ip_nodeTable:  params.IPmap_nodeTable,
		pStop:         make(chan uint64),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano() + int64(nodeID))),
	}

	// Initialize delay manager
	initialBeta := 50 * time.Millisecond
	waitWindow := time.Duration(pcc.BlockInterval) * time.Millisecond
	node.delayManager = NewDelayManager(initialBeta, waitWindow)

	// Initialize database
	dbPath := params.DatabaseWrite_path + "mptDB/ldb/s" +
		strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	node.db, err = rawdb.NewLevelDBDatabase(dbPath, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}

	// Initialize blockchain
	node.CurChain, err = chain.NewBlockChain(pcc, node.db)
	if err != nil {
		log.Panic("cannot create blockchain")
	}

	// Initialize node info
	node.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  node.ip_nodeTable[shardID][nodeID],
	}

	// Initialize CBF
	node.cbf = NewCountingBloomFilter(100000, 4)

	// Initialize order manager
	validators := make([]uint64, pcc.Nodes_perShard)
	for i := uint64(0); i < pcc.Nodes_perShard; i++ {
		validators[i] = i
	}
	node.orderManager = NewOrderManager(validators)

	node.stopSignal.Store(false)
	node.lastBlockTime = time.Now()

	return node
}

// Propose starts the consensus process (called from build module)
func (ec *ExCliqueNode) Propose() {
	log.Printf("[ExClique] Node S%d-N%d starting consensus...\n", ec.ShardID, ec.NodeID)
	ec.consensusLoop()
}

// consensusLoop is the main consensus loop
func (ec *ExCliqueNode) consensusLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if ec.stopSignal.Load() {
				return
			}
			ec.tryProposeBlock()
		case <-ec.pStop:
			return
		}
	}
}

// tryProposeBlock attempts to propose a new block
func (ec *ExCliqueNode) tryProposeBlock() {
	currentHeight := ec.CurChain.CurrentBlock.Header.Number + 1

	// Get in-turn validator using order manager
	inTurnValidator := ec.orderManager.GetInTurnValidator(currentHeight)
	isInTurn := ec.NodeID == inTurnValidator

	timeSinceLastBlock := time.Since(ec.lastBlockTime)

	if isInTurn {
		// In-turn node: propose immediately after block interval
		if timeSinceLastBlock >= ec.blockInterval {
			ec.proposeBlock(true)
		}
	} else {
		// No-turn node: use delay manager to decide
		if ec.delayManager.ShouldPropose(timeSinceLastBlock) {
			ec.proposeBlock(false)
		}
	}
}

// proposeBlock creates and broadcasts a new block
func (ec *ExCliqueNode) proposeBlock(isInTurn bool) {
	ec.txPoolLock.Lock()

	if len(ec.txPool) == 0 {
		ec.txPoolLock.Unlock()
		return
	}

	// Create block
	blockTxs := ec.txPool
	maxSize := int(params.MaxBlockSize_global)
	if len(blockTxs) > maxSize {
		blockTxs = blockTxs[:maxSize]
	}

	block := ec.createBlock(blockTxs)

	// Remove used transactions
	ec.txPool = ec.txPool[len(blockTxs):]
	ec.txPoolLock.Unlock()

	// Broadcast using PCB
	startTime := time.Now()
	ec.broadcastBlockWithPCB(block)
	broadcastDuration := time.Since(startTime)
	ec.broadcastTime.Store(broadcastDuration.Milliseconds())
	ec.delayManager.RecordBroadcastTime(broadcastDuration)

	// Add to local chain
	ec.addBlock(block)

	// Update timing
	ec.lastBlockTime = time.Now()

	// Record in order manager
	ec.orderManager.RecordBlock(ec.NodeID, isInTurn)

	// Update metrics
	ec.blockCount.Add(1)

	turnType := "no-turn"
	if isInTurn {
		turnType = "in-turn"
	}

	log.Printf("[ExClique] S%d-N%d proposed block #%d (%s) with %d txs, beta=%dms\n",
		ec.ShardID, ec.NodeID, block.Header.Number, turnType, len(blockTxs),
		ec.delayManager.GetBeta().Milliseconds())
}

// createBlock creates a new block
func (ec *ExCliqueNode) createBlock(txs []*core.Transaction) *core.Block {
	prevBlock := ec.CurChain.CurrentBlock

	header := &core.BlockHeader{
		ParentBlockHash: prevBlock.Hash,
		Number:          prevBlock.Header.Number + 1,
		Time:            time.Now(),
		Miner:           int32(ec.NodeID),
	}

	block := core.NewBlock(header, txs)
	block.Hash = header.Hash()

	return block
}

// addBlock adds a block to the local chain
func (ec *ExCliqueNode) addBlock(block *core.Block) {
	startTime := time.Now()

	// Verify block
	if !ec.verifyBlock(block) {
		log.Printf("[ExClique] Block verification failed\n")
		ec.forkCount.Add(1)
		return
	}

	verifyDuration := time.Since(startTime)
	ec.verifyTime.Store(verifyDuration.Milliseconds())
	ec.delayManager.RecordVerifyTime(verifyDuration)

	// Add to chain
	ec.CurChain.AddBlock(block)

	// Update known transactions
	ec.knownTxLock.Lock()
	for _, tx := range block.Body {
		ec.knownTxs[string(tx.TxHash)] = true
		ec.cbf.Add(tx.TxHash)
	}
	ec.knownTxLock.Unlock()
}

// verifyBlock verifies a block
func (ec *ExCliqueNode) verifyBlock(block *core.Block) bool {
	// Basic checks
	if block.Header.Number != ec.CurChain.CurrentBlock.Header.Number+1 {
		return false
	}

	// Verify parent hash
	if !bytes.Equal(block.Header.ParentBlockHash, ec.CurChain.CurrentBlock.Hash) {
		return false
	}

	return true
}

// Stop stops the consensus node
func (ec *ExCliqueNode) Stop() {
	ec.stopSignal.Store(true)
	close(ec.pStop)

	// Print final statistics
	ec.printStatistics()
}

// printStatistics prints final statistics
func (ec *ExCliqueNode) printStatistics() {
	log.Printf("[ExClique] S%d-N%d Final Statistics:\n", ec.ShardID, ec.NodeID)
	log.Printf("  Blocks proposed: %d\n", ec.blockCount.Load())
	log.Printf("  Forks detected: %d\n", ec.forkCount.Load())
	log.Printf("  Avg broadcast time: %dms\n", ec.broadcastTime.Load())
	log.Printf("  Avg verify time: %dms\n", ec.verifyTime.Load())

	delayStats := ec.delayManager.GetStats()
	log.Printf("  Beta: %dms\n", delayStats["beta"])
	log.Printf("  Wait window: %dms\n", delayStats["waitWindow"])

	fairnessScores := ec.orderManager.GetAllScores()
	log.Printf("  Fairness scores: %v\n", fairnessScores)
}
