// Shared input and placement transport for baseline account allocation.
package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"log"
	"math/big"
	"os"
	"sync"
	"time"
)

type BaselineAllocationCommittee struct {
	scheme          string
	epochMu         sync.Mutex
	readyShards     map[uint64]bool
	csvPath         string
	dataTotalNum    int
	nowDataNum      int
	batchDataNum    int
	reconfigTimeGap int
	IpNodeTable     map[uint64]map[uint64]string
	partitionMap    map[string]uint64
	Ss              *signal.StopSignal
	sl              *supervisor_log.SupervisorLog
}

func newBaselineAllocationCommittee(
	Ip_nodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum int,
	batchNum int,
	reconfigTimeGap int,
	scheme string,
) *BaselineAllocationCommittee {
	return &BaselineAllocationCommittee{
		scheme:          scheme,
		readyShards:     make(map[uint64]bool),
		csvPath:         csvFilePath,
		dataTotalNum:    dataNum,
		nowDataNum:      0,
		batchDataNum:    batchNum,
		reconfigTimeGap: reconfigTimeGap,
		IpNodeTable:     Ip_nodeTable,
		partitionMap:    make(map[string]uint64),
		Ss:              Ss,
		sl:              sl,
	}
}

// 实现 CommitteeModule 接口
func (p *BaselineAllocationCommittee) HandleBlockInfo(b *message.BlockInfoMsg) {
	if b.Epoch >= 1 {
		p.epochMu.Lock()
		p.readyShards[b.SenderShardID] = true
		p.epochMu.Unlock()
	}
}

func (p *BaselineAllocationCommittee) HandleOtherMessage([]byte) {}

func (p *BaselineAllocationCommittee) MsgSendingControl() {
	p.RunAllocation()
}

// Transaction 表示一笔交易
type Transaction struct {
	Sender    string
	Recipient string
	Value     float64
	payload   *core.Transaction
}

// RunAllocation uses the selected baseline policy and shared placement transport.
func (p *BaselineAllocationCommittee) RunAllocation() {
	log.Println("=== Phase 0: Preprocessing - Scanning ALL transactions ===")
	startTime := time.Now()

	// 读取所有交易
	allTxs, err := p.loadAllTransactions()
	if err != nil {
		log.Fatalf("Failed to load transactions: %v", err)
	}

	totalTxs := len(allTxs)
	log.Println("=== Preprocessing Complete ===")
	log.Printf("  Processed: %d transactions\n", totalTxs)

	// 构建图（只保留有交易的边）
	graph := make(map[string]map[string]float64)
	addresses := make(map[string]bool)

	for _, tx := range allTxs {
		addresses[tx.Sender] = true
		addresses[tx.Recipient] = true

		if graph[tx.Sender] == nil {
			graph[tx.Sender] = make(map[string]float64)
		}
		if graph[tx.Recipient] == nil {
			graph[tx.Recipient] = make(map[string]float64)
		}

		// 双向边，权重为交易价值
		graph[tx.Sender][tx.Recipient] += tx.Value
		graph[tx.Recipient][tx.Sender] += tx.Value
	}

	// 统计实际的边数
	actualEdges := 0
	for _, neighbors := range graph {
		actualEdges += len(neighbors)
	}

	log.Printf("  Graph: %d vertices, %d edges\n", len(addresses), actualEdges)
	log.Printf("  Time: %v\n", time.Since(startTime))

	// Select the baseline account allocator.
	log.Printf("=== Phase 1: %s account allocation ===", p.scheme)
	partition := map[string]uint64{}
	if p.scheme == "ContribChain" {
		partition = ContribChainAccountAllocation(graph, allTxs, params.ShardNum)
	} else {
		weights := map[string]float64{}
		owner := map[string]uint64{}
		for _, tx := range allTxs {
			weights[tx.Sender]++
			owner[tx.Sender] = uint64(utils.Addr2Shard(tx.Sender))
			owner[tx.Recipient] = uint64(utils.Addr2Shard(tx.Recipient))
		}
		for a, sid := range owner {
			partition[a] = sid
		}
		for a, sid := range LBChainAccountAllocation(weights, owner, params.ShardNum, 10, 1.3) {
			partition[a] = sid
		}
	}

	// 保存分区映射
	for addr, shard := range partition {
		p.partitionMap[addr] = shard
	}

	preprocessTime := time.Since(startTime)
	log.Printf("=== Initial Partitioning Complete (Time: %v) ===\n", preprocessTime)
	log.Printf("  Partition map size: %d addresses\n", len(p.partitionMap))
	p.sendPartitionMap()

	// 发送交易
	log.Println("=== Phase 2: Sending transactions with optimized partition ===")
	p.sendTransactionsWithPartition(allTxs)
}

func (p *BaselineAllocationCommittee) sendPartitionMap() {
	pm := message.PartitionModifiedMap{PartitionModified: p.partitionMap}
	b, err := json.Marshal(pm)
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CPartitionMsg, b)
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		ip, ok := p.IpNodeTable[sid][0]
		if !ok {
			panic("missing partition destination")
		}
		if err := networks.TcpDial(msg, ip); err != nil {
			log.Panic(err)
		}
	}
	deadline := time.Now().Add(120 * time.Second)
	for {
		p.epochMu.Lock()
		ready := len(p.readyShards) == params.ShardNum
		p.epochMu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			panic("partition synchronization timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.Ss.StopGap_Reset()
	log.Println("Partition synchronization confirmed by all shard leaders")
}

// 加载所有交易
func (p *BaselineAllocationCommittee) loadAllTransactions() ([]Transaction, error) {
	file, err := os.Open(p.csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	allTxs := make([]Transaction, 0)
	totalToRead := p.dataTotalNum

	for i, record := range records {
		if len(allTxs) >= totalToRead {
			break
		}
		if len(record) < 9 {
			continue
		}
		payload, ok := data2tx(record, uint64(i))
		if !ok {
			continue
		}
		value, _ := new(big.Float).SetInt(payload.Value).Float64()
		tx := Transaction{
			Sender:    payload.Sender,
			Recipient: payload.Recipient,
			Value:     value,
			payload:   payload,
		}
		allTxs = append(allTxs, tx)

		if i%50000 == 0 {
			log.Printf("  Preprocessing: %d txs (%.1f%%)\n", i, float64(i)/float64(totalToRead)*100)
		}
	}

	return allTxs, nil
}

// 发送交易
func (p *BaselineAllocationCommittee) sendTransactionsWithPartition(allTxs []Transaction) {
	startTime := time.Now()
	totalSent := 0

	// 按分片组织交易
	shardTxs := make(map[uint64][]*core.Transaction)

	for _, tx := range allTxs {
		// 获取发送方的分片
		shard, exists := p.partitionMap[tx.Sender]
		if !exists {
			// 如果地址不在分区映射中，使用默认分片策略
			shard = uint64(utils.Addr2Shard(tx.Sender))
		}

		if tx.payload == nil {
			panic("LB transaction has no exact source payload")
		}
		coreTx := core.NewTransaction(tx.Sender, tx.Recipient, new(big.Int).Set(tx.payload.Value), tx.payload.Nonce, time.Now())

		shardTxs[shard] = append(shardTxs[shard], coreTx)
		totalSent++

		// 按批次发送
		if len(shardTxs[shard]) >= p.batchDataNum {
			p.sendBatchToShard(shard, shardTxs[shard])
			shardTxs[shard] = nil
		}

		// 控制注入速度
		if params.InjectSpeed > 0 && totalSent%params.InjectSpeed == 0 {
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				if len(shardTxs[sid]) > 0 {
					p.sendBatchToShard(sid, shardTxs[sid])
					shardTxs[sid] = nil
				}
			}
			time.Sleep(1 * time.Second)
		}

		if totalSent%10000 == 0 {
			elapsed := time.Since(startTime)
			log.Printf("  Sending: %d / %d txs (%.1f%%). Time: %v\n",
				totalSent, len(allTxs), float64(totalSent)/float64(len(allTxs))*100, elapsed)
		}
	}

	// 发送剩余交易
	for shard, txs := range shardTxs {
		if len(txs) > 0 {
			p.sendBatchToShard(shard, txs)
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("=== Transaction Injection Complete ===\n")
	log.Printf("  Total sent: %d txs\n", totalSent)
	log.Printf("  Time: %v\n", elapsed)
	log.Printf("  Average speed: %.2f txs/s\n", float64(totalSent)/elapsed.Seconds())
}

// 发送一批交易到指定分片
func (p *BaselineAllocationCommittee) sendBatchToShard(shardID uint64, txs []*core.Transaction) {
	it := message.InjectTxs{
		Txs:       txs,
		ToShardID: shardID,
	}

	itByte, err := json.Marshal(it)
	if err != nil {
		log.Panic(err)
	}

	send_msg := message.MergeMessage(message.CInject, itByte)

	// Initial PBFT leader is node 0; Go map iteration order is unspecified.
	ip, exists := p.IpNodeTable[shardID][0]
	if !exists {
		panic("missing shard leader address")
	}
	p.Ss.StopGap_Reset()
	if err := networks.TcpDial(send_msg, ip); err != nil {
		log.Panic(err)
	}
}
