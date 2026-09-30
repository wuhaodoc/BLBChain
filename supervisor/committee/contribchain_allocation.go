// ContribChain account allocation: inherited community, boundary and load refinement.
// The community phase is based on the P-Louvain family; it is not an LB-Chain policy.
package committee

import (
	"fmt"
	"log"
	"sort"
)

// 改进的 PLouvain 算法
func ContribChainAccountAllocation(graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	log.Println("=== Starting ContribChain Allocation ===")

	// Phase 0: 基于交易频率的初始分区
	log.Println("Phase 0: Transaction-based initial partitioning...")
	partition := transactionBasedPartitioning(graph, allTxs, numShards)

	// Phase 1: Louvain 社区检测优化
	log.Println("Phase 1: ContribChain community optimization...")
	partition = contribChainCommunityOptimization(partition, graph, numShards)

	// Phase 2: 边界节点优化
	log.Println("Phase 2: Boundary node optimization...")
	partition = optimizeBoundaryNodes(partition, graph, allTxs, numShards)

	// Phase 3: 最终负载均衡
	log.Println("Phase 3: Final load balancing...")
	partition = finalLoadBalancing(partition, graph, allTxs, numShards)

	// 统计结果
	printPartitionStats(partition, allTxs, numShards)

	return partition
}

// 基于交易频率的初始分区
func transactionBasedPartitioning(graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	// 统计每个地址的交易频率
	addrTxCount := make(map[string]int)
	for _, tx := range allTxs {
		addrTxCount[tx.Sender]++
		addrTxCount[tx.Recipient]++
	}

	// 按交易频率排序地址
	type AddrFreq struct {
		Addr  string
		Count int
	}
	addrList := make([]AddrFreq, 0)
	for addr, count := range addrTxCount {
		addrList = append(addrList, AddrFreq{addr, count})
	}
	sort.Slice(addrList, func(i, j int) bool {
		return addrList[i].Count > addrList[j].Count
	})

	// 初始化分片
	partition := make(map[string]uint64)
	shardWorkload := make([]int, numShards)

	// 贪心分配：高频地址优先分配到负载轻的分片
	for _, af := range addrList {
		// 找到负载最轻的分片
		minShard := 0
		minWorkload := shardWorkload[0]
		for sid := 1; sid < numShards; sid++ {
			if shardWorkload[sid] < minWorkload {
				minShard = sid
				minWorkload = shardWorkload[sid]
			}
		}

		partition[af.Addr] = uint64(minShard)
		shardWorkload[minShard] += af.Count
	}

	log.Println("  Initial partitioning complete:")
	for sid := 0; sid < numShards; sid++ {
		nodeCount := 0
		for _, shard := range partition {
			if shard == uint64(sid) {
				nodeCount++
			}
		}
		log.Printf("    Shard %d: %d nodes, workload=%d\n", sid, nodeCount, shardWorkload[sid])
	}

	return partition
}

// Louvain 优化
func contribChainCommunityOptimization(partition map[string]uint64, graph map[string]map[string]float64, numShards int) map[string]uint64 {
	maxIterations := 5

	for iter := 0; iter < maxIterations; iter++ {
		moved := 0

		// 遍历所有节点
		for node := range graph {
			currentShard := partition[node]

			// 计算该节点与每个分片的连接权重
			shardWeights := make([]float64, numShards)
			for neighbor, weight := range graph[node] {
				neighborShard := partition[neighbor]
				shardWeights[neighborShard] += weight
			}

			// 找到连接权重最大的分片
			maxShard := uint64(0)
			maxWeight := shardWeights[0]
			for sid := 1; sid < numShards; sid++ {
				if shardWeights[sid] > maxWeight {
					maxShard = uint64(sid)
					maxWeight = shardWeights[sid]
				}
			}

			// 如果移动能显著增加片内连接，则移动
			currentShardWeight := shardWeights[currentShard]
			if maxWeight > currentShardWeight*1.2 {
				partition[node] = maxShard
				moved++
			}
		}

		log.Printf("  ContribChain community iteration %d: %d nodes moved\n", iter+1, moved)
		if moved == 0 {
			break
		}
	}

	return partition
}

// 边界节点优化
func optimizeBoundaryNodes(partition map[string]uint64, graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	maxIterations := 3

	for iter := 0; iter < maxIterations; iter++ {
		moved := 0

		// 遍历所有节点
		for node := range graph {
			currentShard := partition[node]

			// 计算该节点与每个分片的连接权重
			shardWeights := make([]float64, numShards)
			for neighbor, weight := range graph[node] {
				neighborShard := partition[neighbor]
				shardWeights[neighborShard] += weight
			}

			// 找到连接权重最大的分片
			maxShard := uint64(0)
			maxWeight := shardWeights[0]
			for sid := 1; sid < numShards; sid++ {
				if shardWeights[sid] > maxWeight {
					maxShard = uint64(sid)
					maxWeight = shardWeights[sid]
				}
			}

			// 如果移动能显著减少跨片边，则移动
			currentShardWeight := shardWeights[currentShard]
			if maxWeight > currentShardWeight*1.5 {
				partition[node] = maxShard
				moved++
			}
		}

		log.Printf("  Boundary optimization iteration %d: %d nodes moved\n", iter+1, moved)
		if moved == 0 {
			break
		}
	}

	return partition
}

// 最终负载均衡
func finalLoadBalancing(partition map[string]uint64, graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	// 计算每个分片的负载（片内交易数）
	shardWorkload := make([]int, numShards)
	shardNodes := make([][]string, numShards)

	for node, shard := range partition {
		shardNodes[shard] = append(shardNodes[shard], node)
	}

	// 计算负载（片内交易数）
	for _, tx := range allTxs {
		senderShard, senderExists := partition[tx.Sender]
		recipientShard, recipientExists := partition[tx.Recipient]

		if senderExists && recipientExists && senderShard == recipientShard {
			shardWorkload[senderShard]++
		}
	}

	avgWorkload := float64(len(allTxs)) / float64(numShards)

	// 迭代均衡
	maxIterations := 10
	for iter := 0; iter < maxIterations; iter++ {
		// 找到负载最重和最轻的分片
		maxShard, maxWorkload := 0, shardWorkload[0]
		minShard, minWorkload := 0, shardWorkload[0]

		for sid := 1; sid < numShards; sid++ {
			if shardWorkload[sid] > maxWorkload {
				maxShard, maxWorkload = sid, shardWorkload[sid]
			}
			if shardWorkload[sid] < minWorkload {
				minShard, minWorkload = sid, shardWorkload[sid]
			}
		}

		// 如果负载已经均衡（最大不超过平均的 1.3 倍），则停止
		if float64(maxWorkload) < avgWorkload*1.3 {
			log.Printf("  Load balanced at iteration %d (max=%d, avg=%.0f, ratio=%.2f)\n",
				iter+1, maxWorkload, avgWorkload, float64(maxWorkload)/avgWorkload)
			break
		}

		// 找到最重分片中跨片连接最多的节点
		bestNode := ""
		maxCrossShardEdges := 0

		for _, node := range shardNodes[maxShard] {
			crossShardEdges := 0
			for neighbor := range graph[node] {
				if neighborShard, exists := partition[neighbor]; exists && neighborShard != uint64(maxShard) {
					crossShardEdges++
				}
			}
			if crossShardEdges > maxCrossShardEdges {
				maxCrossShardEdges = crossShardEdges
				bestNode = node
			}
		}

		if bestNode == "" || maxCrossShardEdges == 0 {
			break
		}

		// 移动节点
		partition[bestNode] = uint64(minShard)

		// 更新分片节点列表
		for i, node := range shardNodes[maxShard] {
			if node == bestNode {
				shardNodes[maxShard] = append(shardNodes[maxShard][:i], shardNodes[maxShard][i+1:]...)
				break
			}
		}
		shardNodes[minShard] = append(shardNodes[minShard], bestNode)

		// 重新计算负载
		shardWorkload = make([]int, numShards)
		for _, tx := range allTxs {
			senderShard, senderExists := partition[tx.Sender]
			recipientShard, recipientExists := partition[tx.Recipient]

			if senderExists && recipientExists && senderShard == recipientShard {
				shardWorkload[senderShard]++
			}
		}

		if iter%2 == 0 {
			log.Printf("  Balancing iteration %d: moved node from S%d to S%d (workloads: ",
				iter+1, maxShard, minShard)
			for sid := 0; sid < numShards; sid++ {
				fmt.Printf("S%d=%d ", sid, shardWorkload[sid])
			}
			fmt.Println(")")
		}
	}

	return partition
}

// 打印分区统计信息
func printPartitionStats(partition map[string]uint64, allTxs []Transaction, numShards int) {
	shardNodes := make([]int, numShards)
	shardInternalTxs := make([]int, numShards)
	totalCrossShardTxs := 0

	for _, shard := range partition {
		shardNodes[shard]++
	}

	for _, tx := range allTxs {
		senderShard, senderExists := partition[tx.Sender]
		recipientShard, recipientExists := partition[tx.Recipient]

		if senderExists && recipientExists {
			if senderShard == recipientShard {
				shardInternalTxs[senderShard]++
			} else {
				totalCrossShardTxs++
			}
		}
	}

	log.Println("=== ContribChain Allocation Stats ===")
	for sid := 0; sid < numShards; sid++ {
		log.Printf("Shard %d: Nodes=%d, InternalTxs=%d (%.1f%% of total)\n",
			sid, shardNodes[sid], shardInternalTxs[sid],
			float64(shardInternalTxs[sid])/float64(len(allTxs))*100)
	}

	crossShardRatio := float64(totalCrossShardTxs) / float64(len(allTxs))
	log.Printf("Cross-Shard Transactions: %d / %d (%.2f%%)\n",
		totalCrossShardTxs, len(allTxs), crossShardRatio*100)
	log.Println("=======================")
}
