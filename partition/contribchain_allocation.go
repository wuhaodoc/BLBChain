// ContribChain community allocation state. P-Louvain attribution is retained in comments.
package partition

import (
	"log"
	"math"
	"sort"
	"sync"
)

type ContribChainAllocationState struct {
	NetGraph     *Graph
	PartitionMap map[Vertex]int

	ShardNum          int
	ShardPerformances []float64
	ShardWorkloads    []float64

	R_max float64

	lock sync.RWMutex
}

type Community struct {
	ID            int
	Vertices      []Vertex
	Workload      float64
	InternalEdges int // 新增：社区内部边数
	ExternalEdges int // 新增：社区外部边数
}

func NewContribChainAllocationState(graph *Graph, shardNum int) *ContribChainAllocationState {
	perfs := make([]float64, shardNum)
	for i := range perfs {
		perfs[i] = 1.0
	}

	return &ContribChainAllocationState{
		NetGraph:          graph,
		PartitionMap:      make(map[Vertex]int),
		ShardNum:          shardNum,
		ShardPerformances: perfs,
		ShardWorkloads:    make([]float64, shardNum),
		R_max:             0.001, // 更激进的阈值
	}
}

func (pl *ContribChainAllocationState) SetShardPerformance(shardID int, perf float64) {
	if shardID < pl.ShardNum {
		pl.ShardPerformances[shardID] = perf
	}
}

func (pl *ContribChainAllocationState) GetProcessingTime(shardID int) float64 {
	if pl.ShardPerformances[shardID] == 0 {
		return math.Inf(1)
	}
	return pl.ShardWorkloads[shardID] / pl.ShardPerformances[shardID]
}

func (pl *ContribChainAllocationState) RunContribChain() {
	pl.lock.Lock()
	defer pl.lock.Unlock()

	log.Println("=== Starting ContribChain Community Allocation ===")

	// Phase 0: 多轮 Louvain（更多迭代）
	communities := pl.detectCommunities()
	log.Printf("Phase 0: Detected %d communities\n", len(communities))

	// Phase 1: 社区移动（考虑内部连接）
	pl.phase1_CommunityMovement(communities)
	log.Println("Phase 1: Communities assigned")

	// Phase 2: 多轮账户移动（更激进）
	totalMoved := 0
	for round := 0; round < 5; round++ {
		moved := pl.phase2_AccountMovement()
		totalMoved += moved
		log.Printf("Phase 2 Round %d: %d accounts moved\n", round+1, moved)
		if moved == 0 {
			break
		}
	}
	log.Printf("Phase 2 Total: %d accounts moved\n", totalMoved)

	pl.printStats()
}

func (pl *ContribChainAllocationState) getNeighborWeights(v Vertex) map[Vertex]int {
	weights := make(map[Vertex]int)
	if neighbors, ok := pl.NetGraph.EdgeSet[v]; ok {
		for _, neighbor := range neighbors {
			weights[neighbor]++
		}
	}
	return weights
}

func (pl *ContribChainAllocationState) calculateNodeDegree(v Vertex) int {
	if neighbors, ok := pl.NetGraph.EdgeSet[v]; ok {
		return len(neighbors)
	}
	return 0
}

// Phase 0: 激进的 Louvain 算法
func (pl *ContribChainAllocationState) detectCommunities() []*Community {
	log.Println("Phase 0: ContribChain Community Detection...")

	nodeToComm := make(map[Vertex]int)
	commID := 0
	for v := range pl.NetGraph.VertexSet {
		nodeToComm[v] = commID
		commID++
	}

	// 多轮迭代（增加到 30 轮）
	maxIterations := 30
	improved := true
	iteration := 0

	for improved && iteration < maxIterations {
		improved = false
		iteration++

		// 按度数排序节点（高度节点优先）
		vertices := make([]Vertex, 0, len(pl.NetGraph.VertexSet))
		for v := range pl.NetGraph.VertexSet {
			vertices = append(vertices, v)
		}
		sort.Slice(vertices, func(i, j int) bool {
			return pl.calculateNodeDegree(vertices[i]) > pl.calculateNodeDegree(vertices[j])
		})

		for _, v := range vertices {
			currentComm := nodeToComm[v]
			bestComm := currentComm
			bestGain := 0.0

			neighborComms := make(map[int]float64)
			neighborWeights := pl.getNeighborWeights(v)

			for neighbor, weight := range neighborWeights {
				if nComm, ok := nodeToComm[neighbor]; ok {
					neighborComms[nComm] += float64(weight)
				}
			}

			currentCommWeight := neighborComms[currentComm]

			for targetComm, targetWeight := range neighborComms {
				if targetComm == currentComm {
					continue
				}

				// 激进的增益计算：优先考虑边权重
				gain := (targetWeight - currentCommWeight) * 2.0

				if gain > bestGain {
					bestGain = gain
					bestComm = targetComm
				}
			}

			// 更低的阈值（0.1 而不是 0.5）
			if bestComm != currentComm && bestGain > 0.1 {
				nodeToComm[v] = bestComm
				improved = true
			}
		}
	}

	log.Printf("  ContribChain community allocation converged after %d iterations\n", iteration)

	// 构建社区
	commMap := make(map[int]*Community)
	for v, cid := range nodeToComm {
		if _, exists := commMap[cid]; !exists {
			commMap[cid] = &Community{
				ID:            cid,
				Vertices:      make([]Vertex, 0),
				Workload:      0,
				InternalEdges: 0,
				ExternalEdges: 0,
			}
		}
		commMap[cid].Vertices = append(commMap[cid].Vertices, v)

		nodeLoad := 1.0
		neighborWeights := pl.getNeighborWeights(v)
		for neighbor, w := range neighborWeights {
			nodeLoad += float64(w) * 0.1

			// 统计内部/外部边
			if nodeToComm[neighbor] == cid {
				commMap[cid].InternalEdges++
			} else {
				commMap[cid].ExternalEdges++
			}
		}
		commMap[cid].Workload += nodeLoad
	}

	communities := make([]*Community, 0, len(commMap))
	for _, c := range commMap {
		if len(c.Vertices) > 0 {
			communities = append(communities, c)
		}
	}

	// 按社区质量排序（内部边多的优先）
	sort.Slice(communities, func(i, j int) bool {
		ratioI := float64(communities[i].InternalEdges) / float64(communities[i].InternalEdges+communities[i].ExternalEdges+1)
		ratioJ := float64(communities[j].InternalEdges) / float64(communities[j].InternalEdges+communities[j].ExternalEdges+1)
		return ratioI > ratioJ
	})

	log.Printf("  Top 5 communities (by internal edge ratio):\n")
	for i := 0; i < 5 && i < len(communities); i++ {
		c := communities[i]
		ratio := float64(c.InternalEdges) / float64(c.InternalEdges+c.ExternalEdges+1)
		log.Printf("    Community %d: %d nodes, internal ratio=%.3f\n", c.ID, len(c.Vertices), ratio)
	}

	return communities
}

// Phase 1: 社区移动（优先分配高质量社区）
func (pl *ContribChainAllocationState) phase1_CommunityMovement(communities []*Community) {
	pl.ShardWorkloads = make([]float64, pl.ShardNum)

	for _, comm := range communities {
		bestShard := -1
		minEstimatedTime := math.MaxFloat64

		for s := 0; s < pl.ShardNum; s++ {
			newWorkload := pl.ShardWorkloads[s] + comm.Workload
			estTime := newWorkload / pl.ShardPerformances[s]

			if estTime < minEstimatedTime {
				minEstimatedTime = estTime
				bestShard = s
			}
		}

		if bestShard == -1 {
			bestShard = 0
		}

		for _, v := range comm.Vertices {
			pl.PartitionMap[v] = bestShard
		}
		pl.ShardWorkloads[bestShard] += comm.Workload
	}
}

// Phase 2: 激进的账户移动
func (pl *ContribChainAllocationState) phase2_AccountMovement() int {
	movedCount := 0

	// 按节点度数排序（高度节点优先调整）
	vertices := make([]Vertex, 0, len(pl.PartitionMap))
	for v := range pl.PartitionMap {
		vertices = append(vertices, v)
	}
	sort.Slice(vertices, func(i, j int) bool {
		return pl.calculateNodeDegree(vertices[i]) > pl.calculateNodeDegree(vertices[j])
	})

	for _, v := range vertices {
		currentShard := pl.PartitionMap[v]

		neighborShards := make(map[int]float64)
		isBoundary := false

		neighborWeights := pl.getNeighborWeights(v)

		for neighbor, weight := range neighborWeights {
			if nShard, ok := pl.PartitionMap[neighbor]; ok {
				neighborShards[nShard] += float64(weight)
				if nShard != currentShard {
					isBoundary = true
				}
			}
		}

		if !isBoundary {
			continue
		}

		// 找到邻居最多的分片
		maxNeighborWeight := 0.0
		bestShard := currentShard
		for shard, weight := range neighborShards {
			if weight > maxNeighborWeight {
				maxNeighborWeight = weight
				bestShard = shard
			}
		}

		// 如果邻居分片的连接显著多于当前分片，则移动
		currentShardWeight := neighborShards[currentShard]
		if bestShard != currentShard && maxNeighborWeight > currentShardWeight*1.5 {
			nodeLoad := 1.0
			for _, w := range neighborWeights {
				nodeLoad += float64(w) * 0.1
			}

			pl.PartitionMap[v] = bestShard
			pl.ShardWorkloads[currentShard] -= nodeLoad
			if pl.ShardWorkloads[currentShard] < 0 {
				pl.ShardWorkloads[currentShard] = 0
			}
			pl.ShardWorkloads[bestShard] += nodeLoad
			movedCount++
		}
	}

	return movedCount
}

func (pl *ContribChainAllocationState) printStats() {
	log.Println("=== ContribChain Allocation Stats ===")

	shardNodeCount := make([]int, pl.ShardNum)
	for _, shard := range pl.PartitionMap {
		shardNodeCount[shard]++
	}

	for i := 0; i < pl.ShardNum; i++ {
		log.Printf("Shard %d: Nodes=%d, Workload=%.2f, Time=%.4f\n",
			i, shardNodeCount[i], pl.ShardWorkloads[i], pl.GetProcessingTime(i))
	}

	// 计算跨片边比例（估算）
	crossShardEdges := 0
	totalEdges := 0
	for v, shard := range pl.PartitionMap {
		if neighbors, ok := pl.NetGraph.EdgeSet[v]; ok {
			for _, neighbor := range neighbors {
				totalEdges++
				if nShard, ok := pl.PartitionMap[neighbor]; ok && nShard != shard {
					crossShardEdges++
				}
			}
		}
	}

	if totalEdges > 0 {
		crossRatio := float64(crossShardEdges) / float64(totalEdges)
		log.Printf("Estimated Cross-Shard Edge Ratio: %.4f (%d / %d)\n", crossRatio, crossShardEdges, totalEdges)
	}

	log.Println("=======================")
}
