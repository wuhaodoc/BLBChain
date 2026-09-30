package partition

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
)

// CLPA算法状态，state of constraint label propagation algorithm
type CLPAState struct {
	NetGraph          Graph          // 需运行CLPA算法的图
	PartitionMap      map[Vertex]int // 记录分片信息的 map，某个节点属于哪个分片
	Edges2Shard       []int          // Shard 相邻接的边数，对应论文中的 total weight of edges associated with label k
	VertexsNumInShard []int          // Shard 内节点的数目
	WeightPenalty     float64        // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int            // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxIterations     int            // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int            // 跨分片边的总数
	ShardNum          int            // 分片数目
	GraphHash         []byte

	// ContribChain 扩展字段
	ContribCalculator *ContributionCalculator // 贡献值计算器
	ContribInShard    []float64               // 每个分片的总贡献值
	ContribWeight     float64                 // 贡献值权重系数
	IncentivePool     map[Vertex]float64      // 激励池
	EnableContrib     bool                    // 是否启用贡献值计算
}

func (graph *CLPAState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *CLPAState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// 加入节点，需要将它默认归到一个分片中
func (cs *CLPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1 // 此处可以批处理完之后再修改 VertexsNumInShard 参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (cs *CLPAState) AddEdge(u, v Vertex) {
	// 如果没有点，则增加边，权恒定为 1
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// 可以批处理完之后再修改 Edges2Shard 等参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 复制CLPA状态
func (dst *CLPAState) CopyCLPA(src CLPAState) {
	dst.NetGraph.CopyGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Edges2Shard = make([]int, src.ShardNum)
	copy(dst.Edges2Shard, src.Edges2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.WeightPenalty = src.WeightPenalty
	dst.MinEdges2Shard = src.MinEdges2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum

	// 复制 ContribChain 相关字段
	dst.ContribWeight = src.ContribWeight
	dst.EnableContrib = src.EnableContrib
	if src.ContribInShard != nil {
		dst.ContribInShard = make([]float64, src.ShardNum)
		copy(dst.ContribInShard, src.ContribInShard)
	}
	if src.IncentivePool != nil {
		dst.IncentivePool = make(map[Vertex]float64)
		for v, val := range src.IncentivePool {
			dst.IncentivePool[v] = val
		}
	}
}

// 输出CLPA
func (cs *CLPAState) PrintCLPA() {
	cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()

	// 如果启用贡献值，打印贡献值信息
	if cs.EnableContrib && cs.ContribInShard != nil {
		fmt.Println("Contribution per shard:")
		for sid, contrib := range cs.ContribInShard {
			fmt.Printf("Shard %d: %.4f\n", sid, contrib)
		}
	}
}

// 根据当前划分，计算 Wk，即 Edges2Shard
func (cs *CLPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0
		interEdge[idx] = 0
	}

	for v, lst := range cs.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := cs.PartitionMap[v]
		for _, u := range lst {
			// 同上，获取节点 u 所属的shard
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 Edges2Shard 加一
				// 仅计算入度，这样不会重复计算
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val
	}
	cs.CrossShardEdgeNum /= 2

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2
	}
	// 修改 MinEdges2Shard, CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// 在账户所属分片变动时，重新计算各个参数，faster
func (cs *CLPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Edges2Shard[new]++
			cs.Edges2Shard[old]--
		} else if neighborShard == new {
			cs.Edges2Shard[old]--
			cs.CrossShardEdgeNum--
		} else {
			cs.Edges2Shard[new]++
			cs.CrossShardEdgeNum++
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	// 修改 MinEdges2Shard, CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// 设置参数
func (cs *CLPAState) Init_CLPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)

	// 初始化 ContribChain 相关字段
	cs.ContribInShard = make([]float64, cs.ShardNum)
	cs.IncentivePool = make(map[Vertex]float64)
	cs.EnableContrib = false // 默认不启用
	cs.ContribWeight = 0.0
}

// 初始化划分，使用节点地址的尾数划分，应该保证初始化的时候不会出现空分片
func (cs *CLPAState) Init_Partition() {
	// 设置划分默认参数
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
}

// 不会出现空分片的初始化划分
func (cs *CLPAState) Stable_Init_Partition() error {
	// 设置划分默认参数
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes. ")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
	return nil
}

// 计算 将节点 v 放入 uShard 所产生的 score
func (cs *CLPAState) getShard_score(v Vertex, uShard int) float64 {
	var score float64
	// 节点 v 的出度
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	// uShard 与节点 v 相连的边数
	Edgesto_uShard := 0
	for _, item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1
		}
	}
	score = float64(Edgesto_uShard) / float64(v_outdegree) * (1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))
	return score
}

// CLPA 划分算法
func (cs *CLPAState) CLPA_Partition() (map[string]uint64, int) {
	cs.ComputeEdges2Shard()
	fmt.Println("Before running CLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				// 重新计算 VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				// 重新计算Wk
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}
	for sid, n := range cs.VertexsNumInShard {
		fmt.Printf("%d has vertexs: %d\n", sid, n)
	}

	cs.ComputeEdges2Shard()
	fmt.Println("After running CLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	return res, cs.CrossShardEdgeNum
}

func (cs *CLPAState) EraseEdges() {
	cs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}

// ==================== ContribChain 扩展方法 ====================

// EnableContribChain 启用贡献值计算
func (cs *CLPAState) EnableContribChain(contribWeight float64) {
	cs.EnableContrib = true
	cs.ContribWeight = contribWeight
	cs.ContribCalculator = NewContributionCalculator(&cs.NetGraph)
	cs.ContribInShard = make([]float64, cs.ShardNum)
	cs.IncentivePool = make(map[Vertex]float64)
}

// ComputeContribInShard 计算每个分片的总贡献值
func (cs *CLPAState) ComputeContribInShard() {
	if !cs.EnableContrib || cs.ContribCalculator == nil {
		return
	}

	cs.ContribInShard = make([]float64, cs.ShardNum)
	for v, shard := range cs.PartitionMap {
		contrib := cs.ContribCalculator.GetContribution(v)
		cs.ContribInShard[shard] += contrib
	}
}

// getShard_score_Contrib 计算将节点v放入uShard的得分（考虑贡献值）
func (cs *CLPAState) getShard_score_Contrib(v Vertex, uShard int) float64 {
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	if v_outdegree == 0 {
		return 0
	}

	// 计算与uShard相连的边数
	Edgesto_uShard := 0
	for _, neighbor := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[neighbor] == uShard {
			Edgesto_uShard += 1
		}
	}

	// 基础CLPA得分
	baseScore := float64(Edgesto_uShard) / float64(v_outdegree) *
		(1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))

	// 贡献值调整因子
	vContrib := cs.ContribCalculator.GetContribution(v)

	// 避免除零错误
	shardNodeCount := cs.VertexsNumInShard[uShard]
	if shardNodeCount == 0 {
		shardNodeCount = 1
	}
	avgContrib := cs.ContribInShard[uShard] / float64(shardNodeCount)

	// 如果节点贡献值高于平均值，增加其留在高贡献分片的倾向
	contribFactor := 1.0
	if avgContrib > 0 && vContrib > avgContrib {
		contribFactor = 1.0 + cs.ContribWeight*(vContrib/avgContrib-1.0)
	}

	return baseScore * contribFactor
}

// ContribChain_Partition ContribChain分片算法（基于CLPA改进）
func (cs *CLPAState) ContribChain_Partition() (map[string]uint64, int) {
	// 1. 计算所有节点的贡献值
	fmt.Println("Calculating contributions...")
	cs.ContribCalculator.CalculateAllContributions()

	// 2. 计算初始状态
	cs.ComputeEdges2Shard()
	cs.ComputeContribInShard()
	fmt.Println("Before ContribChain, cross-shard edge number:", cs.CrossShardEdgeNum)

	res := make(map[string]uint64)
	updateThreshold := make(map[string]int)

	// 3. 迭代优化
	for iter := 0; iter < cs.MaxIterations; iter++ {
		moved := false
		for v := range cs.NetGraph.VertexSet {
			if updateThreshold[v.Addr] >= 50 {
				continue
			}

			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard := cs.PartitionMap[v]
			max_scoreShard := vNowShard

			// 计算移动到每个邻居分片的得分
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score_Contrib(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}

			// 如果找到更好的分片，则移动
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				// 更新贡献值统计
				vContrib := cs.ContribCalculator.GetContribution(v)
				cs.ContribInShard[vNowShard] -= vContrib
				cs.ContribInShard[max_scoreShard] += vContrib

				// 更新分片映射
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateThreshold[v.Addr]++

				// 更新节点数统计
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1

				// 重新计算边数
				cs.changeShardRecompute(v, vNowShard)
				moved = true
			}
		}

		// 如果没有节点移动，提前结束
		if !moved {
			fmt.Printf("Converged at iteration %d\n", iter)
			break
		}
	}

	// 4. 输出结果
	for sid, n := range cs.VertexsNumInShard {
		fmt.Printf("Shard %d has vertices: %d, contribution: %.4f\n",
			sid, n, cs.ContribInShard[sid])
	}

	cs.ComputeEdges2Shard()
	fmt.Println("After ContribChain, cross-shard edge number:", cs.CrossShardEdgeNum)

	return res, cs.CrossShardEdgeNum
}

// CalculateIncentives 计算激励分配
func (cs *CLPAState) CalculateIncentives(totalReward float64) map[string]float64 {
	if !cs.EnableContrib || cs.ContribCalculator == nil {
		return nil
	}

	// 计算总贡献值
	totalContrib := 0.0
	for _, metrics := range cs.ContribCalculator.Contributions {
		totalContrib += metrics.TotalContribution
	}

	if totalContrib == 0 {
		return nil
	}

	// 按贡献值比例分配奖励
	incentives := make(map[string]float64)
	for v, metrics := range cs.ContribCalculator.Contributions {
		reward := (metrics.TotalContribution / totalContrib) * totalReward
		incentives[v.Addr] = reward
		cs.IncentivePool[v] = reward
	}

	return incentives
}

// GetTopContributors 获取贡献值最高的K个节点
func (cs *CLPAState) GetTopContributors(k int) []Vertex {
	if !cs.EnableContrib || cs.ContribCalculator == nil {
		return nil
	}
	return cs.ContribCalculator.GetTopContributors(k)
}

// GetNodeContribution 获取指定节点的贡献值
func (cs *CLPAState) GetNodeContribution(addr string) float64 {
	if !cs.EnableContrib || cs.ContribCalculator == nil {
		return 0.0
	}

	for v := range cs.NetGraph.VertexSet {
		if v.Addr == addr {
			return cs.ContribCalculator.GetContribution(v)
		}
	}
	return 0.0
}
