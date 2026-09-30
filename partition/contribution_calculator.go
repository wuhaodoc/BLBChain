package partition

import (
	"sync"
)

// ContributionMetrics 贡献值指标
type ContributionMetrics struct {
	TotalTxCount      int     // 总交易数
	IntraShardTxCount int     // 片内交易数
	CrossShardTxCount int     // 跨片交易数
	ContribScore      float64 // 贡献分数
	TotalContribution float64 // 总贡献值（用于CLPA算法）
	LastUpdateEpoch   int     // 最后更新的epoch
}

// ContributionCalculator 贡献值计算器
type ContributionCalculator struct {
	NetGraph      *Graph                          // 网络图引用
	Contributions map[Vertex]*ContributionMetrics // 每个顶点的贡献值指标
	DecayFactor   float64                         // 衰减因子
	lock          sync.RWMutex                    // 读写锁
}

// NewContributionCalculator 创建新的贡献值计算器
func NewContributionCalculator(graph *Graph) *ContributionCalculator {
	return &ContributionCalculator{
		NetGraph:      graph,
		Contributions: make(map[Vertex]*ContributionMetrics),
		DecayFactor:   0.9, // 默认衰减因子
	}
}

// GetContribution 获取顶点的贡献值
func (cc *ContributionCalculator) GetContribution(v Vertex) float64 {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	if metrics, exists := cc.Contributions[v]; exists {
		return metrics.ContribScore
	}
	return 0.0
}

// UpdateContribution 更新顶点的贡献值
func (cc *ContributionCalculator) UpdateContribution(v Vertex, isIntraShard bool, epoch int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	if _, exists := cc.Contributions[v]; !exists {
		cc.Contributions[v] = &ContributionMetrics{
			TotalTxCount:      0,
			IntraShardTxCount: 0,
			CrossShardTxCount: 0,
			ContribScore:      0.0,
			TotalContribution: 0.0,
			LastUpdateEpoch:   epoch,
		}
	}

	metrics := cc.Contributions[v]
	metrics.TotalTxCount++

	if isIntraShard {
		metrics.IntraShardTxCount++
		// 片内交易增加贡献值
		metrics.ContribScore += 1.0
		metrics.TotalContribution += 1.0
	} else {
		metrics.CrossShardTxCount++
		// 跨片交易不增加或减少贡献值
		// metrics.ContribScore -= 0.5  // 可选：惩罚跨片交易
	}

	metrics.LastUpdateEpoch = epoch
}

// CalculateAllContributions 计算所有顶点的贡献值（批量计算）
func (cc *ContributionCalculator) CalculateAllContributions() {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	// 对所有顶点应用衰减
	for v, metrics := range cc.Contributions {
		metrics.ContribScore *= cc.DecayFactor
		metrics.TotalContribution *= cc.DecayFactor

		// 清理贡献值过低的记录
		if metrics.ContribScore < 0.01 {
			delete(cc.Contributions, v)
		}
	}
}

// GetTopContributors 获取贡献值最高的k个顶点
func (cc *ContributionCalculator) GetTopContributors(k int) []Vertex {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	// 创建顶点切片
	type vertexContrib struct {
		vertex  Vertex
		contrib float64
	}

	vertices := make([]vertexContrib, 0, len(cc.Contributions))
	for v, metrics := range cc.Contributions {
		vertices = append(vertices, vertexContrib{
			vertex:  v,
			contrib: metrics.TotalContribution,
		})
	}

	// 简单冒泡排序（实际应用中可以用更高效的排序）
	for i := 0; i < len(vertices) && i < k; i++ {
		for j := i + 1; j < len(vertices); j++ {
			if vertices[j].contrib > vertices[i].contrib {
				vertices[i], vertices[j] = vertices[j], vertices[i]
			}
		}
	}

	// 返回top k
	result := make([]Vertex, 0, k)
	for i := 0; i < len(vertices) && i < k; i++ {
		result = append(result, vertices[i].vertex)
	}

	return result
}

// ResetContributions 重置所有贡献值
func (cc *ContributionCalculator) ResetContributions() {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	cc.Contributions = make(map[Vertex]*ContributionMetrics)
}

// GetMetrics 获取顶点的详细指标
func (cc *ContributionCalculator) GetMetrics(v Vertex) *ContributionMetrics {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	if metrics, exists := cc.Contributions[v]; exists {
		// 返回副本以避免并发问题
		return &ContributionMetrics{
			TotalTxCount:      metrics.TotalTxCount,
			IntraShardTxCount: metrics.IntraShardTxCount,
			CrossShardTxCount: metrics.CrossShardTxCount,
			ContribScore:      metrics.ContribScore,
			TotalContribution: metrics.TotalContribution,
			LastUpdateEpoch:   metrics.LastUpdateEpoch,
		}
	}
	return nil
}

// SetDecayFactor 设置衰减因子
func (cc *ContributionCalculator) SetDecayFactor(factor float64) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	if factor > 0 && factor <= 1.0 {
		cc.DecayFactor = factor
	}
}

// GetTotalContribution 获取所有顶点的总贡献值
func (cc *ContributionCalculator) GetTotalContribution() float64 {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	total := 0.0
	for _, metrics := range cc.Contributions {
		total += metrics.TotalContribution
	}
	return total
}

// GetAverageContribution 获取平均贡献值
func (cc *ContributionCalculator) GetAverageContribution() float64 {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	if len(cc.Contributions) == 0 {
		return 0.0
	}

	total := 0.0
	for _, metrics := range cc.Contributions {
		total += metrics.TotalContribution
	}
	return total / float64(len(cc.Contributions))
}

// GetContributionByVertex 获取指定顶点的总贡献值
func (cc *ContributionCalculator) GetContributionByVertex(v Vertex) float64 {
	cc.lock.RLock()
	defer cc.lock.RUnlock()

	if metrics, exists := cc.Contributions[v]; exists {
		return metrics.TotalContribution
	}
	return 0.0
}
