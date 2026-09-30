// Differentiated ordering implementation for ExClique
package exclique

import (
	"crypto/sha256"
	"encoding/binary"
	"math/big"
	"sort"
)

// OrderManager manages the differentiated ordering of validators
type OrderManager struct {
	validators    []uint64 // List of validator node IDs
	baseOrder     []uint64 // Base ordering
	currentEpoch  uint64
	fairnessScore map[uint64]int // Track fairness scores
}

// NewOrderManager creates a new order manager
func NewOrderManager(validators []uint64) *OrderManager {
	om := &OrderManager{
		validators:    validators,
		baseOrder:     make([]uint64, len(validators)),
		currentEpoch:  0,
		fairnessScore: make(map[uint64]int),
	}

	// Initialize base order
	copy(om.baseOrder, validators)

	// Initialize fairness scores
	for _, v := range validators {
		om.fairnessScore[v] = 0
	}

	return om
}

// GetInTurnValidator returns the in-turn validator for a given block height
func (om *OrderManager) GetInTurnValidator(blockHeight uint64) uint64 {
	order := om.GetOrderForHeight(blockHeight)
	index := blockHeight % uint64(len(order))
	return order[index]
}

// GetOrderForHeight returns the validator order for a given block height
func (om *OrderManager) GetOrderForHeight(blockHeight uint64) []uint64 {
	// Calculate epoch
	epoch := blockHeight / uint64(len(om.validators))

	// If epoch changed, update order
	if epoch != om.currentEpoch {
		om.updateOrderForEpoch(epoch)
		om.currentEpoch = epoch
	}

	return om.baseOrder
}

// updateOrderForEpoch updates the validator order for a new epoch
func (om *OrderManager) updateOrderForEpoch(epoch uint64) {
	// Create a copy of validators
	newOrder := make([]uint64, len(om.validators))
	copy(newOrder, om.validators)

	// Sort by fairness score (ascending) and deterministic tie-breaking
	sort.Slice(newOrder, func(i, j int) bool {
		scoreI := om.fairnessScore[newOrder[i]]
		scoreJ := om.fairnessScore[newOrder[j]]

		if scoreI != scoreJ {
			return scoreI < scoreJ // Lower score = higher priority
		}

		// Deterministic tie-breaking using hash
		hashI := om.hashValidator(newOrder[i], epoch)
		hashJ := om.hashValidator(newOrder[j], epoch)
		return hashI.Cmp(hashJ) < 0
	})

	om.baseOrder = newOrder
}

// hashValidator creates a deterministic hash for a validator in an epoch
func (om *OrderManager) hashValidator(validatorID uint64, epoch uint64) *big.Int {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], validatorID)
	binary.BigEndian.PutUint64(data[8:16], epoch)

	hash := sha256.Sum256(data)
	return new(big.Int).SetBytes(hash[:])
}

// RecordBlock records that a validator proposed a block
func (om *OrderManager) RecordBlock(validatorID uint64, wasInTurn bool) {
	if wasInTurn {
		// In-turn block: increase score (lower priority next time)
		om.fairnessScore[validatorID]++
	} else {
		// No-turn block: decrease score (higher priority next time)
		om.fairnessScore[validatorID]--
	}
}

// GetFairnessScore returns the fairness score for a validator
func (om *OrderManager) GetFairnessScore(validatorID uint64) int {
	return om.fairnessScore[validatorID]
}

// GetAllScores returns all fairness scores
func (om *OrderManager) GetAllScores() map[uint64]int {
	scores := make(map[uint64]int)
	for k, v := range om.fairnessScore {
		scores[k] = v
	}
	return scores
}

// ResetScores resets all fairness scores
func (om *OrderManager) ResetScores() {
	for k := range om.fairnessScore {
		om.fairnessScore[k] = 0
	}
}

// CalculateGiniCoefficient calculates the Gini coefficient of block distribution
func (om *OrderManager) CalculateGiniCoefficient(blockCounts map[uint64]int) float64 {
	if len(blockCounts) == 0 {
		return 0
	}

	// Convert to sorted slice
	counts := make([]int, 0, len(blockCounts))
	for _, count := range blockCounts {
		counts = append(counts, count)
	}
	sort.Ints(counts)

	n := len(counts)
	sum := 0
	weightedSum := 0

	for i, count := range counts {
		sum += count
		weightedSum += (i + 1) * count
	}

	if sum == 0 {
		return 0
	}

	// Gini = (2 * weightedSum) / (n * sum) - (n + 1) / n
	gini := float64(2*weightedSum)/float64(n*sum) - float64(n+1)/float64(n)

	return gini
}
