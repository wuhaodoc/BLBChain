// Counting Bloom Filter implementation for ExClique
package exclique

import (
	"hash/fnv"
	"sync"
)

// CountingBloomFilter implements a counting bloom filter
type CountingBloomFilter struct {
	counters []uint8
	size     uint64
	hashNum  uint
	lock     sync.RWMutex
}

// NewCountingBloomFilter creates a new counting bloom filter
func NewCountingBloomFilter(size uint64, hashNum uint) *CountingBloomFilter {
	return &CountingBloomFilter{
		counters: make([]uint8, size),
		size:     size,
		hashNum:  hashNum,
	}
}

// Add adds an item to the filter
func (cbf *CountingBloomFilter) Add(item []byte) {
	cbf.lock.Lock()
	defer cbf.lock.Unlock()

	for i := uint(0); i < cbf.hashNum; i++ {
		pos := cbf.hash(item, i)
		if cbf.counters[pos] < 255 {
			cbf.counters[pos]++
		}
	}
}

// Remove removes an item from the filter
func (cbf *CountingBloomFilter) Remove(item []byte) {
	cbf.lock.Lock()
	defer cbf.lock.Unlock()

	for i := uint(0); i < cbf.hashNum; i++ {
		pos := cbf.hash(item, i)
		if cbf.counters[pos] > 0 {
			cbf.counters[pos]--
		}
	}
}

// Contains checks if an item might be in the filter
func (cbf *CountingBloomFilter) Contains(item []byte) bool {
	cbf.lock.RLock()
	defer cbf.lock.RUnlock()

	for i := uint(0); i < cbf.hashNum; i++ {
		pos := cbf.hash(item, i)
		if cbf.counters[pos] == 0 {
			return false
		}
	}
	return true
}

// GetCount returns the minimum count for an item
func (cbf *CountingBloomFilter) GetCount(item []byte) uint8 {
	cbf.lock.RLock()
	defer cbf.lock.RUnlock()

	minCount := uint8(255)
	for i := uint(0); i < cbf.hashNum; i++ {
		pos := cbf.hash(item, i)
		if cbf.counters[pos] < minCount {
			minCount = cbf.counters[pos]
		}
	}
	return minCount
}

// hash computes the hash for an item with a given seed
func (cbf *CountingBloomFilter) hash(item []byte, seed uint) uint64 {
	h := fnv.New64a()
	h.Write(item)
	h.Write([]byte{byte(seed)})
	return h.Sum64() % cbf.size
}

// Clear resets the filter
func (cbf *CountingBloomFilter) Clear() {
	cbf.lock.Lock()
	defer cbf.lock.Unlock()

	for i := range cbf.counters {
		cbf.counters[i] = 0
	}
}

// Size returns the size of the filter
func (cbf *CountingBloomFilter) Size() uint64 {
	return cbf.size
}

// Encode serializes the CBF for network transmission
func (cbf *CountingBloomFilter) Encode() []byte {
	cbf.lock.RLock()
	defer cbf.lock.RUnlock()

	// Simple encoding: just return the counters
	result := make([]byte, len(cbf.counters))
	copy(result, cbf.counters)
	return result
}

// Decode deserializes the CBF from network data
func (cbf *CountingBloomFilter) Decode(data []byte) {
	cbf.lock.Lock()
	defer cbf.lock.Unlock()

	if len(data) == len(cbf.counters) {
		copy(cbf.counters, data)
	}
}
