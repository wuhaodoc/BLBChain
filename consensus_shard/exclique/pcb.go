// Proactive Compact Block (PCB) implementation
package exclique

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
)

// CompactBlock represents a compact block with CBF
type CompactBlock struct {
	Header       *core.BlockHeader
	TxHashes     [][]byte            // Transaction hashes
	MissingTxs   []*core.Transaction // Transactions not in receiver's pool
	CBF          []byte              // Encoded Counting Bloom Filter
	SenderNodeID uint64
	ShardID      uint64
}

// EncodeCompactBlock encodes a compact block
func EncodeCompactBlock(cb *CompactBlock) ([]byte, error) {
	if cb == nil || cb.Header == nil {
		return nil, fmt.Errorf("EncodeCompactBlock: nil compact block or header")
	}

	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	if err := enc.Encode(cb); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// DecodeCompactBlock decodes a compact block
func DecodeCompactBlock(data []byte) (*CompactBlock, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("DecodeCompactBlock: empty data")
	}

	var cb CompactBlock
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&cb); err != nil {
		// 核心修复：这里绝对不能 panic
		return nil, err
	}

	if cb.Header == nil {
		return nil, fmt.Errorf("DecodeCompactBlock: nil header")
	}
	return &cb, nil
}

// broadcastBlockWithPCB broadcasts a block using PCB protocol
func (ec *ExCliqueNode) broadcastBlockWithPCB(block *core.Block) {
	// Get all peer addresses
	peers := make([]string, 0)
	for nodeID := uint64(0); nodeID < ec.nodeNums; nodeID++ {
		if nodeID != ec.NodeID {
			peers = append(peers, ec.ip_nodeTable[ec.ShardID][nodeID])
		}
	}

	// For each peer, create a customized compact block
	for _, peerAddr := range peers {
		go ec.sendCompactBlockToPeer(block, peerAddr)
	}
}

// sendCompactBlockToPeer sends a compact block to a specific peer
func (ec *ExCliqueNode) sendCompactBlockToPeer(block *core.Block, peerAddr string) {
	// Create compact block
	cb := &CompactBlock{
		Header:       block.Header,
		TxHashes:     make([][]byte, 0),
		MissingTxs:   make([]*core.Transaction, 0),
		CBF:          ec.cbf.Encode(),
		SenderNodeID: ec.NodeID,
		ShardID:      ec.ShardID,
	}

	// Classify transactions
	for _, tx := range block.Body {
		// For simplicity, we use local CBF as approximation
		if ec.cbf.Contains(tx.TxHash) && ec.cbf.GetCount(tx.TxHash) > 1 {
			// Peer likely has this tx, send only hash
			cb.TxHashes = append(cb.TxHashes, tx.TxHash)
		} else {
			// Peer likely doesn't have this tx, send full tx
			cb.MissingTxs = append(cb.MissingTxs, tx)
		}
	}

	// Encode and send
	cbData, err := EncodeCompactBlock(cb)
	if err != nil {
		log.Printf("[ExClique] S%d-N%d EncodeCompactBlock error: %v\n", ec.ShardID, ec.NodeID, err)
		return
	}

	msg := message.MergeMessage(message.MessageType("ExCliqueBlock"), cbData)

	// 注意：如果你已把 networks.TcpDial 改成返回 error，这里最好处理一下
	if err := networks.TcpDial(msg, peerAddr); err != nil {
		log.Printf("[ExClique] S%d-N%d TcpDial ExCliqueBlock to %s error: %v\n", ec.ShardID, ec.NodeID, peerAddr, err)
		return
	}
}

// handleCompactBlock handles a received compact block
func (ec *ExCliqueNode) handleCompactBlock(cb *CompactBlock) {
	// 基础防御：避免 nil 导致后续崩溃
	if cb == nil || cb.Header == nil {
		log.Printf("[ExClique] S%d-N%d handleCompactBlock: nil compact block/header\n", ec.ShardID, ec.NodeID)
		return
	}

	// Reconstruct full block
	fullTxs := make([]*core.Transaction, 0, len(cb.MissingTxs))

	// 你原实现里 TxHashes 只是占位，没有真正从池子补齐，这里保持原逻辑（不 panic）
	ec.knownTxLock.RLock()
	for _, txHash := range cb.TxHashes {
		_ = txHash
	}
	ec.knownTxLock.RUnlock()

	// Add missing transactions
	fullTxs = append(fullTxs, cb.MissingTxs...)

	// Create full block
	block := core.NewBlock(cb.Header, fullTxs)
	block.Hash = cb.Header.Hash()

	// Add block to chain
	ec.addBlock(block)

	log.Printf("[ExClique] S%d-N%d received compact block #%d from N%d (hashes: %d, full: %d)\n",
		ec.ShardID, ec.NodeID, block.Header.Number, cb.SenderNodeID,
		len(cb.TxHashes), len(cb.MissingTxs))
}
