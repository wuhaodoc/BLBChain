// Message handling for ExClique
package exclique

import (
	"blockEmulator/message"
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
)

// TcpListen starts the TCP listener for receiving messages
func (ec *ExCliqueNode) TcpListen() {
	ln, err := net.Listen("tcp", ec.RunningNode.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	ec.tcpln = ln

	log.Printf("[ExClique] Node S%d-N%d listening on %s\n",
		ec.ShardID, ec.NodeID, ec.RunningNode.IPaddr)

	for {
		conn, err := ec.tcpln.Accept()
		if err != nil {
			if ec.stopSignal.Load() {
				return
			}
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go ec.handleConnection(conn)
	}
}

// handleConnection handles an incoming TCP connection
func (ec *ExCliqueNode) handleConnection(conn net.Conn) {
	defer conn.Close()

	// 大缓冲区：避免 6MB 级注入消息频繁 ErrBufferFull
	reader := bufio.NewReaderSize(conn, 16*1024*1024)

	for {
		var full bytes.Buffer

		for {
			frag, err := reader.ReadSlice('\n')
			if err == nil {
				// frag 包含 '\n'
				full.Write(frag)
				break
			}

			if err == bufio.ErrBufferFull {
				// 本片段没有 '\n'，继续拼接
				full.Write(frag)
				continue
			}

			// err != nil 且不是 ErrBufferFull：连接结束或异常
			if err == io.EOF {
				// EOF 下如果还有残留数据：这是半包/截断包，直接丢弃（关键止血）
				if full.Len() > 0 {
					log.Printf("[ExClique] S%d-N%d drop truncated message at EOF (len=%d)\n",
						ec.ShardID, ec.NodeID, full.Len())
				}
				return
			}

			log.Printf("Error reading from connection: %v\n", err)
			return
		}

		b := full.Bytes()
		if len(b) == 0 {
			continue
		}

		// 只有当末尾真的是 '\n' 才去掉（关键止血：避免误删最后字节导致 unexpected EOF）
		if b[len(b)-1] == '\n' {
			b = b[:len(b)-1]
		} else {
			// 理论上 ReadSlice('\n') 不会发生，但加上防御
			log.Printf("[ExClique] S%d-N%d received message without newline terminator (len=%d), drop\n",
				ec.ShardID, ec.NodeID, len(b))
			continue
		}

		if len(b) == 0 {
			continue
		}

		// 是否要开 goroutine 看你压力：为了快速止血可以直接同步处理，避免 goroutine 爆炸
		// go ec.handleMessage(b)
		ec.handleMessage(b)
	}
}

// handleMessage handles a received message
func (ec *ExCliqueNode) handleMessage(data []byte) {
	// Check minimum message length (message format requires 30-byte prefix)
	if len(data) < 30 {
		log.Printf("[ExClique] S%d-N%d received invalid message (too short: %d bytes), ignoring\n",
			ec.ShardID, ec.NodeID, len(data))
		return
	}

	msgType, content := message.SplitMessage(data)

	log.Printf("[ExClique] S%d-N%d handling message type: '%s', content length: %d\n",
		ec.ShardID, ec.NodeID, msgType, len(content))

	switch msgType {
	case "ExCliqueBlock":
		// 核心修复：DecodeCompactBlock 不再 panic，解码失败直接丢弃
		cb, err := DecodeCompactBlock(content)
		if err != nil {
			log.Printf("[ExClique] S%d-N%d DecodeCompactBlock error (len=%d): %v\n",
				ec.ShardID, ec.NodeID, len(content), err)
			return
		}
		ec.handleCompactBlock(cb)

	case message.CInject:
		log.Printf("[ExClique] S%d-N%d detected CInject message\n", ec.ShardID, ec.NodeID)
		ec.handleInjectTxs(content)

	case message.CStop:
		ec.Stop()

	default:
		log.Printf("[ExClique] Unknown message type: %s\n", msgType)
	}
}

// handleInjectTxs handles transaction injection from supervisor
func (ec *ExCliqueNode) handleInjectTxs(content []byte) {
	log.Printf("[ExClique] S%d-N%d handleInjectTxs called with %d bytes\n",
		ec.ShardID, ec.NodeID, len(content))

	var injectMsg message.InjectTxs
	if err := json.Unmarshal(content, &injectMsg); err != nil {
		log.Printf("[ExClique] S%d-N%d Error unmarshaling inject txs: %v\n", ec.ShardID, ec.NodeID, err)
		return
	}

	log.Printf("[ExClique] S%d-N%d successfully unmarshaled %d transactions for shard %d\n",
		ec.ShardID, ec.NodeID, len(injectMsg.Txs), injectMsg.ToShardID)

	// Add transactions to pool
	ec.txPoolLock.Lock()
	for _, tx := range injectMsg.Txs {
		ec.txPool = append(ec.txPool, tx)

		// Update known transactions
		ec.knownTxLock.Lock()
		ec.knownTxs[string(tx.TxHash)] = true
		ec.cbf.Add(tx.TxHash)
		ec.knownTxLock.Unlock()
	}
	log.Printf("[ExClique] S%d-N%d txPool now has %d transactions\n",
		ec.ShardID, ec.NodeID, len(ec.txPool))
	ec.txPoolLock.Unlock()

	log.Printf("[ExClique] S%d-N%d received %d transactions\n",
		ec.ShardID, ec.NodeID, len(injectMsg.Txs))
}
