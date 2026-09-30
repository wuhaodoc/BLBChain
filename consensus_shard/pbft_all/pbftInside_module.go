// addtional module for new consensus
package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// simple implementation of pbftHandleModule interface ...
// only for block request and use transaction relay
type RawRelayPbftExtraHandleMod struct {
	pbftNode *PbftConsensusNode
	// pointer to pbft data
}

// propose request with different types
func (rphm *RawRelayPbftExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	// new blocks
	block := rphm.pbftNode.CurChain.GenerateBlock(int32(rphm.pbftNode.NodeID))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()

	return true, r
}

// the DIY operation in preprepare
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	if rphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		return false
	}
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (rphm *RawRelayPbftExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r, ok := rphm.pbftNode.requestPool[string(cmsg.Digest)]
	if !ok || r == nil {
		// 防御：避免 requestPool 缺失导致空指针/异常
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : commit digest not found in requestPool, drop commit\n",
			rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		return false
	}

	// requestType ...
	block := core.DecodeB(r.Msg.Content)
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n",
		rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number, rphm.pbftNode.CurChain.CurrentBlock.Header.Number)

	rphm.pbftNode.CurChain.AddBlock(block)

	rphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n",
		rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
	rphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n",
			rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)

		// generate relay pool and collect txs excuted
		rphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)

			// 核心修复：不再 panic，发现异常 tx 就记录并跳过 relay 处理
			if !tx.Relayed && ssid != rphm.pbftNode.ShardID {
				log.Printf("incorrect tx (not relayed but senderShard mismatch): curShard=%d ssid=%d tx=%v\n",
					rphm.pbftNode.ShardID, ssid, tx)
				continue
			}
			if tx.Relayed && rsid != rphm.pbftNode.ShardID {
				log.Printf("incorrect tx (relayed but recipientShard mismatch): curShard=%d rsid=%d tx=%v\n",
					rphm.pbftNode.ShardID, rsid, tx)
				continue
			}

			if rsid != rphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				rphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			rphm.pbftNode.RelayWithProofSend(block)
		} else {
			rphm.pbftNode.RelayMsgSend()
		}

		// send txs excuted in this block to the listener
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           0,

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: rphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			// 核心修复：不 panic，记录错误即可
			log.Printf("marshal BlockInfoMsg error: %v\n", err)
			return true
		}

		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go func() {
			if err := networks.TcpDial(msg_send, rphm.pbftNode.ip_nodeTable[params.SupervisorShard][0]); err != nil {
				log.Printf("send BlockInfoMsg to supervisor error: %v\n", err)
			}
		}()

		rphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n",
			rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)

		rphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(rphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		rphm.pbftNode.writeCSVline(metricName, metricVal)
		rphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}

	return true
}

func (rphm *RawRelayPbftExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (rphm *RawRelayPbftExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				rphm.pbftNode.CurChain.AddBlock(b)
			}
		}
		rphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		rphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
