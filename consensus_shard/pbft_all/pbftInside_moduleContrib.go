package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"strconv"
	"time"
)

type ContribPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

// propose request
func (cphm *ContribPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if cphm.cdm.PartitionOn {
		cphm.sendPartitionReady()
		for !cphm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		cphm.sendAccounts_and_Txs()
		for !cphm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return cphm.proposePartition()
	}

	// 正常区块提议
	block := cphm.pbftNode.CurChain.GenerateBlock(int32(cphm.pbftNode.NodeID))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r
}

func (cphm *ContribPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	} else {
		if cphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			cphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
			return false
		}
	}

	cphm.pbftNode.pl.Plog.Printf("S%dN%d : pre-prepare message correct, putting into RequestPool.\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	cphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	return true
}

func (cphm *ContribPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	return true
}

func (cphm *ContribPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r, ok := cphm.pbftNode.requestPool[string(cmsg.Digest)]
	if !ok || r == nil {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : commit digest not found in requestPool, drop commit\n",
			cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
		return false
	}

	if r.RequestType == message.PartitionReq {
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		cphm.accountTransfer_do(atm)
		return true
	}

	// 处理正常区块
	block := core.DecodeB(r.Msg.Content)
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : adding block %d...now height = %d\n",
		cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number, cphm.pbftNode.CurChain.CurrentBlock.Header.Number)

	cphm.pbftNode.CurChain.AddBlock(block)

	cphm.pbftNode.pl.Plog.Printf("S%dN%d : added block %d...\n",
		cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)
	cphm.pbftNode.CurChain.PrintBlockChain()

	// 主节点处理relay交易
	if cphm.pbftNode.NodeID == uint64(cphm.pbftNode.view.Load()) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : main node sending relay txs at height = %d\n",
			cphm.pbftNode.ShardID, cphm.pbftNode.NodeID, block.Header.Number)

		cphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := cphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)

			// 核心修复：不再 panic。发现异常 tx：记录并跳过（continue）
			if !tx.Relayed && ssid != cphm.pbftNode.ShardID {
				log.Printf("incorrect tx (not relayed but senderShard mismatch): curShard=%d ssid=%d tx=%v\n",
					cphm.pbftNode.ShardID, ssid, tx)
				continue
			}
			if tx.Relayed && rsid != cphm.pbftNode.ShardID {
				log.Printf("incorrect tx (relayed but recipientShard mismatch): curShard=%d rsid=%d tx=%v\n",
					cphm.pbftNode.ShardID, rsid, tx)
				continue
			}

			if rsid != cphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				cphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		if params.RelayWithMerkleProof == 1 {
			cphm.pbftNode.RelayWithProofSend(block)
		} else {
			cphm.pbftNode.RelayMsgSend()
		}

		// 发送区块信息到supervisor
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           int(cphm.cdm.AccountTransferRound),
			Relay1Txs:       relay1Txs,
			Relay2Txs:       relay2Txs,
			SenderShardID:   cphm.pbftNode.ShardID,
			ProposeTime:     r.ReqTime,
			CommitTime:      time.Now(),
		}

		bByte, err := json.Marshal(bim)
		if err != nil {
			// 核心修复：不 panic
			log.Printf("marshal BlockInfoMsg error: %v\n", err)
			return true
		}

		msgSend := message.MergeMessage(message.CBlockInfo, bByte)
		go func() {
			// 如果你的 networks.TcpDial 已改成返回 error，就能在这里记录失败
			if err := networks.TcpDial(msgSend, cphm.pbftNode.ip_nodeTable[params.SupervisorShard][0]); err != nil {
				log.Printf("send BlockInfoMsg to supervisor error: %v\n", err)
			}
		}()

		cphm.pbftNode.pl.Plog.Printf("S%dN%d : sent executed txs\n",
			cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)

		cphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID",
			"TxPool Size",
			"# of all Txs",
			"# of Relay1 Txs",
			"# of Relay2 Txs",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",
			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs)",
			"SUM of confirm latency (ms, Relay2 Txs)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(cphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),
			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		cphm.pbftNode.writeCSVline(metricName, metricVal)
		cphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}

	return true
}

func (cphm *ContribPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	return true
}

func (cphm *ContribPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		cphm.pbftNode.pl.Plog.Printf("S%dN%d : SendOldMessage not enough\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	} else {
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				cphm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				cphm.accountTransfer_do(atm)
			}
		}
		cphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		cphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}

// 复用CLPA的账户转移逻辑
func (cphm *ContribPbftInsideExtraHandleMod) sendPartitionReady() {
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady[cphm.pbftNode.ShardID] = true
	cphm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: cphm.pbftNode.ShardID,
		NowSeqID:  cphm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(cphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

func (cphm *ContribPbftInsideExtraHandleMod) getPartitionReady() bool {
	cphm.cdm.P_ReadyLock.Lock()
	defer cphm.cdm.P_ReadyLock.Unlock()
	cphm.pbftNode.seqMapLock.Lock()
	defer cphm.pbftNode.seqMapLock.Unlock()
	cphm.cdm.ReadySeqLock.Lock()
	defer cphm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range cphm.pbftNode.seqIDMap {
		if rval, ok := cphm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(cphm.cdm.PartitionReady) == int(cphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

func (cphm *ContribPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	accountToFetch := make([]string, 0)
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	cphm.pbftNode.CurChain.Txpool.GetLocked()
	cphm.pbftNode.pl.Plog.Println("Tx pool size:", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)

		for idx, addr := range accountToFetch {
			if cphm.cdm.ModifiedMap[lastMapid][addr] == i {
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}

		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			_, ok1 := addrSet[ptx.Sender]
			condition1 := ok1 && !ptx.Relayed
			_, ok2 := addrSet[ptx.Recipient]
			condition2 := ok2 && ptx.Relayed

			if condition1 || condition2 {
				txSend = append(txSend, ptx)
			} else {
				cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
				firstPtr++
			}
		}
		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		cphm.pbftNode.pl.Plog.Printf("txSend to shard %d generated\n", i)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cphm.pbftNode.ShardID,
			Txs:          txSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[i][0])
		cphm.pbftNode.pl.Plog.Printf("Message to shard %d sent\n", i)
	}
	cphm.pbftNode.pl.Plog.Println("After sending, tx pool size:", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

func (cphm *ContribPbftInsideExtraHandleMod) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

func (cphm *ContribPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n",
		cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)

	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)
	}

	cphm.pbftNode.pl.Plog.Println("ReceivedNewTx count:", len(cphm.cdm.ReceivedNewTx))

	// 核心修复：不 panic，异常 tx 丢弃（或仅记录）
	filtered := make([]*core.Transaction, 0, len(cphm.cdm.ReceivedNewTx))
	for _, tx := range cphm.cdm.ReceivedNewTx {
		if tx == nil {
			log.Printf("error tx: nil tx in ReceivedNewTx\n")
			continue
		}

		if !tx.Relayed {
			if cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Sender] != cphm.pbftNode.ShardID {
				log.Printf("error tx (sender shard mismatch in partition): curShard=%d sender=%s mapped=%d tx=%v\n",
					cphm.pbftNode.ShardID, tx.Sender,
					cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Sender], tx)
				continue
			}
		} else {
			if cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Recipient] != cphm.pbftNode.ShardID {
				log.Printf("error tx (recipient shard mismatch in partition): curShard=%d recipient=%s mapped=%d tx=%v\n",
					cphm.pbftNode.ShardID, tx.Recipient,
					cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Recipient], tx)
				continue
			}
		}

		filtered = append(filtered, tx)
	}

	cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(filtered)
	cphm.pbftNode.pl.Plog.Println("Txpool size:", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}

	atm := message.AccountTransferMsg{
		ModifiedMap:  cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(cphm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

func (cphm *ContribPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals updated\n", cnt)

	cphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())

	if uint64(len(cphm.cdm.ModifiedMap)) != atm.ATid {
		cphm.cdm.ModifiedMap = append(cphm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	cphm.cdm.AccountTransferRound = atm.ATid
	cphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cphm.cdm.PartitionOn = false

	cphm.cdm.CollectLock.Lock()
	cphm.cdm.CollectOver = false
	cphm.cdm.CollectLock.Unlock()

	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady = make(map[uint64]bool)
	cphm.cdm.P_ReadyLock.Unlock()

	cphm.pbftNode.CurChain.PrintBlockChain()
}
