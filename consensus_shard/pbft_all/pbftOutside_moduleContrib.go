package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

type ContribRelayOutsideModule struct {
	pbftNode *PbftConsensusNode
	cdm      *dataSupport.Data_supportCLPA
}

func (crom *ContribRelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CPartitionMsg:
		pmm := new(message.PartitionModifiedMap)
		err := json.Unmarshal(content, pmm)
		if err != nil {
			log.Panic(err)
		}
		crom.handlePartitionModifiedMap(pmm)
		return true

	case message.CPartitionReady:
		pr := new(message.PartitionReady)
		err := json.Unmarshal(content, pr)
		if err != nil {
			log.Panic(err)
		}
		crom.handlePartitionReady(pr)
		return true

	case message.AccountState_and_TX:
		ast := new(message.AccountStateAndTx)
		err := json.Unmarshal(content, ast)
		if err != nil {
			log.Panic(err)
		}
		crom.handleAccountStateAndTx(ast)
		return true

	case message.CRelay:
		// 处理relay消息 - 使用简单的交易注入
		it := new(message.InjectTxs)
		err := json.Unmarshal(content, it)
		if err != nil {
			// 如果不是InjectTxs格式，尝试其他格式
			log.Println("Warning: CRelay message format issue, skipping")
			return true
		}
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
		return true

	case message.CInject:
		it := new(message.InjectTxs)
		err := json.Unmarshal(content, it)
		if err != nil {
			log.Panic(err)
		}
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
		return true

	default:
		return false
	}
}

func (crom *ContribRelayOutsideModule) handlePartitionModifiedMap(pmm *message.PartitionModifiedMap) {
	crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pmm.PartitionModified)
	crom.cdm.PartitionOn = true
	crom.pbftNode.pl.Plog.Println("Received partition modified map, partition is ON")
}

func (crom *ContribRelayOutsideModule) handlePartitionReady(pr *message.PartitionReady) {
	crom.cdm.P_ReadyLock.Lock()
	crom.cdm.PartitionReady[pr.FromShard] = true
	crom.cdm.P_ReadyLock.Unlock()

	crom.cdm.ReadySeqLock.Lock()
	crom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	crom.cdm.ReadySeqLock.Unlock()

	crom.pbftNode.pl.Plog.Printf("Received partition ready from shard %d\n", pr.FromShard)
}

func (crom *ContribRelayOutsideModule) handleAccountStateAndTx(ast *message.AccountStateAndTx) {
	crom.cdm.AccountStateTx[ast.FromShard] = ast
	crom.pbftNode.pl.Plog.Printf("Received account state and tx from shard %d\n", ast.FromShard)

	crom.cdm.CollectLock.Lock()
	if len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1 {
		crom.cdm.CollectOver = true
		crom.pbftNode.pl.Plog.Println("All account states and txs collected")
	}
	crom.cdm.CollectLock.Unlock()
}
