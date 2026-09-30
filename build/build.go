package build

import (
	"blockEmulator/consensus_shard/exclique"
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/experiment"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor"
	"fmt"
	"log"
	"time"
)

func initConfig(nid, nnm, sid, snm uint64) *params.ChainConfig {
	// Read the contents of ipTable.json
	ipMap := readIpTable("./ipTable.json")
	params.IPmap_nodeTable = ipMap
	params.SupervisorAddr = params.IPmap_nodeTable[params.SupervisorShard][0]

	// check the correctness of params
	if len(ipMap)-1 < int(snm) {
		log.Panicf("Input ShardNumber = %d, but only %d shards in ipTable.json.\n", snm, len(ipMap)-1)
	}
	for shardID := 0; shardID < len(ipMap)-1; shardID++ {
		if len(ipMap[uint64(shardID)]) < int(nnm) {
			log.Panicf("Input NodeNumber = %d, but only %d nodes in Shard %d.\n", nnm, len(ipMap[uint64(shardID)]), shardID)
		}
	}

	params.NodesInShard = int(nnm)
	params.ShardNum = int(snm)

	if params.Overhead.Enabled {
		id := fmt.Sprintf("s%d_n%d", sid, nid)
		if sid == 123 {
			id = "supervisor"
		}
		experiment.Init(id)
	}
	// init the network layer
	networks.InitNetworkTools()

	pcc := &params.ChainConfig{
		ChainID:        sid,
		NodeID:         nid,
		ShardID:        sid,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      snm,
		BlockSize:      uint64(params.MaxBlockSize_global),
		BlockInterval:  uint64(params.Block_Interval),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return pcc
}

func BuildSupervisor(nnm, snm uint64) {
	methodID := params.ConsensusMethod
	var measureMod []string
	if methodID == 0 || methodID == 2 {
		measureMod = params.MeasureBrokerMod
	} else {
		measureMod = params.MeasureRelayMod
	}
	measureMod = append(measureMod, "Tx_Details")

	lsn := new(supervisor.Supervisor)
	pcc := initConfig(123, nnm, 123, snm)
	method := configuredMethod()
	if params.Overhead.Enabled {
		method = "Overhead"
		measureMod = nil
	}
	lsn.NewSupervisor(params.SupervisorAddr, pcc, method, measureMod...)
	go lsn.TcpListen()
	time.Sleep(5000 * time.Millisecond)
	lsn.SupervisorTxHandling()
}

func BuildNewPbftNode(nid, nnm, sid, snm uint64) {
	method := configuredMethod()
	if method == "LB-Chain" || method == "ContribChain" {
		method = "CLPA"
	}
	if params.Overhead.Enabled {
		method = "CLPA"
	}
	worker := pbft_all.NewPbftNode(sid, nid, initConfig(nid, nnm, sid, snm), method)
	go worker.TcpListen()
	worker.Propose()
}

func configuredMethod() string {
	if params.ConsensusMethod < 0 || params.ConsensusMethod >= len(params.CommitteeMethod) {
		panic("invalid ConsensusMethod")
	}
	return params.CommitteeMethod[params.ConsensusMethod]
}

// BuildNewExCliqueNode creates and starts an ExClique consensus node
func BuildNewExCliqueNode(nid, nnm, sid, snm uint64) {
	log.Printf("[Build] Creating ExClique node: Shard=%d, Node=%d\n", sid, nid)
	worker := exclique.NewExCliqueNode(sid, nid, initConfig(nid, nnm, sid, snm))
	go worker.TcpListen()
	worker.Propose()
}
