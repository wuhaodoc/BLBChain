// Classifies serialized traffic and records its byte count and send duration.
// Classifies serialized traffic and records its byte count and send duration.
package networks

import (
	"blockEmulator/experiment"
	"blockEmulator/message"
	"encoding/json"
	"sync"
	"time"
)

var migrationDigests sync.Map

// Called before preparing/voting on a partition request, so its PBFT votes are
// attributed to migration rather than ordinary transaction consensus.
func MarkMigrationDigest(digest []byte) { migrationDigests.Store(string(digest), true) }

func trafficCategory(data []byte) string {
	if len(data) < 30 {
		return "other"
	}
	t, b := message.SplitMessage(data)
	switch t {
	case "OHStream":
		return "stream"
	case "OHMissing", "OHBodies", message.CRequestOldrequest, message.CSendOldrequest:
		return "recovery"
	case "OHCompact":
		return "consensus"
	case message.CInject:
		return "injection"
	case message.CRelay, message.CRelayWithProof:
		return "cross_shard"
	case message.CPartitionMsg, message.AccountState_and_TX, message.CPartitionReady, "OHMigrationDone":
		return "migration"
	case message.CPrePrepare:
		var pp message.PrePrepare
		if json.Unmarshal(b, &pp) == nil && pp.RequestMsg != nil && pp.RequestMsg.RequestType == message.PartitionReq {
			return "migration"
		}
		return "consensus"
	case message.CPrepare, message.CCommit:
		var vote struct{ Digest []byte }
		if json.Unmarshal(b, &vote) == nil {
			if _, ok := migrationDigests.Load(string(vote.Digest)); ok {
				return "migration"
			}
		}
		return "consensus"
	case message.ViewChangePropose, message.NewChange:
		return "consensus"
	case message.CBlockInfo, message.CStop:
		return "control"
	default:
		return "other"
	}
}

func recordTraffic(data []byte, addr string, n int, err error, begin time.Time) {
	experiment.Record("network", []string{"unix_ns", "category", "destination", "bytes", "write_ms", "success"},
		time.Now().UnixNano(), trafficCategory(data), addr, n, float64(time.Since(begin).Nanoseconds())/1e6, err == nil)
}
