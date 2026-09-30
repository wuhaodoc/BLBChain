package measure

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"testing"
	"time"
)

func TestEmptyEpochDoesNotCorruptTPS(t *testing.T) {
	old := params.DataWrite_path
	params.DataWrite_path = t.TempDir() + "/"
	defer func() { params.DataWrite_path = old }()
	m := NewTestModule_avgTPS_Relay()
	m.UpdateMeasureRecord(&message.BlockInfoMsg{Epoch: 1, BlockBodyLength: 20, InnerShardTxs: make([]*core.Transaction, 20), ProposeTime: time.Unix(100, 0), CommitTime: time.Unix(102, 0)})
	per, total := m.OutputRecord()
	if total != 10 || per[0] != 0 || per[1] != 10 {
		t.Fatalf("empty epoch included in TPS window: %v %v", per, total)
	}
}
