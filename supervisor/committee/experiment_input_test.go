package committee

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExperimentLBInputCompatibility(t *testing.T) {
	row := ",,,0x32be343b94f860124dc4fee278fdcbd38c102d88,0x104994f45d9d697ca104e5704a7b77d7fec3537c,,0,0,149990000000000000000,,,,,,,,,\n"
	path := filepath.Join(t.TempDir(), "transactions.csv")
	if err := os.WriteFile(path, []byte(row+row), 0600); err != nil {
		t.Fatal(err)
	}
	p := &BaselineAllocationCommittee{csvPath: path, dataTotalNum: 2}
	txs, err := p.loadAllTransactions()
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 2 {
		t.Errorf("loaded %d of 2 headerless transactions", len(txs))
	}
	reference, ok := data2tx(strings.Split(strings.TrimSpace(row), ","), 0)
	if !ok {
		t.Fatal("reference loader rejects fixture")
	}
	for _, tx := range txs {
		if tx.Sender != reference.Sender || tx.Recipient != reference.Recipient {
			t.Errorf("different account parsing: LB sender=%q recipient=%q; shared loader sender=%q recipient=%q", tx.Sender, tx.Recipient, reference.Sender, reference.Recipient)
		}
	}
}
