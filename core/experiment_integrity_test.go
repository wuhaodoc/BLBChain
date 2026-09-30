package core

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestExperimentTransactionIntegrity(t *testing.T) {
	now := time.Unix(1700000000, 0)
	a := NewTransaction("sender-a", "recipient", big.NewInt(1), 1, now)
	b := NewTransaction("sender-b", "recipient", big.NewInt(2), 2, now)
	if len(a.Encode()) == 0 {
		t.Error("transaction encoding is empty")
	}
	if bytes.Equal(a.TxHash, b.TxHash) {
		t.Errorf("different transactions share hash %x", a.TxHash)
	}
	if len(a.Encode()) != 0 {
		restored := DecodeTx(a.Encode())
		if restored.Sender != a.Sender || restored.Value.Cmp(a.Value) != 0 {
			t.Error("transaction round trip changed payload")
		}
	}
}
