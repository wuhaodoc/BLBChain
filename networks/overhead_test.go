package networks

import (
	"blockEmulator/message"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"golang.org/x/time/rate"
)

func TestTrafficClassification(t *testing.T) {
	for typ, want := range map[message.MessageType]string{"OHStream": "stream", "OHMissing": "recovery", "OHBodies": "recovery", message.CInject: "injection", message.CRelay: "cross_shard", message.CPartitionMsg: "migration", "OHMigrationDone": "migration", message.CBlockInfo: "control"} {
		if got := trafficCategory(message.MergeMessage(typ, []byte("{}"))); got != want {
			t.Fatalf("%s: %s != %s", typ, got, want)
		}
	}
	d := []byte("migration-round-test")
	MarkMigrationDigest(d)
	b, _ := json.Marshal(message.Prepare{Digest: d})
	if trafficCategory(message.MergeMessage(message.CPrepare, b)) != "migration" {
		t.Fatal("partition vote not attributed to migration")
	}
	b, _ = json.Marshal(message.Prepare{Digest: []byte("block")})
	if trafficCategory(message.MergeMessage(message.CPrepare, b)) != "consensus" {
		t.Fatal("ordinary vote misclassified")
	}
}

type partialWriter struct {
	bytes.Buffer
	fail bool
}

func (w *partialWriter) Write(b []byte) (int, error) {
	if len(b) > 3 {
		b = b[:3]
	}
	n, _ := w.Buffer.Write(b)
	if w.fail {
		return n, io.ErrClosedPipe
	}
	return n, nil
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestLimitedWriterCountsActualBytes(t *testing.T) {
	w := &partialWriter{}
	limited := rateLimitedWriter{writer: w, limiter: rate.NewLimiter(rate.Inf, 4)}
	n, err := limited.Write([]byte("abcdefghij"))
	if err != nil || n != 10 || w.String() != "abcdefghij" {
		t.Fatalf("%d %v %s", n, err, w.String())
	}
	w = &partialWriter{fail: true}
	limited.writer = w
	n, err = limited.Write([]byte("abcdefghij"))
	if n != 3 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("partial failure: %d %v", n, err)
	}
	limited.writer = zeroWriter{}
	n, err = limited.Write([]byte("x"))
	if n != 0 || !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero writer: %d %v", n, err)
	}
}
