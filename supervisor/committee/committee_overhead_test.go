package committee

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOverheadCSVPreservesIntegerPrecision(t *testing.T) {
	p := filepath.Join(t.TempDir(), "input.csv")
	data := "i,x,sender,recipient,value\n0,,0000000000000000000000000000000000000001,0000000000000000000000000000000000000002,123456789012345678901234567890\n"
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	rows, err := loadOverheadCSV(p, 1)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].value.String() != "123456789012345678901234567890" {
		t.Fatal("integer truncated")
	}
	if _, err = loadOverheadCSV(p, 2); err == nil {
		t.Fatal("short dataset accepted")
	}
	if err := os.WriteFile(p, []byte("i,x,s,r,v\n0,,bad,bad,1.5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadOverheadCSV(p, 1); err == nil {
		t.Fatal("malformed row accepted")
	}
}
