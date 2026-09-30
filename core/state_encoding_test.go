package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"testing"
)

func TestStateEncodingProcessIndependent(t *testing.T) {
	if mode := os.Getenv("STATE_ENCODING_HELPER"); mode != "" {
		if mode == "warm" {
			var b bytes.Buffer
			gob.NewEncoder(&b).Encode(struct{ Other string }{"x"})
		}
		state := &AccountState{Nonce: 42, Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)}
		encoded := state.Encode()
		decoded := DecodeAS(encoded)
		if decoded.Nonce != state.Nonce || decoded.Balance.Cmp(state.Balance) != 0 {
			os.Exit(2)
		}
		fmt.Printf("%x", encoded)
		os.Exit(0)
	}
	var results [][]byte
	for _, mode := range []string{"cold", "warm"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestStateEncodingProcessIndependent$")
		cmd.Env = append(os.Environ(), "STATE_ENCODING_HELPER="+mode)
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v %s", mode, err, data)
		}
		results = append(results, data)
	}
	if !bytes.Equal(results[0], results[1]) {
		t.Fatal("state encoding depends on prior gob type allocation")
	}
}
