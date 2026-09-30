package main

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	result := map[string]interface{}{}
	for s := 0; s < 4; s++ {
		var reference string
		for n := 0; n < 4; n++ {
			path := filepath.Join(os.Args[1], "results/database/chainDB", fmt.Sprintf("S%d_N%d", s, n))
			db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
			if err != nil {
				panic(err)
			}
			err = db.View(func(tx *bolt.Tx) error {
				hash := tx.Bucket([]byte("newestBlockHash")).Get([]byte("OnlyNewestBlock"))
				b := core.DecodeB(tx.Bucket([]byte("block")).Get(hash))
				h := hex.EncodeToString(hash)
				if n == 0 {
					reference = h
				} else if h != reference {
					return fmt.Errorf("shard %d replica %d tip mismatch", s, n)
				}
				result[fmt.Sprintf("s%d_n%d", s, n)] = map[string]interface{}{"height": b.Header.Number, "hash": h, "state_root": hex.EncodeToString(b.Header.StateRoot)}
				return nil
			})
			db.Close()
			if err != nil {
				panic(err)
			}
		}
	}
	json.NewEncoder(os.Stdout).Encode(result)
}
