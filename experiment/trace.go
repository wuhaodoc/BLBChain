// Package experiment contains opt-in measurement helpers, not a consensus protocol.
package experiment

import (
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

var mu sync.Mutex
var files = map[string]*csv.Writer{}
var identity string

func Init(id string) {
	identity = id
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			Record("resources", []string{"unix_ns", "go_heap_bytes", "go_sys_bytes", "goroutines"}, time.Now().UnixNano(), m.HeapAlloc, m.Sys, runtime.NumGoroutine())
		}
	}()
}

// Record flushes each row so interrupted runs retain diagnostics. Failed writes are fatal.
func Record(name string, header []string, values ...interface{}) {
	if !params.Overhead.Enabled {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	w := files[name]
	if w == nil {
		dir := filepath.Join(params.ExpDataRootDir, "overhead")
		if err := os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
		f, err := os.OpenFile(filepath.Join(dir, identity+"_"+name+".csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		w = csv.NewWriter(f)
		files[name] = w
		if err := w.Write(header); err != nil {
			panic(err)
		}
	}
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = fmt.Sprint(v)
	}
	if err := w.Write(row); err != nil {
		panic(err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		panic(err)
	}
}

func Event(kind string, shard, node, epoch uint64, count int) {
	Record("events", []string{"unix_ns", "event", "shard", "node", "epoch", "accounts"}, time.Now().UnixNano(), kind, shard, node, epoch, count)
}
