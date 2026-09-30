package networks

import (
	"blockEmulator/params"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

var connMaplock sync.Mutex
var randomDelayLock sync.Mutex
var connectionPool = make(map[string]net.Conn, 0)

// network params.
var randomDelayGenerator *rand.Rand
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

// Define the latency, jitter and bandwidth here.
// Init tools.
func InitNetworkTools() {
	// avoid wrong params.
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	// generate the random seed.
	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	// Limit the download rate
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	// Limit the upload rate
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

// TcpDial dials (or reuses) a connection and writes one message (ended by '\n').
// It is synchronous (returns when write finishes) so caller can measure real completion.
func TcpDial(context []byte, addr string) error {
	// simulate the delay
	thisDelay := params.Delay
	if params.JitterRange != 0 {
		randomDelayLock.Lock()
		thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
		randomDelayLock.Unlock()
	}
	time.Sleep(time.Millisecond * time.Duration(thisDelay))

	if addr == "" {
		return errors.New("empty addr")
	}

	// 轻量重试：抹平瞬时 refused / 短暂资源抖动
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		// 这里用全局锁把“取连接 + 写”串起来：
		// 快速止血，避免同一条 conn 被并发写导致对端读乱/断连。
		connMaplock.Lock()

		conn := connectionPool[addr]
		var err error

		// 没有连接就建
		if conn == nil {
			conn, err = net.DialTimeout("tcp", addr, 10*time.Second)
			if err != nil {
				connMaplock.Unlock()
				lastErr = err
				time.Sleep(time.Millisecond * time.Duration(50*(attempt+1)))
				continue
			}
			connectionPool[addr] = conn
		}

		// 写：失败则剔除坏连接
		if params.Overhead.Enabled {
			_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(params.Overhead.TimeoutSeconds) * time.Second))
		}
		begin := time.Now()
		framed := append(append([]byte(nil), context...), '\n')
		n, writeErr := (&rateLimitedWriter{writer: conn, limiter: rateLimiterUpload}).Write(framed)
		err = writeErr
		recordTraffic(context, addr, n, err, begin)
		if err != nil {
			_ = conn.Close()
			delete(connectionPool, addr)
			connMaplock.Unlock()

			lastErr = err
			time.Sleep(time.Millisecond * time.Duration(50*(attempt+1)))
			continue
		}

		connMaplock.Unlock()
		return nil
	}

	log.Println("Connect error", lastErr)
	return lastErr
}

// Broadcast sends a message to multiple receivers, excluding the sender.
// It waits until all dial+write finished, and returns counts for accurate stats.
func Broadcast(sender string, receivers []string, msg []byte) (okCnt int, failCnt int) {
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := TcpDial(msg, addr); err != nil {
				mu.Lock()
				failCnt++
				mu.Unlock()
				return
			}
			mu.Lock()
			okCnt++
			mu.Unlock()
		}(ip)
	}

	wg.Wait()
	return
}

// CloseAllConnInPool closes all connections in the connection pool.
func CloseAllConnInPool() {
	connMaplock.Lock()
	defer connMaplock.Unlock()

	for _, conn := range connectionPool {
		if conn != nil {
			_ = conn.Close()
		}
	}
	connectionPool = make(map[string]net.Conn) // Reset the pool
}

// ReadFromConn reads data from a connection.
func ReadFromConn(addr string) {
	connMaplock.Lock()
	conn := connectionPool[addr]
	connMaplock.Unlock()

	if conn == nil {
		log.Println("ReadFromConn: nil conn for address", addr)
		return
	}

	// new a conn reader
	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		// add message to buffer
		messageBuffer.Write(buffer[:n])

		// handle the full message
		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				// continue to load if buffer is short
				break
			} else if err == nil {
				// log the full message
				log.Println("Received from", addr, ":", message)
			} else {
				// handle other errs
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
