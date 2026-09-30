package networks

import (
	"context"
	"io"
	"log"
	"net"

	"golang.org/x/time/rate"
)

// Write to conn with bandwidth limit.
type rateLimitedWriter struct {
	writer  io.Writer
	limiter *rate.Limiter
}

// Write method writes data in chunks gated by the limiter.
// Important: handles partial writes to net.Conn (Write may return n < len(p)).
func (w *rateLimitedWriter) Write(p []byte) (int, error) {
	total := 0

	for len(p) > 0 {
		chunk := w.limiter.Burst()
		if chunk <= 0 {
			chunk = 1
		}
		if chunk > len(p) {
			chunk = len(p)
		}

		if err := w.limiter.WaitN(context.TODO(), chunk); err != nil {
			return total, err
		}

		// WriteFull for this chunk
		written := 0
		for written < chunk {
			n, err := w.writer.Write(p[written:chunk])
			if n > 0 {
				written += n
				total += n
			}
			if err != nil {
				return total, err
			}
			if n == 0 {
				return total, io.ErrNoProgress
			}
		}

		p = p[chunk:]
	}

	return total, nil
}

func writeToConn(connMsg []byte, conn net.Conn, limiter *rate.Limiter) error {
	rateLimitedConn := &rateLimitedWriter{writer: conn, limiter: limiter}
	_, err := rateLimitedConn.Write(connMsg)
	if err != nil {
		log.Println("Write error", err)
		return err
	}
	return nil
}
