// Precise delay range implementation for ExClique
package exclique

import (
	"math/rand"
	"time"
)

// DelayManager manages the delay parameters for ExClique
type DelayManager struct {
	beta          time.Duration // β = b(m) + v(m)
	waitWindow    time.Duration // w
	broadcastHist []time.Duration
	verifyHist    []time.Duration
	historySize   int
}

// NewDelayManager creates a new delay manager
func NewDelayManager(initialBeta, waitWindow time.Duration) *DelayManager {
	return &DelayManager{
		beta:          initialBeta,
		waitWindow:    waitWindow,
		broadcastHist: make([]time.Duration, 0),
		verifyHist:    make([]time.Duration, 0),
		historySize:   10,
	}
}

// RecordBroadcastTime records a broadcast time measurement
func (dm *DelayManager) RecordBroadcastTime(duration time.Duration) {
	dm.broadcastHist = append(dm.broadcastHist, duration)
	if len(dm.broadcastHist) > dm.historySize {
		dm.broadcastHist = dm.broadcastHist[1:]
	}
	dm.updateBeta()
}

// RecordVerifyTime records a verification time measurement
func (dm *DelayManager) RecordVerifyTime(duration time.Duration) {
	dm.verifyHist = append(dm.verifyHist, duration)
	if len(dm.verifyHist) > dm.historySize {
		dm.verifyHist = dm.verifyHist[1:]
	}
	dm.updateBeta()
}

// updateBeta updates β based on historical measurements
func (dm *DelayManager) updateBeta() {
	if len(dm.broadcastHist) == 0 || len(dm.verifyHist) == 0 {
		return
	}

	// Calculate average broadcast time
	avgBroadcast := time.Duration(0)
	for _, d := range dm.broadcastHist {
		avgBroadcast += d
	}
	avgBroadcast /= time.Duration(len(dm.broadcastHist))

	// Calculate average verify time
	avgVerify := time.Duration(0)
	for _, d := range dm.verifyHist {
		avgVerify += d
	}
	avgVerify /= time.Duration(len(dm.verifyHist))

	// β = b(m) + v(m) with safety margin
	dm.beta = (avgBroadcast + avgVerify) * 12 / 10 // 1.2x safety margin
}

// GetRandomDelay returns a random delay in range (β, w)
func (dm *DelayManager) GetRandomDelay() time.Duration {
	if dm.waitWindow <= dm.beta {
		return dm.beta
	}

	// Random delay in (β, w)
	deltaRange := dm.waitWindow - dm.beta
	randomDelta := time.Duration(rand.Int63n(int64(deltaRange)))

	return dm.beta + randomDelta
}

// GetBeta returns the current β value
func (dm *DelayManager) GetBeta() time.Duration {
	return dm.beta
}

// GetWaitWindow returns the wait window
func (dm *DelayManager) GetWaitWindow() time.Duration {
	return dm.waitWindow
}

// SetWaitWindow updates the wait window
func (dm *DelayManager) SetWaitWindow(w time.Duration) {
	dm.waitWindow = w
}

// ShouldPropose checks if a no-turn node should propose at current time
func (dm *DelayManager) ShouldPropose(timeSinceLastBlock time.Duration) bool {
	// Must be after β
	if timeSinceLastBlock < dm.beta {
		return false
	}

	// Must be before w
	if timeSinceLastBlock >= dm.waitWindow {
		return false
	}

	// Random chance based on time position in (β, w)
	// Probability increases linearly from 0 to 1
	progress := float64(timeSinceLastBlock-dm.beta) / float64(dm.waitWindow-dm.beta)

	return rand.Float64() < progress
}

// GetStats returns statistics about the delay manager
func (dm *DelayManager) GetStats() map[string]interface{} {
	avgBroadcast := time.Duration(0)
	if len(dm.broadcastHist) > 0 {
		for _, d := range dm.broadcastHist {
			avgBroadcast += d
		}
		avgBroadcast /= time.Duration(len(dm.broadcastHist))
	}

	avgVerify := time.Duration(0)
	if len(dm.verifyHist) > 0 {
		for _, d := range dm.verifyHist {
			avgVerify += d
		}
		avgVerify /= time.Duration(len(dm.verifyHist))
	}

	return map[string]interface{}{
		"beta":         dm.beta.Milliseconds(),
		"waitWindow":   dm.waitWindow.Milliseconds(),
		"avgBroadcast": avgBroadcast.Milliseconds(),
		"avgVerify":    avgVerify.Milliseconds(),
		"historySize":  len(dm.broadcastHist),
	}
}
