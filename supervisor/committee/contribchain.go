// ContribChain public committee entry; allocation code is separate from transport.
package committee

import (
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
)

func NewContribChainCommittee(ips map[uint64]map[uint64]string, ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, path string, total, batch, interval int) *BaselineAllocationCommittee {
	return newBaselineAllocationCommittee(ips, ss, sl, path, total, batch, interval, "ContribChain")
}
