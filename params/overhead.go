package params

import "os"

// Overhead is opt-in. The legacy entry points are unchanged when Enabled is false.
var Overhead OverheadConfig

type OverheadConfig struct {
	// MigrationPolicy: stress, ContribChain, LB-Chain, or timed (legacy).
	MigrationPolicy       string
	Enabled               bool
	Lightweight           bool
	Migration             bool
	MigrationAfterSeconds int
	MigrationAccounts     int
	TimeoutSeconds        int
	// Skip streaming to this replica to exercise real on-demand recovery; -1 disables.
	StreamSkipNode int
}

// StressMigrationEnabled selects observation-driven allocation; legacy environment
// settings remain supported for old experiment scripts.
func StressMigrationEnabled() bool {
	return Overhead.MigrationPolicy == "stress" || Overhead.MigrationPolicy == "ContribChain" || Overhead.MigrationPolicy == "LB-Chain" || os.Getenv("BLB_STRESS_MIGRATION") == "1"
}

// MigrationBaseline returns the candidate selector used by the controlled study.
func MigrationBaseline() string {
	if Overhead.MigrationPolicy == "ContribChain" || Overhead.MigrationPolicy == "LB-Chain" {
		return Overhead.MigrationPolicy
	}
	if Overhead.MigrationPolicy != "" {
		return ""
	}
	return os.Getenv("BLB_MIGRATION_BASELINE")
}
