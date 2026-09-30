package build

import (
	"blockEmulator/params"
	"testing"
)

func TestExperimentMethodSelection(t *testing.T) {
	old := params.ConsensusMethod
	defer func() { params.ConsensusMethod = old }()
	for id, want := range params.CommitteeMethod {
		params.ConsensusMethod = id
		if got := configuredMethod(); got != want {
			t.Fatalf("method %d: got %q want %q", id, got, want)
		}
	}
}
