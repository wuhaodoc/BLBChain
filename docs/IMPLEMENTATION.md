# Implementation and baseline ownership

## Scheme names

BLBChain, BLBChain-Capacity, ContribChain, LB-Chain, Presto, CLPA and Broker are the public names. There is no separate Louvain baseline or configuration. P-Louvain is an account-allocation algorithm associated with ContribChain; its attribution remains in comments, not as a public scheme name.

## ContribChain

`contribchain.go` is the entry and `contribchain_allocation.go` contains the inherited community-allocation heuristic, including transaction-frequency initialization, community and boundary refinement, and a load-balancing pass. `partition/contribchain_allocation.go` retains the separate graph-state implementation under the ContribChain namespace. The old account-score/round-robin heuristic is no longer used or shipped as ContribChain.

These are adapted implementations. This package does not claim to reproduce the full published ContribChain node-contribution evaluation or NACV committee assignment. The active community heuristic is not a verified exact reproduction of the published P-Louvain procedure.

## LB-Chain

`lbchain.go` is the entry and `lbchain_allocation.go` implements a separate load-based allocation adapter. It moves active accounts from high-load to low-load shards, accepts only load-dispersion reductions, limits the selection to 10 iterations, and stops at a maximum-to-average load ratio of 1.3. It does not call a community or Louvain allocator.

In the controlled experiment, account load is outstanding sender work plus an estimate of upcoming work from observed arrival rate. This is a simple predictor, not the learned predictor from the original paper. State transfer uses the shared experiment barrier, not a complete reproduction of the original LB-Chain migration protocol. The native initialization path uses transaction counts from its input scan; use the controlled runner for history-only allocation.

## BLBChain

`blbchain_stream.go` implements body caching, pre-distribution, compact hash-keyed proposals and missing-body recovery. `blbchain_allocation.go` estimates capacity from observed rounds and accepts capacity-aware stress-variance reductions. BLBChain-Capacity disables migration.

The current supervisor harness is not a distributed BFT A-Shard. The stream path does not implement the manuscript's complete tip-list/cut-height protocol. These remain implementation gaps, not features added by renaming this code.

## Presto, CLPA and Broker

Presto selects the inherited post-commit relay path with optional Merkle proofs; it does not implement optimistic execution. CLPA and Broker retain their respective inherited allocation and relay implementations. Their legacy modes are provided, but the audited experiment CLI currently supports the four schemes listed in its --help.

## Shared controlled experiments

The controlled path enables Overhead instrumentation, preserves scheduled arrival timestamps, and uses a common CLPA state-transfer barrier. It waits for at least two nonempty block observations per shard before trying one migration. Candidate selectors are scheme-specific; an account-count cap is imposed and actual counts are checked. This observation trigger replaces native timing for the controlled study.

The native baseline path performs initial allocation from its input scan; the saved ReconfigTimeGap is not a guarantee of periodic reallocation in that entry. Wire messages and numeric IDs remain for compatibility, but algorithm routing changed in this release. Rerun comparisons before claiming quantitative results for this code.
