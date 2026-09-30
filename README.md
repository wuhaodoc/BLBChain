# BLBChain

Research prototype for **Beyond Stress-Balanced Sharding: A Cross-Layer Perspective on Throughput Scaling in Blockchain**, built on BlockEmulator.

This source package integrates compact-proposal streaming, missing-body recovery, capacity-aware account allocation, baseline allocation adapters, and auditable performance experiments. Scheme names are explicit: no numeric method ID is needed for the provided experiments.

## Scheme map

| Scheme | Main entry | Behavior in the controlled experiments |
|---|---|---|
| BLBChain | `supervisor/committee/blbchain_allocation.go` | Stream pre-distribution plus stress-based allocation when migration is enabled |
| BLBChain-Capacity | `consensus_shard/pbft_all/blbchain_stream.go` | Stream pre-distribution with migration disabled |
| ContribChain | `supervisor/committee/contribchain.go`, `contribchain_allocation.go` | Community-based account allocation; full proposals |
| LB-Chain | `supervisor/committee/lbchain.go`, `lbchain_allocation.go` | Load-based active-account migration; full proposals |
| Presto | `supervisor/committee/presto_relay.go` | Existing post-commit relay implementation; legacy mode |
| CLPA / Broker | `supervisor/committee/committee_clpa.go`, `committee_broker.go` | Existing legacy implementations |

Implementation scope and shared experiment machinery are documented in [IMPLEMENTATION.md](docs/IMPLEMENTATION.md). A scheme label identifies the implementation evaluated here; it is not a claim of an unchanged reproduction of every feature in the corresponding paper.

## Requirements

- Go 1.19 or newer (a recent Go toolchain is recommended).
- Python 3.9 or newer for experiment orchestration.
- Optional plotting dependencies: `python -m pip install -r requirements.txt`.
- A local TCP environment; the runner starts one process per node plus a supervisor.

Run commands from the repository root. The first Go build downloads the dependencies pinned in `go.mod` / `go.sum`.

## Quick check

```sh
go test ./params ./build ./core ./networks ./supervisor/committee ./supervisor/measure
python experiments/run.py --study smoke --schemes BLBChain-Capacity BLBChain --output runs/smoke
```

The smoke command creates **synthetic data for correctness checking only**. It is not a paper performance result. Outputs are never overwritten: use a new directory for each run.

## Dataset

Provide a CSV with a header and columns `index,unused,sender,recipient,value`. Addresses must be 40 hexadecimal characters (an input `0x` prefix is accepted); values are nonnegative integers. The runner selects the first `--total` rows and preserves arrival order. Do not substitute the smoke trace for the evaluation trace.

The original transaction dataset is not bundled. See [data/README.md](data/README.md). Record its source and redistribution terms before publishing a trace.

## Reproduce an experiment

Default common settings: 4 shards, 4 nodes per shard, 2,000 transactions per block, 3,000 ms block interval, 3,000 transactions/s injection, batch size 300, and 60,000 transactions. Bandwidth is 100 Mbps unless swept.

### Migration count: 1–5 accounts

```sh
python experiments/run.py --study migration --schemes BLBChain-Capacity BLBChain ContribChain LB-Chain --dataset data/transactions.csv --accounts 1 2 3 4 5 --bandwidth-mbps 100 --output runs/migration
python experiments/plot.py runs/migration/summary.csv --x requested_accounts --output runs/migration/comparison
```

The capacity-only run has zero migrations and is a constant reference. Other schemes use their own candidate selector with a count cap, a common observation trigger, and the same state-transfer barrier. Requested and actual counts are exported separately. A point with fewer accepted moves is rejected from the requested-count summary rather than relabeled.

### Bandwidth: 5–10 Mbps

```sh
python experiments/run.py --study bandwidth --schemes BLBChain ContribChain LB-Chain --dataset data/transactions.csv --bandwidth-mbps 5 6 7 8 9 10 --output runs/bandwidth
python experiments/plot.py runs/bandwidth/summary.csv --x bandwidth_mbps --output runs/bandwidth/comparison
```

This controlled network experiment disables account migration in all schemes. Consequently BLBChain here exercises its capacity component. It must not be described as an evaluation of each baseline's native periodic allocation behavior.

### One operating point

```sh
python experiments/run.py --study point --schemes BLBChain --dataset data/transactions.csv --output runs/point
```

Use `--repeats`, `--total`, `--rate`, `--block-size`, `--block-ms`, `--shards`, `--nodes`, and `--timeout` to set the experiment explicitly. `--skip-stream-node` deliberately withholds stream data from one replica to test recovery; keep the default `-1` for normal performance runs.

## Results and completion

- `status.json`: `running`, `validating`, `complete`, or `partial`.
- `manifest.json`: arguments, data hash, and executable hash.
- `summary.csv`: validated TPS, confirmation latency (ms), actual migration count, and migration duration (ms).
- `invalid_trials.csv`: failures and count mismatches; never silently converted into measurements.
- `trials/`: per-run config, logs, account-state audits, timing and traffic records.

TPS is unique finalized transactions divided by the interval from the first scheduled arrival to the last final commit. Confirmation latency includes batching, queueing, and drain time. Migration duration is a different metric: request to acknowledgments from all replicas. See [METRICS.md](docs/METRICS.md).

## Source guide

See [CODE_MAP.md](docs/CODE_MAP.md) for every Go/Python source file and [CONFIGURATION.md](docs/CONFIGURATION.md) for units and selection rules. Existing wire names, Go module path `blockEmulator`, and legacy numeric IDs are preserved for compatibility.

## Attribution and status

The BlockEmulator license and attribution are preserved in [LICENSE](LICENSE). This is a source-only research release. No experiment results, datasets, databases, executables or validation reports are bundled. All study commands generate fresh results locally. The baseline routing was corrected in this release; previous measured values must not be assumed to reproduce under the changed algorithms.
