# BLBChain

Research code accompanying **Beyond Stress-Balanced Sharding: A Cross-Layer Perspective on Throughput Scaling in Blockchain**.

**Authors:** Hao Wu, Rui Jin, Yebo Feng, Yu Liu, Konglin Zhu, and Lin Zhang.

BLBChain studies throughput scaling through coordinated transaction dissemination and account allocation. Its network-layer mechanism pre-distributes transaction bodies and uses lightweight proposals, while its application-layer mechanism allocates active accounts according to shard workload and processing capacity.

This prototype builds on BlockEmulator. It provides body streaming, compact proposals, missing-body recovery, capacity-aware allocation, baseline implementations, and experiment scripts. The complete tip-list/cut-height protocol and a distributed BFT A-Shard are not implemented in this release. See [Implementation scope](docs/IMPLEMENTATION.md) for protocol coverage and baseline adaptations.

## Repository structure

| Path | Purpose |
| --- | --- |
| `main.go`, `build/` | Program entry point and node/supervisor construction. |
| `params/`, `paramsConfig.json`, `configs/` | Parameters and scheme configurations. |
| `supervisor/committee/` | Transaction injection, allocation, and migration coordination. |
| `consensus_shard/pbft_all/` | PBFT, pre-distribution, compact proposals, recovery, and state transfer. |
| `consensus_shard/exclique/` | Inherited experimental ExClique components. |
| `partition/` | Account-partitioning components. |
| `core/`, `chain/`, `storage/`, `shard/` | Transactions, blocks, state storage, and shard structures. |
| `networks/`, `message/`, `utils/` | Networking, messages, and shared utilities. |
| `broker/`, `supervisor/measure/` | Broker components and performance measurements. |
| `experiment/`, `experiments/` | Instrumentation, experiment orchestration, validation, and plotting. |
| `dataset/download.py`, `docs/` | Dataset downloader and technical documentation. |

## Getting started

### 1. Prerequisites

- Go **1.19 or later**; a recent toolchain is recommended.
- Python **3.9 or later** for experiment orchestration.
- Access to dependencies pinned in `go.mod` and `go.sum` for the first build.
- Memory and disk space for per-node processes, databases, and logs.
- Optional plotting dependencies in `requirements.txt`.

Run commands from the repository root. Use `python3` instead of `python` where appropriate.

### 2. Clone the repository

```bash
git clone https://github.com/wuhaodoc/BLBChain.git
cd BLBChain
go mod download
```

### 3. Prepare the dataset

Transaction data are not bundled. The optional `dataset/download.py` helper downloads XBlock-ETH data and requires `requests`:

```bash
python -m pip install requests
cd dataset
python download.py
cd ..
```

Choose `2` for Block Transaction data. Extract the downloaded archives and prepare the CSV schema below before using the controlled runner; downloading does not perform normalization. Files are downloaded into the current working directory. The helper is retained from the original source; remote availability has not been verified in this release.

The controlled runner accepts a CSV with a header and this column order:

```text
index,unused,sender,recipient,value
```

Addresses must contain 40 hexadecimal characters; an optional `0x` prefix is accepted. Values must be nonnegative integers. Place the prepared file at `dataset/transactions.csv`, or specify another path with `--dataset`.

The runner selects the first `--total` records and preserves arrival order. Record the source and preprocessing of the input trace when reporting results. Synthetic smoke-test data are only for functional checks.

### 4. Check the installation

```bash
go test ./params ./build ./core ./networks ./supervisor/committee ./supervisor/measure
python experiments/run.py --study smoke --schemes BLBChain-Capacity BLBChain --output runs/smoke
```

The runner builds the executable unless `--binary` is supplied, generates the smoke workload, and launches nodes and a supervisor. Use a new output directory for each execution; existing output directories are not overwritten.

### 5. Configure the experiment

The runner generates a separate configuration and address table for each trial. Set common parameters through its command-line options:

| Option | Default | Description |
| --- | --- | --- |
| `--shards` | `4` | Number of shards. |
| `--nodes` | `4` | Consensus nodes per shard. |
| `--block-size` | `2000` | Maximum transactions per block. |
| `--block-ms` | `3000` | Target block interval in milliseconds. |
| `--rate` | `3000` | Scheduled injection rate in transactions/s. |
| `--batch` | `300` | Transactions per injection batch. |
| `--total` | `60000` | Input transaction count. |
| `--bandwidth-mbps` | Study-dependent | Upload limit in decimal Mbps; 100 for point/migration studies, 5–10 for the bandwidth study. |
| `--accounts` | `1 2 3 4 5` | Requested migration-count limits. |
| `--repeats` | `1` | Repetitions per operating point. |
| `--timeout` | `240` | Trial timeout in seconds. |
| `--skip-stream-node` | `-1` | Replica omitted from stream delivery for recovery testing; `-1` disables this test. |
| `--output` | Required | New output directory. |

These are runner defaults, not a guarantee of reproducing every manuscript figure. Match the workload and settings of the experiment being reproduced. View all options with:

```bash
python experiments/run.py --help
```

## Schemes and entry points

| Scheme | Main source | Controlled runner |
| --- | --- | --- |
| BLBChain | `supervisor/committee/blbchain_allocation.go`, `consensus_shard/pbft_all/blbchain_stream.go` | Supported; migration enabled in the migration study. |
| BLBChain-Capacity | `consensus_shard/pbft_all/blbchain_stream.go` | Supported; migration disabled. |
| ContribChain | `supervisor/committee/contribchain.go`, `supervisor/committee/contribchain_allocation.go` | Supported. |
| LB-Chain | `supervisor/committee/lbchain.go`, `supervisor/committee/lbchain_allocation.go` | Supported. |
| Presto | `supervisor/committee/presto_relay.go` | Native mode only. |
| CLPA | `supervisor/committee/committee_clpa.go` | Native mode only. |
| Broker | `supervisor/committee/committee_broker.go` | Native mode only. |

ContribChain and LB-Chain use adapted allocation components; Presto uses the inherited post-commit relay path. These labels do not imply full reproduction of the original protocols. Community-based allocation belongs to ContribChain and is not a separate baseline. Details are in [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md).

## Run experiments

### Bandwidth and stream pre-distribution

Evaluate throughput and confirmation latency at 5–10 Mbps:

```bash
python experiments/run.py --study bandwidth --schemes BLBChain ContribChain LB-Chain --dataset dataset/transactions.csv --bandwidth-mbps 5 6 7 8 9 10 --output runs/bandwidth
```

This controlled network study disables migration for all schemes. BLBChain therefore exercises its capacity component; the study does not evaluate native periodic allocation behavior.

### Account migration

Evaluate performance with requested migration counts from one to five accounts:

```bash
python experiments/run.py --study migration --schemes BLBChain-Capacity BLBChain ContribChain LB-Chain --dataset dataset/transactions.csv --accounts 1 2 3 4 5 --bandwidth-mbps 100 --output runs/migration
```

Each migration-enabled trial uses a scheme-specific selector, a common observation trigger, and a shared state-transfer barrier. The trigger waits for at least two nonempty block observations per shard before attempting one migration. BLBChain-Capacity performs no migration and serves as a constant reference.

Requested and actual counts are recorded separately. Trials with fewer accepted moves than requested are rejected from the requested-count summary rather than relabeled.

### Single operating point

```bash
python experiments/run.py --study point --schemes BLBChain ContribChain LB-Chain --dataset dataset/transactions.csv --shards 4 --nodes 4 --block-size 2000 --block-ms 3000 --rate 3000 --bandwidth-mbps 100 --output runs/point
```

The point study also disables migration. Increase `--timeout` if needed; timeouts are not valid performance measurements.

## Experimental outputs

Results are written under `--output`:

| Output | Contents |
| --- | --- |
| `status.json` | Suite state: running, validating, complete, or partial. |
| `manifest.json` | Arguments and input/executable hashes. |
| `summary.csv` | Validated trial metrics. |
| `invalid_trials.csv` | Failures and validation/count mismatches. |
| `trials/` | Configurations, logs, account-state audits, and raw measurements. |

| Metric | Definition |
| --- | --- |
| Throughput (TPS) | Unique finalized transactions divided by time from the first scheduled arrival to the last final commit. |
| Confirmation latency (ms) | Scheduled arrival to final confirmation, including batching, queueing, and migration waiting. |
| Migration duration (ms) | Migration request to state-application acknowledgments from all replicas. |
| Actual migrated accounts | Accepted moves, recorded separately from the requested limit. |

All outgoing message classes share one upload limiter per process. Communication counters measure application bytes written to sockets, not TCP/IP wire traffic. Migration duration and confirmation latency are distinct metrics. See [docs/METRICS.md](docs/METRICS.md).

## Plot results

```bash
python -m pip install -r requirements.txt
python experiments/plot.py runs/bandwidth/summary.csv --x bandwidth_mbps --output runs/bandwidth/comparison
python experiments/plot.py runs/migration/summary.csv --x requested_accounts --output runs/migration/comparison
```

The plotter generates PDF and PNG figures. Missing or invalid observations are not interpolated into measured results.

## Native node deployment

The controlled runner manages startup automatically. For native deployment, choose a configuration from `configs/`, copy it to `paramsConfig.json`, and supply a matching `ipTable.json`. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

Build on Linux/macOS:

```bash
go build -o blbchain .
```

Build on Windows:

```powershell
go build -o blbchain.exe .
```

Start all nodes with their own shard/node IDs, followed by one supervisor. Example on Linux/macOS:

```bash
# One node; repeat for every node in every shard.
./blbchain -S 4 -N 4 -s 0 -n 0

# Supervisor; start after the shard nodes.
./blbchain -S 4 -N 4 -c
```

On Windows, replace `./blbchain` with `.\blbchain.exe`. The `-c` flag selects the **supervisor**, not a consensus node. Topology is supplied through `-S` and `-N`. Native modes use legacy input and measurement paths; the controlled runner provides normalized inputs and audited summaries.

## Documentation

- [Implementation scope](docs/IMPLEMENTATION.md)
- [Configuration reference](docs/CONFIGURATION.md)
- [Metric definitions](docs/METRICS.md)
- [Source code map](docs/CODE_MAP.md)

This source-only release excludes experiment results, datasets, databases, and executables. Baseline routing and allocation code changed during cleanup; earlier measurements should not be assumed to reproduce unchanged with this release.

## Citation and acknowledgments

Please cite the accompanying manuscript when using this work:

```bibtex
@unpublished{wu_blbchain,
  title  = {Beyond Stress-Balanced Sharding: A Cross-Layer Perspective on Throughput Scaling in Blockchain},
  author = {Wu, Hao and Jin, Rui and Feng, Yebo and Liu, Yu and Zhu, Konglin and Zhang, Lin},
  note   = {Research manuscript}
}
```

BLBChain builds on BlockEmulator by HuangLab-SYSU. Please also acknowledge the underlying emulator, baseline papers, and dataset when using their components. We welcome feedback and contributions through the repository.

## License

See [LICENSE](LICENSE) for the MIT License and preserved upstream copyright notice. Dependencies and datasets retain their respective licensing terms.
