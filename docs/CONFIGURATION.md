# Configuration reference

`Scheme` overrides the legacy `ConsensusMethod`. IDs: CLPA-Broker=0, CLPA=1, Broker=2, Presto=3, ContribChain=4, LB-Chain=5. BLBChain and its capacity variant select the instrumented compact-proposal path over method 4. Scheme alone does not imply that native ContribChain and BLBChain execute the same allocation algorithm.

| Key | Unit / meaning |
|---|---|
| BlockSize | Maximum transactions per block |
| Block_Interval | Target interval in milliseconds |
| InjectSpeed | Scheduled transactions per second |
| TxBatchSize | Transactions per injection batch |
| TotalDataSize | Number of input transactions |
| Bandwidth | Bytes per second; 100 Mbps = 12,500,000 B/s |
| Delay / JitterRange | Milliseconds |
| ReconfigTimeGap | Seconds; used by periodic CLPA paths, not by the controlled migration trigger |
| PbftViewChangeTimeOut | Milliseconds |
| DatasetFile | CSV path relative to process working directory, or an absolute path |
| ExpDataRootDir | Per-run output directory |
| Overhead.Enabled | Controlled experiment path |
| Overhead.Lightweight | Stream/compact proposals rather than full proposals |
| Overhead.Migration | Enable one controlled account migration |
| Overhead.MigrationPolicy | stress, ContribChain, LB-Chain, timed |
| Overhead.MigrationAccounts | Upper bound on accepted moves |
| Overhead.MigrationAfterSeconds | Legacy timed policy only |
| Overhead.TimeoutSeconds | Experiment deadline in seconds |
| Overhead.StreamSkipNode | Fault injection replica ID; -1 disables |

The runner passes shard and node counts through `-S` and `-N`, creates a fresh loopback IP table, and saves every effective parameter. Native configurations for Presto, CLPA and Broker are provided for advanced use, but the audited runner currently supports the four schemes listed in its --help.

Low-level native launch: copy the chosen JSON to paramsConfig.json, supply an ipTable.json, build with `go build -o bin/blbchain .`, then start each node with `-S 4 -N 4 -s <shard> -n <node>` and one supervisor with `-S 4 -N 4 -c`. Native CSV parsing is legacy code; use the controlled runner for the normalized five-column format and audited metrics.
