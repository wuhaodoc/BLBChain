# Measurement and validation

- TPS: unique final commits / (last final commit - first scheduled arrival), in seconds.
- Confirmation latency: final commit timestamp minus scheduled arrival timestamp. Includes batching, queuing, migration waits and drain.
- Migration duration: migration request to all replica completion acknowledgments; excludes candidate-selection computation. Baseline selection computation is logged separately in baseline_decisions.
- Traffic: logged serialized message bytes by category (stream, recovery, consensus, migration, cross_shard, injection, control, other). These are application-layer bytes, not Ethernet/IP packet overhead.
- Bandwidth limiter: use bytes/s internally and Mbps at the command line. All competing categories use the existing network path.

Validation checks expected transaction identities and unique final commits, replays balances from the input, checks migrated ownership and replica agreement, and rejects failed writes or incomplete migrations. Requested migration count must equal actual count for a requested-count plot.

The plotter averages repeated measurements if supplied, leaves missing points as gaps, and never creates measurements by interpolation. A constant BLBChain-Capacity reference is reused across migration counts. Values supplied outside this runner require separate provenance and must not be marked as independently audited by it.

All processes use a common host clock. Cross-host execution requires clock synchronization and a revised measurement design. Single-run results are exploratory; no confidence intervals are inferred from one run.
