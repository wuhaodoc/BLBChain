"""Validate complete trials and export auditable summary/timeseries CSV files."""
import csv
import json
import math
from pathlib import Path
import statistics
import sys

CATEGORIES = ["stream", "consensus", "recovery", "cross_shard", "migration", "injection", "control", "other"]


def rows(path):
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def percentile(values, q):
    if not values:
        return ""
    v = sorted(values)
    return v[max(0, math.ceil(len(v)*q)-1)]


def write_csv(path, records, fields=None):
    if not records and fields is None:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields or list(records[0]))
        w.writeheader()
        w.writerows(records)


def summarize(root):
    root = Path(root)
    results, invalid = [], []
    for trial in sorted(root.iterdir()):
        status_path = trial / "status.json"
        if not status_path.exists():
            continue
        status = json.loads(status_path.read_text())
        if status["status"] != "complete":
            invalid.append(dict(trial=trial.name,error=status.get("error","failed")))
            continue
        try:
            cfg = json.loads((trial / "paramsConfig.json").read_text())
            out = trial / "results" / "overhead"
            events = rows(out / "supervisor_events.csv")
            event = {r["event"]: r for r in events}
            start = int(event["workload_start"]["unix_ns"])
            finished = int(event["workload_complete"]["unix_ns"])
            tx = rows(out / "supervisor_transactions.csv")
            expected = cfg["TotalDataSize"]
            assert len(tx) == expected and len({t["hash"] for t in tx}) == expected, "missing or duplicate final transactions"
            assert {int(t["nonce"]) for t in tx} == set(range(expected)), "transaction identity coverage mismatch"
            # Independent balance oracle, including migrated ownership. Do not
            # accept throughput figures when state replay or replica agreement fails.
            balances, owner = {}, {}
            ips=json.loads((trial / "ipTable.json").read_text())
            shards=len(ips)-1
            nodes=len(ips["0"])
            audit=list(out.glob("s*_account_audit.csv"))
            audit_rows=[r for path in audit for r in rows(path)]
            assert audit_rows,"missing final state audit"
            initial=int(audit_rows[0]["initial_balance"])
            for r in rows(Path(cfg["DatasetFile"])):
                a,b=r["sender"],r["recipient"]
                balances.setdefault(a,initial);balances.setdefault(b,initial)
                balances[a]-=int(r["value"]);balances[b]+=int(r["value"])
            owner={a:int(a[-8:],16)%shards for a in balances}
            for r in rows(out / "supervisor_migration_plan.csv"):
                owner[r["account"]]=int(r["destination"])
            seen=set()
            for r in audit_rows:
                key=(r["account"],int(r["node"]))
                assert key not in seen,"duplicate owned account state"
                seen.add(key)
                assert int(r["shard"])==owner[r["account"]],"incorrect migrated ownership"
                assert int(r["balance"])==balances[r["account"]],"final balance differs from input replay"
            assert len(seen)==len(balances)*nodes,"incomplete replica state audit"
            end = max(int(t["commit_ns"]) for t in tx)
            assert start < end <= finished, "invalid measurement interval"
            duration = (end-start)/1e9
            latencies = [float(t["latency_ms"]) for t in tx]
            assert all(math.isfinite(x) and x>=0 for x in latencies), "invalid latency"
            counts = dict.fromkeys(CATEGORIES,0)
            # All traffic from the first arrival through the last final commit.
            # Also retain lifecycle totals to expose startup/drain costs.
            lifecycle = dict.fromkeys(CATEGORIES,0)
            failures = 0
            for path in out.glob("*_network.csv"):
                for r in rows(path):
                    cat=r["category"]
                    lifecycle[cat] += int(r["bytes"])
                    if start <= int(r["unix_ns"]) <= end:
                        counts[cat] += int(r["bytes"])
                        failures += r["success"] != "true"
            assert failures == 0, "failed network writes during measurement"
            pause_ms=[]
            for path in out.glob("s*_events.csv"):
                ev=rows(path)
                for r in ev:
                    if r["event"] != "migration_pause_start":
                        continue
                    done = [d for d in ev if d["event"]=="migration_applied" and d["epoch"]==r["epoch"]]
                    assert len(done)==1,"missing migration application event"
                    pause_ms.append((int(done[0]["unix_ns"])-int(r["unix_ns"]))/1e6)
            migration_ms, migrated = "",0
            if cfg["Overhead"]["Migration"]:
                migration_ms=(int(event["migration_complete"]["unix_ns"])-int(event["migration_requested"]["unix_ns"]))/1e6
                migrated=int(event["migration_complete"]["accounts"])
                assert migrated>0 and pause_ms, "migration did not run"
            result=dict(trial=trial.name,variant=status["variant"],bandwidth_mbps=status["bandwidth_mbps"],repeat=status["repeat"],confirmed=expected,duration_s=duration,tps=expected/duration,mean_latency_ms=statistics.mean(latencies),p95_latency_ms=percentile(latencies,.95),migration_accounts=migrated,migration_ms=migration_ms,shard_pause_p95_ms=percentile(pause_ms,.95))
            for cat in CATEGORIES:
                result[cat+"_bytes"]=counts[cat]
                result[cat+"_bytes_per_tx"]=counts[cat]/expected
                result[cat+"_lifecycle_bytes"]=lifecycle[cat]
            result["total_bytes_per_tx"]=sum(counts.values())/expected
            result["protocol_bytes_per_tx"]=sum(counts[c] for c in ["stream","consensus","recovery","cross_shard","migration"])/expected
            heaps=[]
            for path in out.glob("s*_resources.csv"):
                samples=[int(r["go_heap_bytes"]) for r in rows(path) if start<=int(r["unix_ns"])<=end]
                if samples: heaps.append(max(samples))
            result["max_node_go_heap_bytes"]=max(heaps) if heaps else ""
            result["state_audit"]="passed"
            series=[]
            for i in range(math.ceil(duration)):
                left=start+i*1_000_000_000
                right=min(start+(i+1)*1_000_000_000,end)
                bucket=[t for t in tx if left<=int(t["commit_ns"]) and (int(t["commit_ns"])<right or right==end and int(t["commit_ns"])==end)]
                arrived=sum(int(t["arrival_ns"])<=right for t in tx)
                committed=sum(int(t["commit_ns"])<=right for t in tx)
                active=False
                if migrated:
                    active=int(event["migration_requested"]["unix_ns"])<right and int(event["migration_complete"]["unix_ns"])>left
                series.append(dict(second=i,window_s=(right-left)/1e9,confirmed=len(bucket),tps=len(bucket)/((right-left)/1e9),p95_latency_ms=percentile([float(t["latency_ms"]) for t in bucket],.95),pending=arrived-committed,migration_active=int(active)))
            write_csv(trial / "timeseries.csv",series)
            results.append(result)
        except Exception as exc:
            invalid.append(dict(trial=trial.name,error=str(exc)))
    write_csv(root / "summary.csv",results,list(results[0]) if results else ["trial","variant","bandwidth_mbps","repeat"])
    write_csv(root / "invalid_trials.csv",invalid,["trial","error"])
    groups={}
    for r in results:
        groups.setdefault((r["variant"],r["bandwidth_mbps"]),[]).append(r)
    aggregate=[]
    for (variant,bw), group in sorted(groups.items()):
        a=dict(variant=variant,bandwidth_mbps=bw,n=len(group))
        for metric in ["tps","p95_latency_ms","protocol_bytes_per_tx","migration_ms","shard_pause_p95_ms"]:
            vals=[float(r[metric]) for r in group if r[metric]!=""]
            a[metric+"_mean"]=statistics.mean(vals) if vals else ""
            a[metric+"_std"]=statistics.stdev(vals) if len(vals)>1 else ""
        aggregate.append(a)
    write_csv(root / "aggregate.csv",aggregate,list(aggregate[0]) if aggregate else ["variant","bandwidth_mbps","n"])
    print(f"Validated {len(results)} trials; invalid/failed {len(invalid)}")
    return not invalid and bool(results)


if __name__ == "__main__":
    raise SystemExit(0 if summarize(Path(sys.argv[1])) else 1)
