"""Run named schemes with scheduled arrivals, state audits and explicit units.

Each trial has its own configuration and logs. Existing output directories are
never overwritten. No measured points are interpolated or silently replaced.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from runtime import run_trial, prepare_dataset, synthetic
from summarize import summarize, rows, write_csv

ROOT = Path(__file__).resolve().parents[1]
SCHEMES = ['BLBChain', 'BLBChain-Capacity', 'ContribChain', 'LB-Chain']

def save(path, obj):
    """Replace a small status document atomically for readers during a run."""
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(obj, indent=2), encoding='utf-8')
    temporary.replace(path)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--study', choices=['point', 'bandwidth', 'migration', 'smoke'], default='point')
    parser.add_argument('--schemes', nargs='+', choices=SCHEMES, default=['BLBChain'])
    parser.add_argument('--dataset', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--total', type=int, default=60000)
    parser.add_argument('--rate', type=int, default=3000)
    parser.add_argument('--batch', type=int, default=300)
    parser.add_argument('--shards', type=int, default=4)
    parser.add_argument('--nodes', type=int, default=4)
    parser.add_argument('--block-size', type=int, default=2000)
    parser.add_argument('--block-ms', type=int, default=3000)
    parser.add_argument('--bandwidth-mbps', nargs='+', type=float, default=None)
    parser.add_argument('--accounts', nargs='+', type=int, default=[1,2,3,4,5])
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--timeout', type=int, default=240)
    parser.add_argument('--skip-stream-node', type=int, default=-1)
    a = parser.parse_args()
    if min(a.total,a.rate,a.batch,a.shards,a.nodes,a.block_size,a.block_ms,a.repeats,a.timeout,*a.accounts)<=0:
        parser.error('counts, durations and rates must be positive')
    bandwidths = a.bandwidth_mbps or ([5,6,7,8,9,10] if a.study=='bandwidth' else [100])
    if any(b<=0 for b in bandwidths): parser.error('bandwidth must be positive')
    if a.study!='smoke' and not a.dataset: parser.error('--dataset is required except for smoke tests')
    output = a.output.resolve()
    if output.exists(): parser.error('output already exists; choose a new directory')
    binary = a.binary.resolve() if a.binary else ROOT/'bin'/('blbchain.exe' if os.name=='nt' else 'blbchain')
    if not a.binary:
        binary.parent.mkdir(exist_ok=True)
        subprocess.run(['go','build','-o',str(binary),'.'],cwd=ROOT,check=True)
    if not binary.is_file(): parser.error('binary not found')
    output.mkdir(parents=True)
    if a.study=='smoke':
        a.total,a.rate,a.batch,a.shards,a.block_size,a.block_ms=1200,100,20,2,100,500
        synthetic(output/'input.csv',a.total,a.shards,12345)
    else: prepare_dataset(a.dataset.resolve(),output/'input.csv',a.total,2,3,4)
    # Clear legacy overrides so the saved JSON is authoritative.
    for key in ['BLB_STRESS_MIGRATION','BLB_MIGRATION_BASELINE','BLB_PARTITION_HISTORY','BLB_PARTITION_MAP']:
        os.environ.pop(key,None)
    raw=output/'trials';raw.mkdir()
    save(output/'manifest.json',{'study':a.study,'schemes':a.schemes,'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items()},'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'dataset_sha256':hashlib.sha256((output/'input.csv').read_bytes()).hexdigest(),'bandwidth_unit':'Mbps converted to bytes/s by 1e6/8','execution':'Controlled shared migration execution; named per-scheme allocation policy. See docs/IMPLEMENTATION.md.','data_kind':'synthetic smoke data' if a.study=='smoke' else 'user-supplied trace'})
    for scheme in a.schemes:
        counts=[0] if scheme=='BLBChain-Capacity' or a.study in ['point','bandwidth'] else ([1] if a.study=='smoke' else a.accounts)
        for bw in bandwidths:
            for count in counts:
                for repeat in range(1,a.repeats+1):
                    name=f'{scheme}_bw{bw:g}_m{count}_r{repeat}'
                    save(output/'status.json',{'state':'running','trial':name})
                    cfg=json.loads((ROOT/'configs'/f'{scheme}.json').read_text())
                    cfg.update(DatasetFile=(output/'input.csv').as_posix(),TotalDataSize=a.total,InjectSpeed=a.rate,TxBatchSize=a.batch,BlockSize=a.block_size,Block_Interval=a.block_ms,Bandwidth=int(bw*1000000/8))
                    cfg['Overhead'].update(Migration=count>0,MigrationAccounts=max(1,count),TimeoutSeconds=a.timeout,StreamSkipNode=a.skip_stream_node)
                    print('START',name,flush=True)
                    status=run_trial(binary,raw/name,cfg,a.shards,a.nodes,a.timeout)
                    status.update(variant=scheme,bandwidth_mbps=bw,repeat=repeat)
                    save(raw/name/'status.json',status)
                    print('END',name,status,flush=True)
    save(output/'status.json',{'state':'validating'})
    valid=summarize(raw)
    results=[];invalid=rows(raw/'invalid_trials.csv')
    for r in rows(raw/'summary.csv'):
        cfg=json.loads((raw/r['trial']/'paramsConfig.json').read_text())
        wanted=cfg['Overhead']['MigrationAccounts'] if cfg['Overhead']['Migration'] else 0
        actual=int(r['migration_accounts'])
        if wanted!=actual:
            invalid.append({'trial':r['trial'],'error':f'requested {wanted} migrations, observed {actual}'})
            continue
        results.append(dict(scheme=r['variant'],bandwidth_mbps=r['bandwidth_mbps'],requested_accounts=wanted,actual_accounts=actual,repeat=r['repeat'],tps=r['tps'],latency_ms=r['mean_latency_ms'],migration_time_ms=r['migration_ms'],state_audit=r['state_audit'],trial=r['trial']))
    write_csv(output/'summary.csv',results,['scheme','bandwidth_mbps','requested_accounts','actual_accounts','repeat','tps','latency_ms','migration_time_ms','state_audit','trial'])
    write_csv(output/'invalid_trials.csv',invalid,['trial','error'])
    state='complete' if valid and not invalid else 'partial'
    save(output/'status.json',{'state':state,'valid_trials':len(results),'invalid_trials':len(invalid)})
    print(state.upper(),output,flush=True)
    return 0 if state=='complete' else 1

if __name__=='__main__':
    raise SystemExit(main())
