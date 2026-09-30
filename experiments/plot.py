"""Plot validated summaries without interpolating failed or missing runs."""
import argparse,csv
from collections import defaultdict
from pathlib import Path
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('summary',type=Path)
p.add_argument('--x',choices=['bandwidth_mbps','requested_accounts'],required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
with a.summary.open(newline='') as f: data=list(csv.DictReader(f))
if not data: p.error('summary has no valid measurements')
fixed='requested_accounts' if a.x=='bandwidth_mbps' else 'bandwidth_mbps'
# A capacity-only reference has zero migrations and can be repeated horizontally.
values={r[fixed] for r in data if r['scheme']!='BLBChain-Capacity'}
if len(values)>1: p.error('filter the summary to one value of '+fixed+' before plotting')
x=sorted({float(r[a.x]) for r in data if a.x!='requested_accounts' or float(r[a.x])>0})
if not x: p.error('no points for selected x axis')
styles={'LB-Chain':('#1f77b4','o','--'),'ContribChain':('#ff7f0e','s','--'),'BLBChain-Capacity':('#87CEFA','^',':'),'BLBChain':('#d62728','d','-')}
plt.rcParams.update({'font.family':'serif','font.size':10,'pdf.fonttype':42})
fig,axs=plt.subplots(1,2,figsize=(9,3.6))
for scheme,(color,marker,ls) in styles.items():
 selected=[r for r in data if r['scheme']==scheme]
 if not selected:continue
 for ax,metric,scale,label in [(axs[0],'tps',1,'Throughput (TPS)'),(axs[1],'latency_ms',1000,'Transaction confirmation latency (s)')]:
  groups=defaultdict(list)
  for r in selected:groups[float(r[a.x])].append(float(r[metric])/scale)
  reference=scheme=='BLBChain-Capacity' and a.x=='requested_accounts'
  y=[statistics.mean(groups[0 if reference else k]) if groups[0 if reference else k] else float('nan') for k in x]
  ax.plot(x,y,color=color,marker=marker,linestyle=ls,label=scheme)
  ax.set_ylabel(label);ax.set_xlabel('Bandwidth (Mbps)' if a.x=='bandwidth_mbps' else 'Number of migrated accounts');ax.set_xticks(x);ax.grid(linestyle=':',alpha=.5)
for ax in axs:ax.legend(fontsize=8)
fig.tight_layout();a.output.parent.mkdir(parents=True,exist_ok=True)
for suffix in ['.pdf','.png']:fig.savefig(a.output.with_suffix(suffix),dpi=220,bbox_inches='tight')
