"""Draw the follow-up figures from existing aggregates, without running a DB.

Run: uv run --no-project --with matplotlib python scripts/render_clustered_three_way_figures.py
"""
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'docs/real-vectors/clustered'
with (BASE/'three-way-comparison.csv').open(newline='') as f:
    rows=list(csv.DictReader(f))
assert len(rows)==12
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':9,'axes.spines.top':False,
                     'axes.spines.right':False,'svg.fonttype':'none','figure.facecolor':'white',
                     'savefig.facecolor':'white'})
groups=[('msmarco','postgres'),('msmarco','qdrant'),('wikipedia','postgres'),('wikipedia','qdrant')]
seeds=[11,29,47];paths=['M1','M5','C1','C5'];markers=['o','s','^'];colors={'M':'#2166ac','C':'#b35806'}
def title(d,b): return ('MS MARCO' if d=='msmarco' else 'Wikipedia–Cohere')+' / '+('pgvector' if b=='postgres' else 'Qdrant')
def save(fig,name):
    for ext in ('png','svg'):
        path=BASE/'figures'/f'{name}.{ext}'
        fig.savefig(path,dpi=180,bbox_inches='tight')
        if ext=='svg': path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines())+'\n')
    plt.close(fig)
def get(d,b,seed):return next(r for r in rows if (r['dataset'],r['db'],int(r['seed']))==(d,b,seed))
fig,axes=plt.subplots(2,2,figsize=(11,7.2),sharex=True,sharey=True)
for ax,(d,b) in zip(axes.flat,groups):
    for i,path in enumerate(paths):
        for j,seed in enumerate(seeds):
            r=get(d,b,seed);val=float(r[path+'_change_initial_200_pp']);y=i+(j-1)*.20
            ax.plot(val,y,marker=markers[j],color=colors[path[0]],ms=6,linestyle='none')
    ax.axvline(0,color='#666666',ls='--',lw=1);ax.set_title(title(d,b));ax.set_yticks(range(4),paths);ax.set_ylim(3.65,-.65);ax.set_xlim(-1,.1);ax.grid(axis='x',alpha=.2)
fig.legend([plt.Line2D([],[],color='#333',marker=m,linestyle='none') for m in markers],[f'Seed {s}' for s in seeds],loc='upper center',bbox_to_anchor=(.5,.947),ncol=3,frameon=False)
fig.suptitle('Before vs. after insertion: paired 200 queries',fontsize=14)
fig.supxlabel('Final − initial Recall@100 (percentage points; each state uses its own exact neighbors)')
fig.tight_layout(rect=(0,.045,1,.9));save(fig,'before-after-insertion')
fig,axes=plt.subplots(2,2,figsize=(11,8.5),sharex=True,sharey=True)
for ax,(d,b) in zip(axes.flat,groups):
    labels=[]
    for i,path in enumerate(paths):
        for j,seed in enumerate(seeds):
            r=get(d,b,seed);m=float(r[path+'_gap100_pp']);lo=float(r[path+'_gap100_ci95_low_pp']);hi=float(r[path+'_gap100_ci95_high_pp']);y=i*3+j
            ax.errorbar(m,y,xerr=[[m-lo],[hi-m]],fmt=markers[j],color=colors[path[0]],capsize=2.5,markersize=5)
            labels.append(f'{path} / {seed}')
    ax.axvline(0,color='#666666',ls='--',lw=1);ax.set_title(title(d,b));ax.set_yticks(range(12),labels);ax.set_ylim(11.7,-.7);ax.grid(axis='x',alpha=.2)
    for y in (2.5,5.5,8.5):ax.axhline(y,color='#ddd',lw=.7)
fig.suptitle('Incremental vs. fresh: all insertion paths',fontsize=14)
fig.supxlabel('Incremental − mean of three fresh builds, Recall@100 (percentage points)')
fig.tight_layout(rect=(0,.045,1,.95));save(fig,'three-way-history-gap')
print('PASS: two figures in PNG and SVG, 48 point estimates per figure')
