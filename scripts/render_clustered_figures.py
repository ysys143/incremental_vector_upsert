"""Render report figures from published aggregates; no DB execution.
Run: uv run --no-project --with matplotlib python scripts/render_clustered_figures.py
"""
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e'
OUT=ROOT/'docs/real-vectors/clustered/figures'
OUT.mkdir(exist_ok=True)
def read(name):return json.loads((SOURCE/(name+'.json')).read_text())
E,C,F=read('endpoints'),read('contrasts'),read('ef-curves')
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none','figure.facecolor':'white','savefig.facecolor':'white'})
colors={'postgres':'#2166ac','qdrant':'#b35806'}
groups=[(d,b) for d in ['msmarco','wikipedia'] for b in ['postgres','qdrant']]
def title(d,b):return ('MS MARCO' if d=='msmarco' else 'Wikipedia–Cohere')+' / '+('pgvector' if b=='postgres' else 'Qdrant')
def save(fig,name):
 for ext in ['svg','png']:fig.savefig(OUT/(name+'.'+ext),dpi=180,bbox_inches='tight')
 plt.close(fig)
# Design schematic: colors identify illustrative clusters, not measured sizes.
fig,ax=plt.subplots(figsize=(11,3.8));ax.set_xlim(0,12);ax.set_ylim(-.5,4.8);ax.axis('off')
palette=['#4477aa','#ee6677','#228833','#ccbb44','#aa3377']
for i,(name,mixed,waves) in enumerate([('M1',True,1),('M5',True,5),('C1',False,1),('C5',False,5)]):
 y=3.5-i
 ax.text(.1,y+.22,name,weight='bold',fontsize=12)
 ax.text(1,y+.22,'Same initial\n10,000 vectors',va='center',fontsize=9)
 for j in range(25):
  c=palette[(j*3+j//5)%5] if mixed else palette[j//5]
  ax.add_patch(Rectangle((3+j*.23,y),.22,.45,facecolor=c,edgecolor='white',lw=.4))
 for j in range(waves):
  width=25*.23/waves;ax.plot([3+j*width,3+(j+1)*width],[y-.12,y-.12],color='#333333',lw=2)
  ax.plot([3+j*width,3+j*width],[y-.18,y-.06],color='#333333',lw=1)
 ax.text(9.1,y+.22,f'{waves} logical wave'+('s' if waves>1 else ''),va='center')
ax.text(3,4.4,'Incoming 10,000 vectors: mixed or clustered order',weight='bold')
ax.text(3,-.4,'All paths end at the same 20,000 vectors; compare with 3 fresh builds.\nSchematic only: actual cluster sizes differ. Transport chunks are at most 1,000 vectors.',fontsize=9)
save(fig,'insertion-design')
# C5 vs fresh: per-seed descriptive intervals.
fig,axes=plt.subplots(2,2,figsize=(10,6),sharex=True,sharey=True)
for ax,(d,b) in zip(axes.flat,groups):
 rows=sorted([r for r in E if r['dataset']==d and r['db']==b and r['path']=='C5'],key=lambda r:r['seed'])
 for i,r in enumerate(rows):
  g=r['history_gap100'];m=g['mean_pp'];ax.errorbar(m,i,xerr=[[m-g['lower_pp']],[g['upper_pp']-m]],fmt='o',color=colors[b],capsize=4)
 ax.axvline(0,color='#777777',ls='--',lw=1);ax.set_yticks(range(3),[f'Seed {r["seed"]}' for r in rows]);ax.set_title(title(d,b));ax.grid(axis='x',alpha=.2);ax.set_xlim(-.20,.20);ax.set_ylim(2.5,-.5)
fig.supxlabel('C5 − mean fresh Recall@100 (percentage points)')
fig.suptitle('Final index history gap: descriptive 95% paired-query intervals',fontsize=13)
fig.tight_layout(rect=(0,.03,1,.94));save(fig,'final-history-gap')
# Primary contrasts: all 24 estimates, no pooled seed averages.
fig,axes=plt.subplots(1,2,figsize=(12,7),sharex=True,sharey=True)
for ax,name in zip(axes,['C5-M5','interaction']):
 labels=[]
 for i,(d,b,seed) in enumerate((d,b,seed) for d,b in groups for seed in [11,29,47]):
  r=next(r for r in C if (r['dataset'],r['db'],r['seed'],r['contrast'])==(d,b,seed,name));g=r['familywise_ci'];m=g['mean_pp']
  ax.errorbar(m,i,xerr=[[m-g['lower_pp']],[g['upper_pp']-m]],fmt='o',color=colors[b],capsize=3)
  labels.append(title(d,b)+f' / {seed}')
 ax.axvline(0,color='#777777',ls='--',lw=1);ax.set_title('Cluster order: C5 − M5' if name=='C5-M5' else 'Interaction: (C5 − C1) − (M5 − M1)');ax.set_yticks(range(12),labels);ax.grid(axis='x',alpha=.2);ax.set_xlim(-.24,.24)
axes[0].invert_yaxis();fig.supxlabel('Recall@100 difference (percentage points)')
fig.suptitle('24 primary comparisons: paired-query intervals, α = 0.05 / 24',fontsize=13)
fig.tight_layout(rect=(0,.03,1,.94));save(fig,'primary-contrasts')
# Individual seed trajectories across tested ef points only.
fig,axes=plt.subplots(2,2,figsize=(10,6),sharey=True)
operating={('msmarco','postgres'):1000,('msmarco','qdrant'):256,('wikipedia','postgres'):512,('wikipedia','qdrant'):256}
for ax,(d,b) in zip(axes.flat,groups):
 for i,seed in enumerate([11,29,47]):
  id=f'{d}-{b}-s{seed}-C5';r=next(r for r in E if r['id']==id)
  z=sorted([r for r in F if r['id']==id],key=lambda r:r['ef']);xs=[r['ef'] for r in z]+[operating[d,b]]
  ax.plot(xs,[r['recall100']*100 for r in z]+[r['recall100']*100],marker=['o','s','^'][i],alpha=.7,label=f'Seed {seed}',color=colors[b],ls=['-','--',':'][i])
 ax.set_title(title(d,b));ax.set_xscale('log',base=2);ax.set_xticks([100,128,operating[d,b]],[str(x) for x in [100,128,operating[d,b]]]);ax.set_ylim(80,100.5);ax.grid(alpha=.2)
axes[0,0].legend(fontsize=8);fig.supxlabel('ef (last point: frozen operating setting; lines guide the eye)');fig.supylabel('C5 Recall@100 (%)')
fig.suptitle('Search effort sensitivity on the same final C5 indexes',fontsize=13)
fig.tight_layout(rect=(.02,.03,1,.94));save(fig,'ef-sensitivity')
print('Rendered four figures in SVG and PNG')
