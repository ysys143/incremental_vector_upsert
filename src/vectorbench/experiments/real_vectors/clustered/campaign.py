"""Freeze all approved clustered nodes before opening any measured database."""
import argparse
from pathlib import Path
import sys
import time
import json
import random
from vectorbench.evidence.contracts import Config,GIB,load,save,sha
from vectorbench.evidence.source_manifest import freeze_sources


def catalogue(matrix):
    nodes={}
    def add(role,identity,resource,action,deps=(),**fields):
        key=role+':'+identity
        if key in nodes:raise AssertionError('duplicate node '+key)
        nodes[key]={'id':identity,'role':role,'resource':resource,'action':action,'phase':'CAMPAIGN',
                    'deps':list(dict.fromkeys(deps)),'reserve_bytes':2*GIB if resource=='db' else 256*1024**2,**fields}
        return key
    inputs=[add('inputs',d,'cpu','prepare',dataset=d) for d in matrix['datasets']]
    prepared=add('prepared','all','cpu','barrier',inputs,singleton=True)
    tuning=[]
    for d in matrix['datasets']:
        for db in matrix['databases']:
            groups=[]
            builds=((16,100),(32,200)) if db=='postgres' else ((48,300),(64,400))
            for b in builds:
                groups.append(add('calibration',f'{d}-{db}-m{b[0]}','db','candidate',[prepared,'inputs:'+d],dataset=d,db=db,build=list(b)))
            tuning.append(add('tuning',d+'-'+db,'cpu','select',groups,dataset=d,db=db))
    tuned=add('tuned','all','cpu','barrier',tuning,singleton=True)
    smoke=[];guards=[]
    for d in matrix['datasets']:
        for db in matrix['databases']:
            common=[tuned,'inputs:'+d,'tuning:'+d+'-'+db]
            snap=add('smoke_initial',d+'-'+db,'db','initial',common,dataset=d,db=db,seed=7)
            for path in ('M1','C5'):
                smoke.append(add('smoke_history',d+'-'+db+'-'+path,'db','history',common+[snap],dataset=d,db=db,seed=7,path=path,snapshot_node=snap,extra=False))
            smoke.append(add('smoke_fresh',d+'-'+db,'db','fresh',common,dataset=d,db=db,seed=7,state='final',repeat=0,extra=False))
            if d=='msmarco':
                guards.append(add('path_guard',db,'db','path_guard',common,dataset=d,db=db,seed=7))
    gate=add('cost_gate','all','cpu','cost_gate',smoke+guards+['smoke_initial:'+d+'-'+db for d in matrix['datasets'] for db in matrix['databases']]+inputs+tuning,singleton=True)
    snapshots={}
    for d in matrix['datasets']:
        for db in matrix['databases']:
            for seed in matrix['seeds']:
                stem=f'{d}-{db}-s{seed}'
                snapshots[stem]=add('initial',stem,'db','initial',[gate,'inputs:'+d,'tuning:'+d+'-'+db],dataset=d,db=db,seed=seed)
    initial_gate=add('initialized','all','cpu','barrier',snapshots.values(),singleton=True)
    histories=[]
    for stem,snap in snapshots.items():
        selected=[h for h in matrix['histories'] if h['initial_snapshot_group']==stem]
        selected=sorted(selected,key=lambda h:h['name']);random.Random(81000+selected[0]['seed']).shuffle(selected)
        prior=initial_gate
        for slot,h in enumerate(selected):
            identity=h['id'];d=h['dataset'];db=h['db']
            prior=add('history_slot'+str(slot),identity,'db','history',[gate,prior,snap,'inputs:'+d,'tuning:'+d+'-'+db],
                      dataset=d,db=db,seed=h['seed'],path=h['name'],snapshot_node=snap,extra=True)
            histories.append(prior)
    histories_gate=add('histories_done','all','cpu','barrier',histories,singleton=True)
    fresh=[]
    for f in matrix['fresh_builds']:
        d,db,seed=f['group'].split('-');seed=int(seed[1:])
        fresh.append(add('fresh',f['id'],'db','fresh',[gate,histories_gate,'inputs:'+d,'tuning:'+d+'-'+db],
                         dataset=d,db=db,seed=seed,state=f['state'],repeat=f['repeat'],extra=True))
    add('report','all','cpu','report',histories+fresh+inputs+tuning+[gate],singleton=True)
    if len(histories)!=48 or len(fresh)!=120:raise AssertionError('approved scope changed')
    return nodes


def prepare(root,source_manifest):
    root=Path(root).resolve();auth=load(root/'authorization.json');matrix=load(root/'approved-matrix.json')
    if sha(root/'approved-plan.md')!=auth['approved_plan_sha256'] or sha(root/'approved-matrix.json')!=auth['approved_matrix_sha256']:
        raise AssertionError('approved documents changed')
    if time.time()>=auth['deadline'] or (root/'manifest.json').exists():raise AssertionError('expired/already frozen campaign')
    old=load(source_manifest);refs=old['source_refs']
    for d,ref in refs.items():
        if ref['dataset']!=d or ref['data_kind']!='real-precomputed' or ref['metric']!='ip':raise AssertionError('wrong source')
        for key in ('base','query'):
            if sha(ref[key+'_path'])!=ref[key+'_sha256']:raise AssertionError('source vector bytes changed')
        receipt=Path(ref['base_path']).parent/'receipt.json'
        if sha(receipt)!=ref['receipt_sha256'] or load(receipt)['status']!='PASS':raise AssertionError('source producer receipt differs')
    prior={d:[{'path':str(p.resolve()),'sha256':sha(p)} for p in sorted(Path(source_manifest).parent.glob('cells/calibration_group__'+d+'-*/*/group.json')) if load(p).get('state') in ('initial','growth')] for d in matrix['datasets']}
    if any(not v for v in prior.values()):raise AssertionError('prior calibration sensitivity inputs missing')
    manifest={'prior_calibration_refs':prior,'mode':'real-clustered-insertion','config':Config(stage_seconds=43200).validate(),
              'nodes':catalogue(matrix),'sources':freeze_sources(root),'source_refs':refs,
              'source_manifest':{'path':str(Path(source_manifest).resolve()),'sha256':sha(source_manifest)},
              'matrix':matrix,'expected':matrix['expected'],'space_baseline_free':auth['baseline_free_bytes'],
              'measurement_python':'/opt/homebrew/bin/python3','orchestration_python':str(Path(sys.executable).absolute()),
              'authorization':{'path':str(root/'authorization.json'),'sha256':sha(root/'authorization.json')},
              'execution_window_end':auth['deadline'],'deadlines':{'CAMPAIGN':auth['deadline']-3600},'task_seconds':43200,
              'reason':'paired clustered insertion; all old result roots read-only'}
    save(root/'manifest.json',manifest);return {'root':str(root),'nodes':len(manifest['nodes']),'expected':manifest['expected']}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--source-manifest',type=Path,default=Path('real-results/baseline-setup-20261026-02/manifest.json'))
    p.add_argument('--catalogue-only',action='store_true');a=p.parse_args()
    if a.catalogue_only:
        nodes=catalogue(load(a.root/'approved-matrix.json'));print(json.dumps({'nodes':len(nodes),'roles':{r:sum(v['role']==r for v in nodes.values()) for r in sorted({v['role'] for v in nodes.values()})}}))
    else:print(json.dumps(prepare(a.root,a.source_manifest)))
