"""Compile exactly the approved inventory into native Dagster assets and gates."""
import argparse
from pathlib import Path
import sys
from vectorbench.evidence.contracts import Config, GIB, load, save, sha
from vectorbench.experiments.real_vectors.baseline.baseline_campaign import freeze_sources
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


def catalogue(matrix):
    nodes={}
    def add(role,identity,resource,action,deps=(),**fields):
        key=role+':'+identity
        if key in nodes: raise AssertionError('duplicate native node')
        nodes[key]={'id':identity,'role':role,'resource':resource,'action':action,'phase':'CAMPAIGN','deps':list(dict.fromkeys(deps)),
                    'reserve_bytes':2*GIB if resource=='db' else 128*1024**2,**fields}
        return key
    contexts=[add('study_context',d,'cpu','context',dataset=d) for d in ('msmarco','wikipedia')]
    inputs=add('prepared','all','cpu','barrier',contexts,singleton=True)
    tuning=[add('calibration',d+'-'+db,'db','calibration',[inputs,'study_context:'+d],dataset=d,db=db) for d in ('msmarco','wikipedia') for db in ('postgres','qdrant')]
    tune_gate=add('calibrated','all','cpu','barrier',tuning,singleton=True)
    filter_smokes=[add('filter_smoke',db,'db','filter_smoke',[tune_gate,'study_context:msmarco'],dataset='msmarco',db=db,seed=11,family='filter') for db in ('postgres','qdrant')]
    initial={}
    for d,seed in [('msmarco',11),('msmarco',29),('wikipedia',11)]:
        for db in ('postgres','qdrant'):
            identity=f'{d}-s{seed}-{db}'
            initial[identity]=add('initial',identity,'db','initial',[tune_gate,'study_context:'+d],dataset=d,db=db,seed=seed,family='baseline')
    # All snapshots are known before any histories, keeping CPU validation out of DB runs.
    initial_gate=add('initialized','all','cpu','barrier',initial.values(),singleton=True)
    smoke_histories=[]; history_keys=[]; smoke_fresh=[]; fresh_keys=[]
    for h in matrix['histories']:
        stem=f"{h['dataset']}-s{h['seed']}-{h['db']}"
        is_smoke=h.get('source_case')=='churn-long-uniform' and h['dataset']=='msmarco' and h['seed']==11
        initial_key=('filter_initial:' if h['family']=='filter' else 'initial:')+stem
        if h['family']=='filter':
            add('filter_initial',stem,'db','initial',['cost_gate:all','study_context:msmarco'],dataset='msmarco',db=h['db'],seed=11,family='filter')
        deps=['study_context:'+h['dataset'],initial_key,initial_gate if is_smoke else 'cost_gate:all']
        key=add('smoke_history' if is_smoke else 'history',h['id'],'db','history',deps,**h)
        history_keys.append(key)
        if is_smoke: smoke_histories.append(key)
    for g in matrix['fresh_groups']:
        if g['family']=='baseline':
            fields={k:v for k,v in g.items() if k!='id'}
            is_smoke=g['dataset']=='msmarco' and g['seed']==11 and g['state']['count'] in (5000,10000,20000)
            # Only the original initial (read-only), insert100 and delete50 terminal states are cost probes.
            target_cases={'read_only-long','insert-r100-w1','delete-r50-w1'}
            is_smoke=is_smoke and any(h.get('source_case') in target_cases and h['dataset']=='msmarco' and h['seed']==11 and h['db']==g['db'] and h['terminal_ledger']==g['terminal_ledger'] for h in matrix['histories'] if h['family']=='baseline')
        else:
            stem=g['id'].split('-s11-')[1]; db=stem.split('-')[0]
            fields={**g,'dataset':'msmarco','db':db,'seed':11}; fields.pop('id')
            fields['selection']='near' if '-near-' in g['id'] else 'far' if '-far-' in g['id'] else 'filter'
            fields['state_step']=0 if '-state0-' in g['id'] else 5
            is_smoke=False
        for repeat in range(3):
            probe=is_smoke and repeat==0
            key=add('smoke_fresh' if probe else 'fresh',g['id']+'-r'+str(repeat),'db','fresh',
                    ['study_context:'+fields['dataset'],*(smoke_histories if probe else ['cost_gate:all'])],
                    **fields,group_id=g['id'],repeat=repeat)
            fresh_keys.append(key)
            if probe: smoke_fresh.append(key)
    if len(smoke_fresh)!=6 or len(smoke_histories)!=2: raise AssertionError('smoke coverage differs')
    add('cost_gate','all','cpu','cost_gate',[*contexts,*tuning,*initial.values(),*filter_smokes,*smoke_histories,*smoke_fresh],singleton=True)
    # Ordering is enforced by dependencies, not the unspecified order of queued runs.
    phase_names=('short','long','geometry','filter')
    stages={phase:{'history':[],'fresh':[]} for phase in phase_names}
    renames={}
    for key,spec in list(nodes.items()):
        if spec['role'] not in {'history','fresh'}: continue
        if spec['family'] in {'geometry','filter'}: stage=spec['family']
        elif spec['action']=='history': stage='long' if spec.get('source_case')=='upsert-long-uniform' else 'short'
        else:
            matching=[h for h in matrix['histories'] if h['family']=='baseline' and h['dataset']==spec['dataset'] and h['seed']==spec['seed'] and h['db']==spec['db'] and h['terminal_ledger']==spec['terminal_ledger']]
            stage='long' if matching and all(h.get('source_case') in {'churn-long-uniform','upsert-long-uniform'} for h in matching) else 'short'
        new_role=spec['role']+'_'+stage;new_key=new_role+':'+spec['id']
        spec['role']=new_role; nodes[new_key]=nodes.pop(key);renames[key]=new_key
        stages[stage][spec['action']].append(new_key)
    history_keys=[renames.get(k,k) for k in history_keys];fresh_keys=[renames.get(k,k) for k in fresh_keys]
    previous_gate='cost_gate:all'
    for stage in phase_names:
        for key in stages[stage]['history']:
            nodes[key]['deps'].append(previous_gate)
        history_gate=add('history_done_'+stage,'all','cpu','barrier',[previous_gate,*stages[stage]['history']],singleton=True)
        for key in stages[stage]['fresh']:
            nodes[key]['deps'].append(history_gate)
        previous_gate=add('phase_done_'+stage,'all','cpu','barrier',[history_gate,*stages[stage]['fresh']],singleton=True)
    for spec in nodes.values():
        if spec['role']=='filter_initial': spec['deps'].append('phase_done_geometry:all')
    add('report','all','cpu','report',[*contexts,*history_keys,*fresh_keys,previous_gate,'cost_gate:all'],singleton=True)
    if len(history_keys)!=66 or len(fresh_keys)!=138: raise AssertionError('approved inventory changed')
    return nodes


def prepare(root,setup,predecessor=None):
    root=root.resolve(); auth_path=root/'authorization.json'; auth=load(auth_path)
    matrix=load(root/'approved-matrix.json')
    if sha(root/'approved-matrix.json')!=auth['approved_matrix_sha256'] or sha(root/'approved-plan.md')!=auth['approved_plan_sha256']:
        raise AssertionError('approved plan changed')
    if (root/'manifest.json').exists(): raise FileExistsError('cannot re-freeze an execution')
    original=load(setup/'manifest.json'); config=Config(stage_seconds=43200).validate()
    manifest={'mode':'real-twelve-hour','config':config,'matrix':matrix,'nodes':catalogue(matrix),
       'authorization':{'path':str(auth_path),'sha256':sha(auth_path)},'execution_window_end':auth['deadline'],'task_seconds':43200,
       'deadlines':{'CAMPAIGN':auth['deadline']},'setup_root':str(setup.resolve()),
       'space_baseline_free':original['space_baseline_free'],
       'measurement_python':'/opt/homebrew/bin/python3','orchestration_python':sys.executable}
    if predecessor is not None:
        from vectorbench.orchestration.engine import native_receipt
        from vectorbench.evidence.registry import Registry
        import dagster as dg
        prior=load(predecessor/'controller.json')
        state=Registry(predecessor/'admission.sqlite').snapshot()
        if not prior['finished'] or state['leases'] or any(a['ended'] is None for a in state['attempts']):
            raise AssertionError('predecessor not reconciled')
        with dg.DagsterInstance.from_config(str(predecessor.resolve()/'dagster-home')) as instance:
            for key,spec in manifest['nodes'].items():
                if spec['action'] not in {'context','calibration','filter_smoke','initial'}: continue
                if not any(a['node']==key and a['outcome']=='worker-completed' for a in state['attempts']): continue
                receipt=native_receipt(predecessor.resolve(),key,instance)
                file=predecessor/'cells'/key.replace(':','__')/receipt['run_id']/'receipt.json'
                spec['native_adoption']={'root':str(predecessor.resolve()),'run_id':receipt['run_id'],'receipt_sha256':sha(file)}
        manifest['predecessor']=str(predecessor.resolve())
    validate_window(manifest)
    manifest['sources']=freeze_sources(root);save(root/'manifest.json',manifest)
    from vectorbench.orchestration.engine import build
    import dagster as dg
    dg.Definitions.validate_loadable(build(root))
    return {'nodes':len(manifest['nodes']),'histories':66,'fresh':138,'root':str(root)}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--setup',type=Path,required=True);p.add_argument('--predecessor',type=Path)
    a=p.parse_args();print(prepare(a.root,a.setup,a.predecessor))
