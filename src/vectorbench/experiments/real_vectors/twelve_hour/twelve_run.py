"""Native consumers for the approved finite twelve-hour inventory."""
from pathlib import Path
import time
import numpy as np
from vectorbench.experiments.real_vectors.actions import parents,pool_from
from vectorbench.evidence.contracts import Config,load,save,sha
from vectorbench.datasets.storage import load_state,load_events
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.backends.server import Server
from vectorbench.workloads.workload import history,fresh,oracle_for,read_point
from vectorbench.orchestration.isolation import measured
from vectorbench.experiments.real_vectors.baseline.baseline_tuning import finish_server
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


def query_rows(context):
    p=context['prepared']['query_rows']
    if sha(p)!=context['input_bindings'][str(Path(p).resolve())]: raise AssertionError('query map changed')
    with np.load(p) as q: return q['evaluation'],q['calibration']


def path_for(context,spec):
    if spec['family']=='baseline': return context['prepared']['seeds'][str(spec['seed'])]['paths'][spec['source_case']]
    return context['extensions']['paths'][spec.get('selection','filter')]


def labels_for(context,spec):
    if spec['family']!='filter': return None
    ext=context['extensions']
    if sha(ext['filter_labels'])!=ext['filter_labels_sha256']: raise AssertionError('fixed labels changed')
    return load(ext['filter_labels'])


def extra_callback(context,spec,pool,queries,path,c,kind):
    subset=np.array(context['subset_indices']); tuning=context['tunings'][spec['db']]
    labels=labels_for(context,spec)
    is_curve=spec['family']=='baseline' and spec['dataset']=='msmarco' and spec['seed']==11
    if kind=='history': is_curve=is_curve and spec.get('source_case')=='upsert-r50-w5'
    else:
        seed=context['prepared']['seeds']['11']
        wanted={seed['paths'][name]['final_ledger_sha256'] for name in ('read_only-long','upsert-r50-w5')}
        is_curve=is_curve and spec.get('terminal_ledger') in wanted and spec['repeat']==0
    def callback(server,adapter,live,step,out):
        result={}
        if kind=='history' and step not in (0,spec['waves']): return result
        if labels is not None and (kind=='history' or spec['repeat']==0):
            from vectorbench.evaluation.twelve_filters import probes
            result['filters']=probes(server,adapter,live,pool,queries[subset],labels,out/'filters',c)
        if is_curve:
            control=path['controls'][step] if kind=='history' else path['control']
            truth=oracle_for(control,pool,live,queries)[subset]
            result['ef_curves']=[]
            try:
                for ef in ([100,256,1000] if spec['db']=='postgres' else [100,256,512]):
                    adapter.set_ef(ef)
                    point=read_point(server,adapter,live,pool,pool.queries[queries[subset]],truth,['all']*len(subset),
                        out/f'ef{ef}',c,{**control,'query_subset_indices':subset.tolist()})
                    result['ef_curves'].append({'ef':ef,'point':point})
            finally: adapter.set_ef(tuning['ef'])
        return result
    return callback


def initial_snapshot(context,spec,pool,out,c):
    prepared=context['prepared']; seed=prepared['seeds'][str(spec['seed'])]
    live=load_state(seed['initial'],len(pool.vectors)); queries,_=query_rows(context)
    control=seed['initial_control']; truth=oracle_for(control,pool,live,queries)
    tuning=context['tunings'][spec['db']]; labels=labels_for(context,spec)
    server=Server(spec['db'],out/'server',config=c,filter_labels=labels); error=None
    started=time.time()
    try:
        adapter=server.start();adapter.batch=tuning['initial_batch']
        ids,rows,attrs=metadata_arrays(live)
        setup=measured(server,out/'build',lambda:adapter.setup(ids,pool.vectors[rows],tuning['build'],attributes=attrs))
        adapter.set_ef(tuning['ef']); subset=np.array(context['subset_indices'])
        before=read_point(server,adapter,live,pool,pool.queries[queries[subset]],truth[subset],['all']*len(subset),out/'before-restart',c,{**control,'query_subset_indices':subset.tolist()})
        restart=time.time();adapter=server.restart();adapter.set_ef(tuning['ef'])
        restart_s=time.time()-restart
        after=read_point(server,adapter,live,pool,pool.queries[queries],truth,['all']*len(queries),out/'after-restart',c,control)
        if labels is not None:
            from vectorbench.evaluation.twelve_filters import verify_labels
            verify_labels(adapter,labels)
        target=out/'initial.tar.gz';exported=server.export(target)
        archive={k:exported[k] for k in ('archive_sha','files_hash','segment_signature','graph_hash')};archive['archive']=str(target)
        return {'archive':archive,'setup':setup,'before_restart':before,'after_restart':after,'restart_s':restart_s,
                'initial_ledger_sha256':live.digest(),'elapsed_s':time.time()-started,'family':spec['family']}
    except BaseException as exc: error=exc; raise
    finally: finish_server(server,out,error)


def calibration(context,spec,pool,out,c):
    prepared=context['prepared']; _,queries=query_rows(context); tuning=context['tunings'][spec['db']]; points=[]
    for name,ref in prepared['calibration'].items():
        folder=out/name; live=load_state(ref['state'],len(pool.vectors)); server=Server(spec['db'],folder/'server',config=c);error=None
        try:
            adapter=server.start();adapter.batch=tuning['initial_batch'];ids,rows,attrs=metadata_arrays(live)
            setup=measured(server,folder/'build',lambda:adapter.setup(ids,pool.vectors[rows],tuning['build'],attributes=attrs))
            adapter.set_ef(tuning['ef']); truth=oracle_for(ref['control'],pool,live,queries)
            point=read_point(server,adapter,live,pool,pool.queries[queries],truth,['calibration']*len(queries),folder/'point',c,ref['control'])
            if min(point['recall10'],point['recall100'])<.99 or point['shortfall100']:
                raise AssertionError('frozen candidate fails 99 percent held-out calibration')
            points.append({'state':name,'setup':setup,'point':point})
        except BaseException as exc: error=exc; raise
        finally: finish_server(server,folder,error)
    return {'status':'PASS','points':points,'tuning':tuning}


def filter_smoke(context,spec,pool,out,c):
    from vectorbench.evaluation.twelve_filters import probes
    from vectorbench.workloads.events import Event,Write
    from vectorbench.workloads.writes import wave
    initial=context['prepared']['seeds']['11']['initial']
    live=load_state(initial,len(pool.vectors)); queries,_=query_rows(context)
    queries=queries[np.array(context['subset_indices'])[:2]]
    labels=labels_for(context,spec); tuning=context['tunings'][spec['db']]
    server=Server(spec['db'],out/'server',config=c,filter_labels=labels);error=None
    try:
        adapter=server.start();adapter.batch=tuning['initial_batch'];ids,rows,attrs=metadata_arrays(live)
        adapter.setup(ids,pool.vectors[rows],tuning['build'],attributes=attrs);adapter.set_ef(tuning['ef'])
        before=probes(server,adapter,live,pool,queries,labels,out/'before',c)
        # Actual same-ID vector replacement, retaining the original fixed labels.
        selected=load_events(context['extensions']['paths']['filter']['events'])[0].writes[:2]
        event=Event(writes=tuple(selected),operation='upsert')
        trace=wave(adapter,event,pool.vectors,out,1,c);live.apply(event)
        after=probes(server,adapter,live,pool,queries,labels,out/'after',c)
        return {'status':'PASS','scope':'FILTER_PATH_SMOKE_ONLY','before':before,'after':after,'write':trace}
    except BaseException as exc: error=exc;raise
    finally: finish_server(server,out,error)


def execute(root,node,run_id,out,manifest,spec):
    auth=validate_window(manifest)
    if time.time()>=auth['deadline']: raise TimeoutError('global twelve-hour window expired')
    deps=parents(root,out); action=spec['action'];c=Config(**manifest['config'])
    if action=='context':
        from vectorbench.experiments.real_vectors.twelve_hour.twelve_data import context
        return context(manifest,spec['dataset'],out)
    if action=='barrier': return {'status':'PASS','native_parents':list(deps)}
    if action=='cost_gate':
        from vectorbench.experiments.real_vectors.twelve_hour.twelve_budget import assess
        return assess(root,manifest,deps,out)
    if action=='report':
        from vectorbench.reporting.twelve_report import report
        return report(root,manifest,deps,out)
    context=deps['study_context:'+spec['dataset']];pool=pool_from(context['prepared'])
    if action=='calibration': return calibration(context,spec,pool,out,c)
    if action=='filter_smoke': return filter_smoke(context,spec,pool,out,c)
    if action=='initial': return initial_snapshot(context,spec,pool,out,c)
    queries,_=query_rows(context); tuning=context['tunings'][spec['db']]
    if action=='history':
        path=path_for(context,spec)
        with np.load(path['cohorts']) as data: groups={step:[path['cohort_names'][int(v)] for v in data[step]] for step in data.files}
        initial=load_state(context['prepared']['seeds'][str(spec['seed'])]['initial'],len(pool.vectors))
        stem=f"{spec['dataset']}-s{spec['seed']}-{spec['db']}"
        result=history(spec['db'],pool,initial.records(),load_events(path['events']),queries,groups,path['controls'],tuning,
             deps[('filter_initial:' if spec['family']=='filter' else 'initial:')+stem]['archive'],out,config=c,quality_steps=spec['quality_steps'],subset_indices=context['subset_indices'],
             extra_probe=extra_callback(context,spec,pool,queries,path,c,'history'),server_options={'filter_labels':labels_for(context,spec)})
        if result['final_ledger_sha256']!=path['final_ledger_sha256']: raise AssertionError('history ledger differs')
        result.update(path=path,dataset=spec['dataset'],db=spec['db'],seed=spec['seed'],family=spec['family'])
        return result
    if action=='fresh':
        if spec['family']=='baseline': ref=context['prepared']['seeds'][str(spec['seed'])]['fresh_states'][spec['terminal_ledger']]
        else:
            path=context['extensions']['paths'][spec['selection']];step=spec['state_step']
            ref={'state':path['states'][str(step)],'control':path['controls'][step]}
        live=load_state(ref['state']['path'],len(pool.vectors))
        result=fresh(spec['db'],pool,live,queries,['all']*len(queries),ref['control'],tuning,out,c,
            extra_probe=extra_callback(context,spec,pool,queries,ref,c,'fresh'),server_options={'filter_labels':labels_for(context,spec)})
        result.update(group_id=spec['group_id'],repeat=spec['repeat'],family=spec['family'],dataset=spec['dataset'],db=spec['db'],seed=spec['seed'])
        return result
    raise ValueError(action)
