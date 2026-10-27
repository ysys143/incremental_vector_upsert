"""Native action dispatch for the frozen clustered-insertion catalogue."""
from pathlib import Path
import time
from vectorbench.evidence.contracts import Config,load,save,sha
from vectorbench.experiments.real_vectors.actions import parents,pool_from
from .contract import validate_window


def execute(root,node,run_id,out,manifest,spec):
    auth=validate_window(manifest);deps=parents(root,out);c=Config(**manifest['config']);action=spec['action']
    for ref in load(out/'admission.json')['dependencies']:
        receipt=load(root/'cells'/ref['node'].replace(':','__')/ref['run_id']/'receipt.json')
        deps[ref['node']]['native_timing']=receipt['result'].get('original_native_timing',
            {'lease_acquired':receipt['lease_acquired'],'finished':receipt['finished'],'elapsed_s':receipt['finished']-receipt['lease_acquired']})
    if action=='prepare':
        from vectorbench.datasets.data import Pool
        from .data import prepare,validate_prepared
        source=manifest['source_refs'][spec['dataset']]
        pool=Pool.open(source['dataset'],source['base_path'],source['query_path'])
        if pool.base_sha!=source['base_sha256'] or pool.query_sha!=source['query_sha256']:raise AssertionError('source changed')
        from .diagnostics import prior_sensitivity
        sensitivity=prior_sensitivity(manifest['prior_calibration_refs'][spec['dataset']],out)
        result=prepare(pool,out/'data',c)
        result['prior_sensitivity']=sensitivity
        result['source']=source
        validate_prepared(result,pool)
        save(out/'prepared.json',result);return result
    if action=='barrier':
        if node=='tuned:all' and time.time()-min(v['started_at'] for v in deps.values())>5400:
            raise TimeoutError('G2 declared 1.5h calibration reservation exceeded')
        return {'status':'PASS','parents':list(deps)}
    if action=='cost_gate':
        from .gates import cost_gate
        return cost_gate(root,manifest,deps,out)
    if action=='report':
        from .report import report
        return report(root,manifest,deps,out)
    if action=='select':
        from .tuning import select
        return select([v for k,v in deps.items() if k.startswith('calibration:')],out)
    if 'cost_gate:all' in deps and spec.get('seed')!=7 and not manifest.get('completion_policy'):
        rates=deps['cost_gate:all']['rates'][spec['db']]
        unit=rates['initial_s'] if action=='initial' else rates['fresh_s'] if action=='fresh' else rates['one_history_s' if spec['path'].endswith('1') else 'five_history_s']
        if spec.get('extra') and (action=='history' or spec.get('state')=='final'):unit+=2*rates['extra_ef_s']
        admission={'status':'PASS','estimated_seconds':1.5*unit+30,'started_at':time.time(),
                   'deadline':min(auth['deadline'],manifest['deadlines']['CAMPAIGN'])}
        if admission['started_at']+admission['estimated_seconds']>=admission['deadline']:raise TimeoutError('next cell plus cleanup does not fit original deadline')
        save(out/'remaining-time-admission.json',admission)
    prepared=deps['inputs:'+spec['dataset']];pool=pool_from(prepared)
    if action=='candidate':
        from .tuning import candidate
        return candidate(spec['db'],pool,prepared,spec['build'],out,c)
    tuning=deps['tuning:'+spec['dataset']+'-'+spec['db']]
    from .runtime import initial_snapshot,history,fresh
    if action=='initial':return initial_snapshot(spec['db'],pool,prepared,spec['seed'],tuning,out,c)
    if action=='history':return history(spec['db'],pool,prepared,spec['seed'],spec['path'],tuning,
                                       deps[spec['snapshot_node']]['archive'],out,c,spec['extra'])
    if action=='fresh':return fresh(spec['db'],pool,prepared,spec['seed'],spec['state'],spec['repeat'],tuning,out,c,spec['extra'])
    if action=='path_guard':
        from .gates import path_guard
        return path_guard(spec['db'],pool,prepared,tuning,out,c)
    raise ValueError('unknown clustered action: '+action)
