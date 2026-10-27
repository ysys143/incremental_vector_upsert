"""Create a new native campaign from verified completed preparation only."""
import argparse
import copy
from pathlib import Path
import shutil
import time
import dagster as dg
from vectorbench.evidence.contracts import load,save,sha
from vectorbench.evidence.registry import Registry
from vectorbench.evidence.source_manifest import freeze_sources
from vectorbench.orchestration.engine import native_receipt
from vectorbench.experiments.real_vectors.clustered.contract import validate_window


def prepare(previous,root):
    previous=previous.resolve();root=root.resolve()
    old=load(previous/'manifest.json');auth=validate_window(old)
    if root.exists():raise AssertionError('successor already exists; never overwrite evidence')
    controller=load(previous/'controller.json');guardian=load(previous/'guardian-receipt.json')
    registry=Registry(previous/'admission.sqlite').snapshot()
    if not controller['finished'] or controller['status']!='STOPPED_INCOMPLETE' or registry['leases']:
        raise AssertionError('previous owner not stopped and reconciled')
    if guardian['status']!='RECONCILED' or any(v['cleanup']['errors'] for v in guardian['cleanup']):
        raise AssertionError('previous resource cleanup failed')
    with dg.DagsterInstance.from_config(str(previous/'dagster-home')) as instance:
        failed=instance.get_runs(filters=dg.RunsFilter(tags={'realstudy/campaign':previous.name},statuses=[dg.DagsterRunStatus.FAILURE,dg.DagsterRunStatus.CANCELED]))
        failed_attempts=[a for a in registry['attempts'] if a['run_id'] in {r.run_id for r in failed}]
        if len(failed)!=1 or len(failed_attempts)!=1 or failed_attempts[0]['node']!='cost_gate:all':
            raise AssertionError('exception is limited to the sole failed forecast gate')
        failed_cost=previous/'cells/cost_gate__all'/failed[0].run_id/'cost-gate.json'
        cost=load(failed_cost)
        if cost['status']!='BLOCK' or cost['estimated_remaining_s']<=cost['remaining_s']:
            raise AssertionError('original forecast was not the blocking condition')
        if cost['free_bytes']-2*1024**3<10*1024**3 or cost['retained_bytes']+2*1024**3>8*1024**3:
            raise AssertionError('user exception does not authorize a failed disk gate')
        refs={}
        for attempt in registry['attempts']:
            if attempt['outcome']!='worker-completed':continue
            node=attempt['node'];receipt=native_receipt(previous,node,instance)
            path=previous/'cells'/node.replace(':','__')/receipt['run_id']/'receipt.json'
            refs[node]={'root':str(previous),'run_id':receipt['run_id'],'receipt_sha256':sha(path)}
        if len(refs)!=34 or 'cost_gate:all' in refs:raise AssertionError('unexpected preparation inventory')
    root.mkdir(parents=True)
    for name in ('authorization.json','approved-plan.md','approved-matrix.json','check_g0_source.py','delivery-requirements.json'):
        shutil.copy2(previous/name,root/name)
    approval={'action':'PROCEED_DESPITE_FORECAST_BLOCK','user_text':'그냥 진행해.',
              'recorded_at':time.time(),'original_t0':auth['t0'],'original_deadline':auth['deadline'],
              'predecessor_root':str(previous),'predecessor_manifest_sha256':sha(previous/'manifest.json'),
              'failed_cost_gate':{'path':str(failed_cost),'sha256':sha(failed_cost)},
              'scope':'forecast-fit admission only; unchanged scientific scope, tuning, original clock, disk and HNSW gates'}
    save(root/'forecast-exception.json',approval)
    current=copy.deepcopy(old)
    for node,ref in refs.items():current['nodes'][node]['native_adoption']=ref
    current['authorization']={'path':str(root/'authorization.json'),'sha256':sha(root/'authorization.json')}
    current['forecast_exception']={'path':str(root/'forecast-exception.json'),'sha256':sha(root/'forecast-exception.json')}
    current['reason']='explicit user forecast-fit exception; 34 verified preparation origins; all 184 remaining native nodes retained'
    current['sources']=freeze_sources(root)
    save(root/'manifest.json',current)
    from vectorbench.experiments.real_vectors.clustered.adopt import verify
    proofs={node:verify(root,node)['reference'] for node in refs}
    save(root/'continuation-preflight.json',{'status':'PASS','adopted':proofs,'original_t0':auth['t0'],
         'deadline':auth['deadline'],'native_nodes':len(current['nodes']),'new_nodes':len(current['nodes'])-len(refs),
         'forecast_status':'BLOCK','admission':'USER_AUTHORIZED_FORECAST_EXCEPTION','finished_at':time.time()})
    return {'root':str(root),'adopted':len(refs),'native_nodes':len(current['nodes']),'deadline':auth['deadline']}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('previous',type=Path);p.add_argument('root',type=Path)
    a=p.parse_args();print(prepare(a.previous,a.root))
