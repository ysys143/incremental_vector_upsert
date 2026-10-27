"""Rebind complete native preparation only; preserve its raw artifacts and clock."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import dagster as dg
from vectorbench.evidence.contracts import load,save,sha
from vectorbench.orchestration.engine import native_receipt

ALLOWED_ACTIONS={'context','calibration','filter_smoke','initial'}
ORCHESTRATION_CHANGES={'src/vectorbench/experiments/real_vectors/twelve_hour/twelve_campaign.py','src/vectorbench/orchestration/task.py','src/vectorbench/experiments/real_vectors/twelve_hour/twelve_adopt.py'}


def verify(current_root,node):
    current=load(current_root/'manifest.json');spec=current['nodes'][node];ref=spec['native_adoption']
    previous_root=Path(ref['root']);previous=load(previous_root/'manifest.json');old=previous['nodes'][node]
    if previous['mode']!=current['mode'] or current['mode']!='real-twelve-hour' or spec['action'] not in ALLOWED_ACTIONS:
        raise AssertionError('adoption is limited to unchanged successful preparation')
    if previous['config']!=current['config'] or previous['matrix']!=current['matrix'] or previous['execution_window_end']!=current['execution_window_end']:
        raise AssertionError('adoption changes scientific scope/config/deadline')
    def scientific(value): return {k:v for k,v in value.items() if k not in {'deps','native_adoption'}}
    if scientific(old)!=scientific(spec): raise AssertionError('adoption changes native task specification')
    for name,digest in previous['sources'].items():
        if sha(previous_root/'source'/name)!=digest: raise AssertionError('prior source snapshot changed')
        if name not in ORCHESTRATION_CHANGES and sha(name)!=digest:
            raise AssertionError('scientific consumer changed: '+name)
    if not load(previous_root/'controller.json')['finished']:
        raise AssertionError('prior controller still owns execution')
    with dg.DagsterInstance.from_config(str(previous_root/'dagster-home')) as instance:
        receipt=native_receipt(previous_root,node,instance)
        checks=[e.asset_check_evaluation for e in instance.all_logs(receipt['run_id']) if e.asset_check_evaluation]
        if len(checks)!=1 or not checks[0].passed or checks[0].partition!=old['id']:
            raise AssertionError('prior partition-scoped native blocking check missing')
    path=previous_root/'cells'/node.replace(':','__')/receipt['run_id']/'receipt.json'
    if sha(path)!=ref['receipt_sha256'] or receipt['run_id']!=ref['run_id']:
        raise AssertionError('adopted receipt changed or run differs')
    bindings={os.path.relpath(previous_root/name,current_root):digest for name,digest in receipt['artifacts'].items()}
    bindings[os.path.relpath(path,current_root)]=sha(path)
    return {'status':'PASS','result':receipt['result'],'artifacts':bindings,
            'reference':{'root':str(previous_root),'node':node,'run_id':receipt['run_id'],'receipt_sha256':sha(path)},
            'scope':'same completed preparation; no failed or partial DB write replay'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('node');a=p.parse_args()
    print(json.dumps(verify(a.root.resolve(),a.node)))
