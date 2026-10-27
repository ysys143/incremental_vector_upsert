"""Read-only native completion audit, independent of report's numerical summary.

Run with the repository's Dagster interpreter after the controller is terminal.
Never upgrades a partial campaign or bypasses native failure/check evidence.
"""
import argparse
from collections import Counter
from pathlib import Path
import time
import dagster as dg
from vectorbench.evidence.contracts import load, save, sha
from vectorbench.orchestration.engine import native_receipt
from vectorbench.evidence.registry import Registry
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


def audit(root):
    manifest=load(root/'manifest.json');auth=validate_window(manifest)
    controller=load(root/'controller.json')
    if controller['status']!='COMPLETE_SELECTED_SCOPE' or not controller['finished']:
        raise AssertionError('controller has not completed the declared inventory')
    if controller['finished_at']>auth['deadline'] or controller['deadline']>auth['deadline']:
        raise AssertionError('original twelve-hour deadline was exceeded')
    guardian=load(root/'guardian-receipt.json')
    if guardian['status']!='RECONCILED' or any(v.get('cleanup',{}).get('errors') for v in guardian['cleanup']):
        raise AssertionError('final owned cleanup incomplete')
    registry=Registry(root/'admission.sqlite').snapshot()
    if registry['leases'] or any(a['ended'] is None for a in registry['attempts']):
        raise AssertionError('live or unreconciled task remains')
    for name,digest in manifest['sources'].items():
        if sha(root/'source'/name)!=digest or sha(name)!=digest:
            raise AssertionError('frozen source differs: '+name)
    receipts={}; checks=[]; check_ends={}
    with dg.DagsterInstance.from_config(str(root/'dagster-home')) as instance:
        runs=instance.get_runs(filters=dg.RunsFilter(tags={'realstudy/campaign':root.name}))
        if any(r.status!=dg.DagsterRunStatus.SUCCESS for r in runs):
            raise AssertionError('non-successful native run exists in completion scope')
        for node,spec in manifest['nodes'].items():
            value=native_receipt(root,node,instance)
            check_events=[e for e in instance.all_logs(value['run_id']) if e.asset_check_evaluation]
            evaluations=[e.asset_check_evaluation for e in check_events]
            expected_partition=None if spec.get('singleton') else spec['id']
            if len(evaluations)!=1 or not evaluations[0].passed or evaluations[0].partition!=expected_partition:
                raise AssertionError('required partition-scoped native check missing: '+node)
            receipts[node]=value
            check_ends[node]=max(e.timestamp for e in check_events)
            checks.append({'node':node,'run_id':value['run_id'],'partition':expected_partition})
    history=[(manifest['nodes'][k],v['result']) for k,v in receipts.items() if manifest['nodes'][k]['action']=='history']
    fresh=[v['result'] for k,v in receipts.items() if manifest['nodes'][k]['action']=='fresh']
    if len(history)!=66 or len(fresh)!=138: raise AssertionError('full history/fresh inventory missing')
    count_points=0; count_filters=0;count_curves=0;queries=0
    declared_filters=Counter(); declared_curves=Counter()
    def probe_counts(value,db,state,index):
        nonlocal count_filters,count_curves,queries
        seen=set()
        for p in value.get('filters',[]):
            if p['profile'] in seen or p['queries']!=200 or p['status']!='PASS' or p['path']['status']!='PASS':
                raise AssertionError('duplicate/unverified filtered condition')
            seen.add(p['profile']); declared_filters[db,state,index,p['profile']]+=1
            count_filters+=1;queries+=200
        for p in value.get('ef_curves',[]):
            if p['point']['queries']!=200 or p['point']['ann']['status']!='PASS': raise AssertionError('unverified ef curve')
            declared_curves[db,state,index,p['ef']]+=1
            count_curves+=1;queries+=200
    for spec,value in history:
        if value['steps']!=spec['quality_steps'] or len(value['points'])!=len(spec['quality_steps']): raise AssertionError('checkpoint coverage changed')
        if len(value['writes'])!=spec['waves']: raise AssertionError('declared update count differs')
        if len(value['unmeasured_wave_verifications'])!=spec['waves']+1-len(spec['quality_steps']): raise AssertionError('unmeasured wave validation missing')
        for p,step in zip(value['points'],value['steps']):
            if p['queries']!=spec['queries_by_step'][str(step)] or p['ann']['status']!='PASS': raise AssertionError('checkpoint query/ANN differs')
            count_points+=1;queries+=p['queries']
            probe_counts(value['extra_probes'].get(str(step),{}),spec['db'],'initial' if step==0 else 'final','incremental')
    for key,receipt in receipts.items():
        spec=manifest['nodes'][key]
        if spec['action']!='fresh': continue
        value=receipt['result'];queries+=value['point']['queries']
        if value['point']['queries']!=600 or value['point']['ann']['status']!='PASS': raise AssertionError('fresh query/ANN differs')
        if spec['family']=='filter': state='initial' if spec['state_step']==0 else 'final'
        elif spec['family']=='baseline':
            context=receipts['study_context:'+spec['dataset']]['result']
            initial_digest=context['prepared']['seeds'][str(spec['seed'])]['paths']['read_only-long']['final_ledger_sha256']
            state='initial' if value['state']==initial_digest else 'final'
        else: state='final'
        probe_counts(value['extra_probes'],spec['db'],state,'fresh')
    expected_filters=Counter((db,state,index,'F'+str(n)) for db in ('postgres','qdrant') for state in ('initial','final') for index in ('incremental','fresh') for n in range(1,9))
    expected_curves=Counter((db,state,index,ef) for db,efs in [('postgres',(100,256,1000)),('qdrant',(100,256,512))] for state in ('initial','final') for index in ('incremental','fresh') for ef in efs)
    if declared_filters!=expected_filters or declared_curves!=expected_curves:
        raise AssertionError('filter or ef conditions missing, duplicated, or mislabeled')
    if (count_points,count_filters,count_curves,queries)!=(316,64,24,190000): raise AssertionError('measured inventory differs')
    # Prove actual heavy-worker windows never overlap a DB lease, including its native check.
    windows=[]
    for node,r in receipts.items():
        a=next(a for a in registry['attempts'] if a['node']==node and a['run_id']==r['run_id'])
        windows.append((r['lease_acquired'],max(a['ended'],check_ends[node]),manifest['nodes'][node]['resource'],node))
    for start,end,kind,node in windows:
        if kind=='db':
            overlaps=[other for s,e,k,other in windows if other!=node and s<end and e>start]
            if overlaps: raise AssertionError('heavy worker overlapped DB interval: '+node)
    report=receipts['report:all']['result']
    if report['status']!='PASS' or report['finished_at']>auth['deadline']: raise AssertionError('final report incomplete/late')
    return {'status':'PASS','audited_at':time.time(),'t0':auth['t0'],'deadline':auth['deadline'],
        'native_nodes':len(receipts),'histories':66,'history_points':count_points,'fresh_builds':138,
        'filter_probes':count_filters,'ef_probes':count_curves,'queries':queries,'native_checks':checks,
        'report':report['report'],'scope':'approved twelve-hour scope only; broader deferred research remains incomplete'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();root=a.root.resolve()
    result=audit(root);save(root/'independent-completion-audit.json',result)
    print({k:v for k,v in result.items() if k!='native_checks'})
