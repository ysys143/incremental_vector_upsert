"""Fail-closed scientific, path, and remaining-cost admissions."""
from pathlib import Path
import copy
import json
import shutil
import time
import numpy as np
from vectorbench.evidence.contracts import load,save,sha,Config,GIB,owned_output_bytes
from vectorbench.backends.adapters import assert_ann
from vectorbench.evidence.synthetic_telemetry import verify_delta,segments,UnverifiedTelemetry
from vectorbench.backends.server import Server
from vectorbench.datasets.storage import load_state
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.experiments.real_vectors.baseline.baseline_tuning import finish_server
from .contract import validate_window
from .runtime import queries,evaluate,settings


def reject(call,expected,label):
    try:call()
    except expected as exc:return {'name':label,'status':'PASS','rejected':type(exc).__name__,'reason':str(exc)}
    raise AssertionError('negative control was accepted: '+label)


def path_guard(db,pool,prepared,tuning,out,c):
    seed=prepared['seeds']['7'];live=load_state(seed['initial'],len(pool.vectors));qrows=queries(prepared,7)
    server=Server(db,out/'server',config=c);error=None;controls=[]
    try:
        a=server.start();a.batch=1000;ids,rows,attrs=metadata_arrays(live)
        setup=a.setup(ids,pool.vectors[rows],tuning['build'],attributes=attrs);a.set_ef(tuning['ef'])
        positive=evaluate(server,a,live,pool,qrows,seed['initial_control'],['calibration']*len(qrows),out/'positive',c,range(4))
        if db=='postgres':
            plan=a.path(pool.queries[qrows[0]])
            wrong=copy.deepcopy(plan)
            def change(node):
                if node.get('Node Type')=='Index Scan':node['Index Name']='other_hnsw'
                for child in node.get('Plans',[]):change(child)
            change(wrong[0]['Plan'])
            controls.append(reject(lambda:assert_ann(wrong,'off','items_hnsw','ip',True),AssertionError,'wrong-index-plan'))
            a.sql('SET enable_seqscan=on;')
            controls.append(reject(lambda:a.checked_search_sql(pool.queries[qrows[0]]),AssertionError,'seqscan-on'))
            a.sql('SET enable_seqscan=off; DROP INDEX items_hnsw;')
            controls.append(reject(lambda:a.checked_search_sql(pool.queries[qrows[0]]),AssertionError,'actual-HNSW-removed'))
        else:
            before=a.request('GET','/telemetry?details_level=4&anonymize=false')
            for q in pool.queries[qrows[:4]]:
                a.request('POST','/collections/items/points/query',{'query':q.tolist(),'limit':100,'params':{'exact':True}})
            after=a.request('GET','/telemetry?details_level=4&anonymize=false')
            save(out/'exact-before.json',before);save(out/'exact-after.json',after)
            controls.append(reject(lambda:verify_delta(before,after,4),UnverifiedTelemetry,'actual-exact-search'))
            controls.append(reject(lambda:verify_delta({},after,4),UnverifiedTelemetry,'missing-telemetry'))
            a.request('PUT','/collections/control',{'vectors':{'size':768,'distance':'Dot'},'shard_number':1,
                'hnsw_config':{'m':0,'full_scan_threshold':1000000},'optimizers_config':{'indexing_threshold':0,'default_segment_number':1}})
            a.request('PUT','/collections/control/points?wait=true',{'batch':{'ids':ids[:1000].tolist(),'vectors':pool.vectors[rows[:1000]].tolist()}})
            pb=a.request('GET','/telemetry?details_level=4&anonymize=false')
            for q in pool.queries[qrows[:4]]:
                a.request('POST','/collections/control/points/query',{'query':q.tolist(),'limit':100,'params':{'exact':False}})
            pa=a.request('GET','/telemetry?details_level=4&anonymize=false')
            save(out/'plain-before.json',pb);save(out/'plain-after.json',pa)
            sb,sa=segments(pb,'control',require_ann=False),segments(pa,'control',require_ann=False)
            plain=sum(v['counters']['unfiltered_plain']['count'] for v in sa.values())-sum(v['counters']['unfiltered_plain']['count'] for v in sb.values())
            if plain<4:raise AssertionError('plain negative control did not actually use plain search')
            controls.append(reject(lambda:verify_delta(pb,pa,4,'control'),UnverifiedTelemetry,'actual-plain-search'))
        if len(controls)!=3:raise AssertionError('incomplete path controls')
        result={'status':'PASS','db':db,'positive':positive,'negative_controls':controls,'setup':setup}
        save(out/'path-guard.json',result);return result
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)


def cost_gate(root,manifest,deps,out):
    auth=validate_window(manifest);rates={}
    for db in ('postgres','qdrant'):
        one=[v for k,v in deps.items() if k.startswith('smoke_history:') and v['db']==db and v['path']=='M1']
        five=[v for k,v in deps.items() if k.startswith('smoke_history:') and v['db']==db and v['path']=='C5']
        fresh=[v for k,v in deps.items() if k.startswith('smoke_fresh:') and v['db']==db]
        initial=[v for k,v in deps.items() if k.startswith('smoke_initial:') and '-'+db in k]
        if not all(len(x)==2 for x in (one,five,fresh,initial)):raise AssertionError('missing smoke dataset/db')
        def scale_point(point,count):
            return (point['finished_at']-point['started_at'])*count/point['queries']
        # Smoke uses 200 calibration queries. Evaluation endpoints/fresh use 600.
        rate1=max(v['native_timing']['elapsed_s']+scale_point(v['points'][-1],400) for v in one)
        rate5=max(v['native_timing']['elapsed_s']+scale_point(v['points'][-1],400) for v in five)
        ratefresh=max(v['native_timing']['elapsed_s']+scale_point(v['point'],400) for v in fresh)
        rateinitial=max(v['native_timing']['elapsed_s'] for v in initial)
        # Extra ef is conservatively priced at the slowest measured main-ef point.
        rateextra=max(scale_point(v['point'],600) for v in fresh)
        base=12*rate1+12*rate5+60*ratefresh+6*rateinitial+84*rateextra
        rates[db]={'one_history_s':rate1,'five_history_s':rate5,'fresh_s':ratefresh,'initial_s':rateinitial,
                   'extra_ef_s':rateextra,'unscaled_db_seconds':base,'conservative_seconds':1.5*base}
    # Every remaining native run has startup/scheduling overhead in addition to measured DB time.
    native_overhead=190*10
    reserve=5400;report=3600;cpu=600
    estimated=sum(v['conservative_seconds'] for v in rates.values())+native_overhead+reserve+report+cpu
    free=shutil.disk_usage(root).free;retained=owned_output_bytes(root)
    result={'status':'PASS' if time.time()+estimated<=auth['deadline'] else 'BLOCK',
            'rates':rates,'estimated_remaining_s':estimated,'remaining_s':auth['deadline']-time.time(),
            'safety_multiplier':1.5,'native_overhead_s':native_overhead,'report_s':report,'reserve_s':reserve,'cpu_s':cpu,
            'retained_bytes':retained,'free_bytes':free,'disk_policy':'OBSERVE_ONLY','finished_at':time.time(),'deadline':auth['deadline'],
            'scope':'full approved 48 histories,120 fresh,168 extra ef; no silent reduction'}
    if manifest.get('forecast_exception'):
        ref=manifest['forecast_exception']
        if sha(ref['path'])!=ref['sha256']:raise AssertionError('forecast exception authorization changed')
        approval=load(ref['path']);oldref=approval['failed_cost_gate']
        if (approval['action']!='PROCEED_DESPITE_FORECAST_BLOCK' or approval['original_deadline']!=auth['deadline']
                or approval['original_t0']!=auth['t0'] or not approval['user_text'].strip()
                or sha(oldref['path'])!=oldref['sha256']):
            raise AssertionError('forecast exception scope or original evidence changed')
        prior=load(oldref['path'])
        if prior['status']!='BLOCK' or prior['deadline']!=auth['deadline'] or prior['rates']!=rates:
            raise AssertionError('forecast exception may not change measured rates')
        result['forecast_status']='PASS' if time.time()+estimated<=auth['deadline'] else 'BLOCK'
        result['forecast_exception']=ref
        result['original_failed_forecast']=oldref
        result['admission_status']='USER_AUTHORIZED_FORECAST_EXCEPTION'
        result['status']='PASS' if time.time()<auth['deadline']-3600 else 'BLOCK'
    save(out/'cost-gate.json',result)
    if result['status']!='PASS':raise AssertionError('G3 full remaining cost/disk does not fit: '+json.dumps(result))
    return result
