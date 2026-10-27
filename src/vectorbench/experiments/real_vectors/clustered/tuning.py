"""Held-out, state-qualified tuning; no evaluation feedback is consumed."""
import time
import json
import re
import numpy as np
from vectorbench.evidence.contracts import save
from vectorbench.datasets.storage import load_state,load_events
from vectorbench.evaluation.measure import metadata_arrays,verify_current
from vectorbench.backends.server import Server
from vectorbench.orchestration.isolation import measured
from vectorbench.experiments.real_vectors.baseline.baseline_tuning import finish_server
from .runtime import EFS,queries,cohort_labels,evaluate,write_wave,settings,state_reference


def memory_peak(out):
    samples=[]
    for path in out.rglob('resource-samples.jsonl'):
        for line in path.read_text().splitlines():
            raw=json.loads(line)['docker']['MemUsage'].split('/')[0].strip()
            found=re.fullmatch(r'([0-9.]+)\s*([A-Za-z]+)',raw)
            if not found:raise AssertionError('unrecognized Docker memory sample')
            scale={'B':1,'kB':1000,'KB':1000,'KiB':1024,'MB':1000**2,'MiB':1024**2,'GB':1000**3,'GiB':1024**3}[found[2]]
            samples.append(float(found[1])*scale)
    if not samples:raise AssertionError('candidate memory sampling missing')
    return int(max(samples))


def candidate(db,pool,prepared,build,out,c):
    started=time.time();seed=prepared['seeds']['7'];path=seed['paths']['C5'];qrows=queries(prepared,7)
    live=load_state(seed['initial'],len(pool.vectors));cohorts=['calibration']*len(qrows)
    events=load_events(path['events']);points=[];writes=[];costs=[]
    server=Server(db,out/'incremental-server',config=c);error=None
    try:
        a=server.start();a.batch=1000;ids,rows,attrs=metadata_arrays(live)
        setup=measured(server,out/'initial-build',lambda:a.setup(ids,pool.vectors[rows],build,attributes=attrs))
        costs.append(setup);settings(a,out)
        for step in range(6):
            if step:
                live.apply(events[step-1]);writes.append(write_wave(a,events[step-1],pool,out,step,c))
            if step in (0,1,3,5):
                for ef in EFS:
                    a.set_ef(ef)
                    p=evaluate(server,a,live,pool,qrows,path['controls'][step],cohorts,out/f'step-{step}-ef-{ef}',c)
                    points.append({'state':f'C5:{step}','ef':ef,'point':p})
            else:measured(server,out/f'guard-{step}',lambda:verify_current(a,live,pool.vectors,out/f'verify-{step}'))
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)
    server=Server(db,out/'fresh-server',config=c);error=None
    try:
        a=server.start();a.batch=1000;ids,rows,attrs=metadata_arrays(live)
        order=np.random.default_rng(70007).permutation(len(ids))
        setup=measured(server,out/'fresh-build',lambda:a.setup(ids[order],pool.vectors[rows[order]],build,attributes=attrs[order]))
        costs.append(setup)
        for ef in EFS:
            a.set_ef(ef)
            p=evaluate(server,a,live,pool,qrows,path['controls'][-1],cohorts,out/f'fresh-ef-{ef}',c)
            points.append({'state':'fresh','ef':ef,'point':p})
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)
    result={'status':'PASS','db':db,'build':list(build),'points':points,'costs':costs,'writes':writes,
            'build_write_s':sum(v['load_s']+v['build_s'] for v in costs)+sum(v['acknowledgement_s'] for v in writes),
            'elapsed_s':time.time()-started,'memory_cap_bytes':c.db_memory_gib*1024**3,
            'peak_memory_usage_bytes':memory_peak(out),'started_at':started,'finished_at':time.time()}
    save(out/'candidate.json',result);return result


def select(groups,out):
    if len(groups)!=2 or any(len(v['points'])!=25 for v in groups):raise AssertionError('incomplete declared tuning grid')
    candidates=[]
    for group in groups:
        for ef in EFS:
            points=[p['point'] for p in group['points'] if p['ef']==ef]
            if len(points)!=5:raise AssertionError('missing calibration state')
            candidates.append({'build':group['build'],'ef':ef,'min_recall10':min(p['recall10'] for p in points),
                 'min_recall100':min(p['recall100'] for p in points),'max_p95_ms':max(p['p95_ms'] for p in points),
                 'build_write_s':group['build_write_s'],'peak_memory_usage_bytes':group['peak_memory_usage_bytes'],
                 'eligible':all(min(p['recall10'],p['recall100'])>=.99 and not p['shortfall100'] for p in points)})
    valid=[p for p in candidates if p['eligible']]
    costs=('max_p95_ms','build_write_s','peak_memory_usage_bytes')
    for p in valid:
        p['dominated']=any(all(q[k]<=p[k] for k in costs) and any(q[k]<p[k] for k in costs) for q in valid)
    save(out/'tuning-candidates.json',candidates)
    if not valid:raise AssertionError('G2: no candidate meets all-state 99% calibration target')
    frontier=[p for p in valid if not p['dominated']]
    fastest=min(p['max_p95_ms'] for p in frontier)
    chosen=min((p for p in frontier if p['max_p95_ms']<=fastest*1.05),key=lambda p:(p['build_write_s'],*p['build'],p['ef']))
    result={'status':'PASS','build':chosen['build'],'ef':chosen['ef'],'initial_batch':1000,'transport_batch':1000,
            'extra_efs':[e for e in EFS if e!=chosen['ef']][:2],'chosen':chosen,'candidates':candidates,
            'selection_scope':'finite held-out frontier, not global optimality',
            'started_at':min(v['started_at'] for v in groups),'finished_at':time.time(),
            'memory_scope':'approximately 1s Docker memory usage samples, not continuous peak RSS'}
    save(out/'tuning.json',result);return result
