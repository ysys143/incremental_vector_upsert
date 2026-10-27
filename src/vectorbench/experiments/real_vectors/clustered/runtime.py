"""Actual isolated COPY/upsert paths for the frozen clustered experiment."""
from pathlib import Path
import json
import math
import time
import numpy as np

from vectorbench.evidence.contracts import Config, load, save, sha, check_space
from vectorbench.datasets.storage import load_state, load_events
from vectorbench.evaluation.measure import metadata_arrays, verify_current
from vectorbench.backends.server import Server
from vectorbench.workloads.events import Live
from vectorbench.workloads.workload import oracle_for, read_point
from vectorbench.orchestration.isolation import measured
from vectorbench.experiments.real_vectors.baseline.baseline_tuning import finish_server

EFS=(100,128,256,512,1000)
BUILDS={'postgres':((16,100),(32,200)), 'qdrant':((48,300),(64,400))}


def queries(prepared, seed):
    with np.load(prepared['query_rows']) as data:
        return data['calibration' if int(seed)==7 else 'evaluation'].astype(np.int64)


def cohort_labels(seed, count):
    # The data producer binds all diagnostic cohort arrays; the primary partition is old/new/boundary.
    value=seed.get('cohort_labels')
    if value is not None:
        return list(value)
    path=seed.get('cohorts')
    if path:
        with np.load(path) as data:
            codes=data['primary'] if 'primary' in data else data[data.files[0]]
        names=seed.get('cohort_names',['old','boundary','new'])
        return [names[int(v)] for v in codes]
    raise AssertionError('frozen final query cohorts missing')


def settings(adapter, out):
    if adapter.name=='postgres':
        names=('enable_seqscan','enable_indexscan','seq_page_cost','random_page_cost','cpu_tuple_cost',
               'cpu_index_tuple_cost','cpu_operator_cost','effective_cache_size','max_parallel_workers_per_gather',
               'max_parallel_maintenance_workers','hnsw.iterative_scan','hnsw.ef_search')
        result={k:adapter.sql('SHOW '+k+';') for k in names}
        if result['enable_seqscan']!='off' or result['enable_indexscan']!='on':
            raise AssertionError('ANN session settings differ')
    else: result=adapter.state()
    save(out/'effective-settings.json',result)
    return result


def write_wave(adapter,event,pool,out,number,c):
    """Keep one PG commit per logical wave; Qdrant sends <=1000 serial points/request."""
    if event.deletes or event.operation!='insert' or not event.writes:
        raise ValueError('clustered experiment accepts nonempty insert-only waves')
    ids=np.array([w.id for w in event.writes],dtype=np.int64)
    rows=np.array([w.row for w in event.writes],dtype=np.int64)
    attrs=np.array([[w.category,w.ordinal,w.version] for w in event.writes],dtype=np.int64)
    if len(set(ids.tolist()))!=len(ids): raise AssertionError('duplicate write ID')
    target=out/f'write-{number}.json';started=time.time()
    result={'operation':'insert','written':len(ids),'deleted':0,'status':'INTENT','started_at':started,
            'requests':[],'commits':[],'transport_batch':1000,'logical_wave':number}
    save(target,result)
    adapter.batch=1000
    original=adapter.sql if adapter.name=='postgres' else adapter.request
    def sql(statement):
        record=None
        if statement.startswith('COPY '):
            record={'kind':'COPY','rows':len(statement.splitlines())-2,'bytes':len(statement.encode()),
                    'started_at':time.time(),'status':'INTENT'}
            result['requests'].append(record);save(target,result)
        answer=original(statement)
        if record is not None:
            record.update(status='ACKNOWLEDGED',finished_at=time.time());save(target,result)
        if statement=='COMMIT;': result['commits'].append({'status':'ACKNOWLEDGED','finished_at':time.time()})
        return answer
    def request(method,url,body=None):
        if method=='PUT' and '/points?' in url:
            encoded=json.dumps(body,separators=(',',':')).encode()
            if len(encoded)>c.max_payload_bytes: raise ValueError('request byte cap')
            record={'kind':'PUT','url':url,'rows':len(body['batch']['ids']),'bytes':len(encoded),
                    'started_at':time.time(),'status':'INTENT'}
            result['requests'].append(record);save(target,result)
            answer=adapter.request_encoded(method,url,encoded)
            record.update(status='ACKNOWLEDGED',finished_at=time.time());save(target,result)
            return answer
        return original(method,url,body)
    if adapter.name=='postgres': adapter.sql=sql
    else: adapter.request=request
    try:
        if adapter.name=='postgres':adapter.sql('BEGIN;')
        adapter.insert(ids,pool.vectors[rows],attrs)
        if adapter.name=='postgres':adapter.sql('COMMIT;')
        if len(result['requests'])!=math.ceil(len(ids)/1000) or sum(x['rows'] for x in result['requests'])!=len(ids):
            raise AssertionError('transport lost or split rows incorrectly')
        if adapter.name=='postgres' and len(result['commits'])!=1:raise AssertionError('one wave must have one PG commit')
        result.update(status='ACKNOWLEDGED',finished_at=time.time(),acknowledgement_s=time.time()-started)
        save(target,result)
        return result
    except BaseException as exc:
        result.update(status='OUTCOME_REQUIRES_RECONCILIATION',error=repr(exc),finished_at=time.time());save(target,result)
        raise
    finally:
        if adapter.name=='postgres':adapter.sql=original
        else:adapter.request=original


def evaluate(server,adapter,live,pool,qrows,control,cohorts,out,c,subset=None):
    truth=oracle_for(control,pool,live,qrows)
    indices=np.arange(len(qrows)) if subset is None else np.asarray(subset,dtype=int)
    return read_point(server,adapter,live,pool,np.asarray(pool.queries[qrows[indices]]),truth[indices],
                      np.asarray(cohorts)[indices].tolist(),out,c,{**control,'query_subset_indices':indices.tolist()})


def ef_points(server,adapter,live,pool,qrows,control,cohorts,out,c,main_ef):
    values=[e for e in EFS if e!=main_ef][:2]
    result=[]
    try:
        for ef in values:
            adapter.set_ef(ef)
            p=evaluate(server,adapter,live,pool,qrows,control,cohorts,out/f'ef-{ef}',c)
            result.append({'ef':ef,'point':p})
    finally:adapter.set_ef(main_ef)
    return result


def initial_snapshot(db,pool,prepared,seed_id,tuning,out,c):
    seed=prepared['seeds'][str(seed_id)];qrows=queries(prepared,seed_id)
    live=load_state(seed['initial'],len(pool.vectors));control=seed['initial_control']
    cohorts=cohort_labels(seed,len(qrows));server=Server(db,out/'server',config=c);error=None;started=time.time()
    try:
        a=server.start();a.batch=1000;ids,rows,attrs=metadata_arrays(live)
        setup=measured(server,out/'build',lambda:a.setup(ids,pool.vectors[rows],tuning['build'],attributes=attrs))
        a.set_ef(tuning['ef']);setting=settings(a,out)
        before=evaluate(server,a,live,pool,qrows,control,cohorts,out/'before-restart',c,range(8))
        a=server.restart();a.set_ef(tuning['ef'])
        after=evaluate(server,a,live,pool,qrows,control,cohorts,out/'after-restart',c,range(8))
        target=out/'initial.tar.gz';export=server.export(target)
        archive={k:export[k] for k in ('archive_sha','files_hash','segment_signature','graph_hash')}
        archive['archive']=str(target)
        return {'status':'PASS','archive':archive,'setup':setup,'initial_ledger_sha256':live.digest(),
                'before_restart':before,'after_restart':after,'settings':setting,'elapsed_s':time.time()-started}
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)


def history(db,pool,prepared,seed_id,path_name,tuning,archive,out,c,extra=True):
    started=time.time();seed=prepared['seeds'][str(seed_id)];path=seed['paths'][path_name]
    qrows=queries(prepared,seed_id);cohorts=cohort_labels(seed,len(qrows))
    live=load_state(seed['initial'],len(pool.vectors));events=load_events(path['events'])
    server=Server(db,out/'server',config=c,**archive);error=None;points=[];writes=[];states=[]
    try:
        a=server.start();a.set_ef(tuning['ef']);first=a.state();setting=settings(a,out)
        for step in range(len(events)+1):
            if step:
                check_space(out,c)
                expected=Live(live.records(),len(pool.vectors)).apply(events[step-1])
                writes.append(write_wave(a,events[step-1],pool,out,step,c));live=expected
            subset=None if step==len(events) else range(min(200,len(qrows)))
            points.append(evaluate(server,a,live,pool,qrows,path['controls'][step],cohorts,out/f'point-{step}',c,subset))
            state=a.state();states.append(state)
            if db=='postgres' and any(first[k]!=state[k] for k in ('table_oid','index_oid')):
                raise AssertionError('HNSW was rebuilt during insertion')
        extras=ef_points(server,a,live,pool,qrows,path['controls'][-1],cohorts,out/'extra',c,tuning['ef']) if extra else []
        if live.digest()!=path['final_ledger_sha256']:raise AssertionError('final ledger mismatch')
        result={'status':'PASS','points':points,'steps':list(range(len(events)+1)),'writes':writes,'states':states,
                'final_ledger_sha256':live.digest(),'initial_snapshot_sha256':archive['archive_sha'],
                'settings':setting,'ef_points':extras,'elapsed_s':time.time()-started,
                'dataset':prepared['source']['dataset'],'db':db,'seed':seed_id,'path':path_name,'input_path':path}
        save(out/'history.json',result);return result
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)


def state_reference(seed,state):
    if state=='initial':return {'state':{'path':seed['initial']},'control':seed['initial_control']}
    if state=='final':path=seed['paths']['M1'];step=1
    else:
        name,step=state.split(':step');path=seed['paths'][name];step=int(step)
    return {'state':path['states'][str(step)],'control':path['controls'][step]}


def fresh(db,pool,prepared,seed_id,state,repeat,tuning,out,c,extra=True):
    started=time.time();seed=prepared['seeds'][str(seed_id)];ref=state_reference(seed,state)
    qrows=queries(prepared,seed_id);live=load_state(ref['state']['path'],len(pool.vectors))
    cohorts=cohort_labels(seed,len(qrows));server=Server(db,out/'server',config=c);error=None
    try:
        a=server.start();a.batch=1000;ids,rows,attrs=metadata_arrays(live)
        permutation=np.random.default_rng(70000+int(seed_id)*100+int(repeat)).permutation(len(ids))
        save(out/'build-order.json',{'seed':70000+int(seed_id)*100+int(repeat),
                                    'ids_sha256':__import__('hashlib').sha256(ids[permutation].tobytes()).hexdigest()})
        setup=measured(server,out/'build',lambda:a.setup(ids[permutation],pool.vectors[rows[permutation]],
                         tuning['build'],attributes=attrs[permutation]))
        a.set_ef(tuning['ef']);setting=settings(a,out)
        subset=range(min(200,len(qrows))) if ':step' in state else None
        point=evaluate(server,a,live,pool,qrows,ref['control'],cohorts,out/'point',c,subset)
        extras=ef_points(server,a,live,pool,qrows,ref['control'],cohorts,out/'extra',c,tuning['ef']) if extra and state=='final' else []
        return {'status':'PASS','point':point,'setup':setup,'state':state,'ledger_sha256':live.digest(),
                'settings':setting,'ef_points':extras,'elapsed_s':time.time()-started,'input_ref':ref,
                'dataset':prepared['source']['dataset'],'db':db,'seed':seed_id,'repeat':repeat}
    except BaseException as exc:error=exc;raise
    finally:finish_server(server,out,error)
