"""Controlled metadata and exact eligible-set quality; classify native search paths."""
import gzip
import hashlib
import json
from pathlib import Path
import time
import numpy as np
from vectorbench.experiments.pilot.model import packed_results, score
from vectorbench.evidence.synthetic_telemetry import COUNTERS, segments, UnverifiedTelemetry
from vectorbench.evidence.contracts import canonical, save, sha
from vectorbench.orchestration.isolation import measured
from vectorbench.evaluation.measure import metadata_arrays, verify_current
from vectorbench.evaluation.metrics import exact, direct_exact

FIELDS=('cat10','cat1000','spatial1000','head1000','unique_rank','uniform_rank')


def install_postgres_labels(adapter, ids):
    labels=adapter.filter_labels
    adapter.sql('ALTER TABLE '+adapter.table+' '+', '.join('ADD COLUMN '+f+' bigint' for f in FIELDS)+';')
    adapter.sql('CREATE TEMP TABLE filter_stage (id bigint,'+','.join(f+' bigint' for f in FIELDS)+');')
    lines=['\t'.join([str(i),*[str(labels[str(i)][f]) for f in FIELDS]]) for i in ids]
    adapter.sql('COPY filter_stage FROM STDIN;\n'+'\n'.join(lines)+'\n\\.')
    adapter.sql('UPDATE '+adapter.table+' t SET '+','.join(f+'=s.'+f for f in FIELDS)+' FROM filter_stage s WHERE t.id=s.id; DROP TABLE filter_stage;')
    for field in FIELDS:
        adapter.sql(f'ALTER TABLE {adapter.table} ALTER COLUMN {field} SET NOT NULL; CREATE INDEX {adapter.table}_{field} ON {adapter.table} ({field});')


def predicates(labels):
    def eq(field,value): return {'key':field,'match':{'value':value}}
    def q(*rules): return {'must':list(rules)}
    return [
      ('F1','cat10=0',q(eq('cat10',0)),lambda m:m['cat10']==0,1000),
      ('F2','cat1000 IN ('+','.join(map(str,range(10)))+')',q({'key':'cat1000','match':{'any':list(range(10))}}),lambda m:m['cat1000']<10,100),
      ('F3','cat1000=0',q(eq('cat1000',0)),lambda m:m['cat1000']==0,10),
      ('F4','spatial1000=0',q(eq('spatial1000',0)),lambda m:m['spatial1000']==0,10),
      ('F5','head1000=0',q(eq('head1000',0)),lambda m:m['head1000']==0,1000),
      ('F6','cat1000=0 AND uniform_rank=0',q(eq('cat1000',0),eq('uniform_rank',0)),lambda m:m['cat1000']==0 and m['uniform_rank']==0,1),
      ('F7','cat1000=-1',q(eq('cat1000',-1)),lambda m:m['cat1000']==-1,0),
      ('F8','unique_rank>=0 AND unique_rank<100',q({'key':'unique_rank','range':{'gte':0,'lt':100}}),lambda m:0<=m['unique_rank']<100,100)]


def filtered_plan(raw):
    setting,text=raw.split('\n',1); plan=json.loads(text)
    if setting!='off': raise AssertionError('filtered seqscan must be off')
    pending=[plan[0]['Plan']]; indexes=[]
    while pending:
        node=pending.pop()
        if node['Node Type'] in {'Seq Scan','Parallel Seq Scan'}: raise AssertionError('filtered sequential scan forbidden')
        if 'Index Name' in node: indexes.append(node['Index Name'])
        pending.extend(node.get('Plans',[]))
    if not indexes: raise AssertionError('filtered plan has no verified index path')
    return {'setting':setting,'plan':plan,'classification':'HNSW' if any(i.endswith('_hnsw') for i in indexes) else 'NATIVE_FILTERED_EXACT'}


def filtered_delta(before,after,count,eligible):
    a,b=segments(before),segments(after)
    if set(a)!=set(b): raise UnverifiedTelemetry('segment set changed during filtered reads')
    delta={name:0 for name in COUNTERS}
    for uid in a:
        if a[uid]['stable']!=b[uid]['stable']: raise UnverifiedTelemetry('segment/index changed during filtered reads')
        for name in COUNTERS:
            left,right=a[uid]['counters'][name],b[uid]['counters'][name]
            change=right['count']-left['count']
            if change<0 or left['fail_count']!=right['fail_count']: raise UnverifiedTelemetry('counter reset or failed filtered search')
            delta[name]+=change
    if any(v for k,v in delta.items() if k.startswith('unfiltered') or 'sparse' in k):
        raise UnverifiedTelemetry('unexpected unfiltered/sparse path')
    if eligible and sum(delta.values())<count: raise UnverifiedTelemetry('filtered telemetry below measured query count')
    used=[k for k,v in delta.items() if v]
    classification='NO_CANDIDATES' if not eligible and not used else 'HNSW' if used==['filtered_large_cardinality'] else 'NATIVE_FILTERED_PLAIN_OR_MIXED'
    return {'status':'PASS','classification':classification,'delta':delta,'request_exact':False,'queries':count}


def verify_labels(adapter,labels):
    if adapter.name=='postgres':
        values=json.loads(adapter.sql('SELECT json_agg(r) FROM (SELECT id,'+','.join(FIELDS)+' FROM '+adapter.table+' ORDER BY id) r;'))
    else:
        values=[]; offset=None
        while True:
            body={'limit':1000,'with_vector':False,'with_payload':True}
            if offset is not None: body['offset']=offset
            page=adapter.request('POST',f'/collections/{adapter.table}/points/scroll',body)['result']
            values.extend({'id':p['id'],**{f:p['payload'][f] for f in FIELDS}} for p in page['points'])
            offset=page.get('next_page_offset')
            if offset is None: break
    actual={str(v['id']):{f:v[f] for f in FIELDS} for v in values}
    if actual!=labels: raise AssertionError('fixed filter labels changed or missing')


def probes(server,adapter,live,pool,query_rows,labels,out,config):
    out.mkdir(parents=True,exist_ok=True)
    verify_current(adapter,live,pool.vectors,out/'state'); verify_labels(adapter,labels)
    ids,rows,_=metadata_arrays(live); queries=np.asarray(pool.queries[query_rows]); results=[]
    projection=np.asarray(pool.vectors[rows],dtype=np.float64) @ np.random.default_rng(1123).normal(size=768)
    save(out/'label-diagnostics.json',{
        'cardinalities':{f:len({labels[str(i)][f] for i in ids}) for f in FIELDS},
        'spatial_label_projection_correlation':float(np.corrcoef([labels[str(i)]['spatial1000'] for i in ids],projection)[0,1]),
        'ledger_sha256':live.digest(),'scope':'controlled label relation to fixed initial projection; not semantic metadata'})
    old_predicate=adapter.predicate
    try:
        for name,sql,predicate,accept,expected_count in predicates(labels):
            folder=out/name; folder.mkdir()
            mask=np.array([accept(labels[str(i)]) for i in ids]); eligible=ids[mask]
            if len(eligible)!=expected_count: raise AssertionError('realized eligible count differs: '+name)
            vectors=pool.vectors[rows[mask]]
            truth=exact(eligible,vectors,queries,metric='ip')
            np.testing.assert_array_equal(truth[:8],direct_exact(eligible,vectors,queries[:8],metric='ip'))
            np.savez_compressed(folder/'oracle.npz',ids=eligible,rows=rows[mask],query_rows=query_rows,truth=truth)
            adapter.predicate=sql if adapter.name=='postgres' else predicate
            def one(guards):
                if adapter.name=='postgres':
                    statement=adapter.search_sql(queries[0]) # replaced per query below
                returned=[]; latency=[]
                for vector in queries:
                    if adapter.name=='postgres':
                        statement=adapter.search_sql(vector)
                        guard=filtered_plan(adapter.sql('SHOW enable_seqscan;\nEXPLAIN (FORMAT JSON) '+statement)); guards.append(guard)
                        start=time.perf_counter(); row=json.loads(adapter.sql(statement))
                    else:
                        start=time.perf_counter(); row=adapter.query(vector)
                    returned.append(row); latency.append((time.perf_counter()-start)*1000)
                return returned,latency
            def collect():
                one([])
                before=adapter.request('GET','/telemetry?details_level=4&anonymize=false') if adapter.name=='qdrant' else None
                guards=[]; returned,latency=one(guards)
                if before is not None:
                    after=adapter.request('GET','/telemetry?details_level=4&anonymize=false')
                    save(folder/'telemetry-before.json',before); save(folder/'telemetry-after.json',after)
                    path=filtered_delta(before,after,len(queries),len(eligible))
                else:
                    actual=filtered_plan(adapter.sql('SHOW enable_seqscan;\nEXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+adapter.search_sql(queries[0])))
                    with gzip.open(folder/'plans.json.gz','wt') as f: json.dump(guards,f)
                    path={'status':'PASS','classifications':sorted({g['classification'] for g in guards}),'actual_plan':actual,'seqscan':'off'}
                quality=score(eligible,truth,returned) if len(eligible) else {'recall10':None,'recall100':None,'empty_correct':all(not r for r in returned)}
                if not len(eligible) and not quality['empty_correct']: raise AssertionError('empty predicate returned a result')
                packed=packed_results(returned)
                np.savez_compressed(folder/'raw.npz',**packed,latency_ms=latency)
                result={'status':'PASS','profile':name,'queries':len(queries),'eligible_count':len(eligible),
                    'predicate':sql if adapter.name=='postgres' else predicate,'path':path,**quality,
                    'p95_ms':float(np.percentile(latency,95)),'ledger_sha256':live.digest(),
                    'query_sha256':hashlib.sha256(queries.astype('<f4').tobytes()).hexdigest(),
                    'labels_sha256':hashlib.sha256(canonical(labels).encode()).hexdigest(),
                    'oracle_sha256':sha(folder/'oracle.npz'),'raw_sha256':sha(folder/'raw.npz'),'folder':str(folder)}
                save(folder/'receipt.json',result); return result
            results.append(measured(server,folder/'isolation',collect))
    finally: adapter.predicate=old_predicate
    return results
