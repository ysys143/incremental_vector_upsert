"""Reuse native baseline inputs and prepare the approved real-vector extensions."""
from pathlib import Path
import json
import subprocess
import numpy as np
from vectorbench.evidence.contracts import Config, load, save, sha
from vectorbench.experiments.real_vectors.actions import pool_from
from vectorbench.experiments.real_vectors.baseline.baseline_data import split, cohorts
from vectorbench.evidence.checkpoints import Checkpoints
from vectorbench.workloads.events import Event, Live, Write
from vectorbench.datasets.storage import load_state, load_events


def native(manifest, node):
    p=subprocess.run([manifest['orchestration_python'],'-m','vectorbench.experiments.real_vectors.baseline.baseline_native',manifest['setup_root'],node],capture_output=True,text=True,timeout=180)
    if p.returncode: raise AssertionError('native setup qualification failed: '+p.stderr[-1000:])
    return json.loads(p.stdout)


def context(manifest, dataset, out):
    ready=native(manifest,'setup_report:all')
    if ready['result']['calibrations']!=4: raise AssertionError('setup incomplete')
    value=native(manifest,'inputs:'+dataset); ref=value['reference']
    receipt=Path(ref['root'])/'cells'/ref['node'].replace(':','__')/ref['run_id']/'receipt.json'
    if sha(receipt)!=ref['receipt_sha256']: raise AssertionError('input receipt changed')
    prepared=value['result']
    bindings={str((Path(ref['root'])/name).resolve()):digest for name,digest in load(receipt)['artifacts'].items()}
    bindings.update(prepared.get('qualified_input_bindings',{}))
    for name,digest in bindings.items():
        if sha(name)!=digest: raise AssertionError('native input changed: '+name)
    tunings={}
    for db in ('postgres','qdrant'):
        prior=native(manifest,'tuning:'+dataset+'-'+db)
        setting=manifest['matrix']['settings_to_revalidate'][dataset+'-'+db]
        candidates=[v for v in prior['result']['candidates'] if v.get('build')==[setting['m'],setting['ef_construction']] and v.get('ef')==setting['ef']]
        if len(candidates)!=1 or min(candidates[0]['min_recall10'],candidates[0]['min_recall100'])<.99 or candidates[0]['shortfall']:
            raise AssertionError('declared tuning not quality-qualified')
        tunings[db]={'build':candidates[0]['build'],'ef':setting['ef'],'initial_batch':prior['result']['initial_batch'],
                     'qualification':prior['reference'],'candidate':candidates[0]}
    subset=np.sort(np.random.default_rng(20261026).choice(600,200,replace=False))
    result={'prepared':prepared,'input_bindings':bindings,'tunings':tunings,'subset_indices':subset.tolist(),
            'native_reference':ref,'dataset':dataset}
    pool=pool_from(prepared) # revalidate official source bytes as part of native CPU preparation
    if dataset=='msmarco': result['extensions']=extensions(pool,prepared,manifest,out/'extensions')
    save(out/'context.json',result)
    return result


def prepare_path(cache,start,events,out,pool,query_rows):
    live=Live(start,len(pool.vectors))
    for e in events: live.apply(e)
    path=cache.path(start,{'events':events,'observed_steps':list(range(len(events)+1)),'final':live,
                        'counts':{'waves':len(events),'final_count':len(live.current)}},out)
    codes={}; current=Live(start,len(pool.vectors)); names=None
    with np.load(path['controls'][0]['path']) as f: first=f['truth']
    for step in range(len(events)+1):
        if step: current.apply(events[step-1])
        with np.load(path['controls'][step]['path']) as f: truth=f['truth']
        codes[str(step)],names=cohorts(events[0].operation,current,first,truth,len(start))
    np.savez_compressed(out/'cohorts.npz',**codes)
    path.update(cohorts=str(out/'cohorts.npz'),cohort_names=names)
    return path


def extensions(pool,prepared,manifest,out):
    out.mkdir(parents=True)
    c=Config(**manifest['config']); selected=split(pool,c)
    initial=load_state(prepared['seeds']['11']['initial'],len(pool.vectors))
    records=initial.records(); rows=np.array([w.row for w in records]); ids=np.array([w.id for w in records])
    with np.load(prepared['query_rows']) as q: queries=q['evaluation']
    cache=Checkpoints(pool,queries,out/'checkpoints',c)
    anchors=np.random.default_rng(110512).choice(rows,512,replace=False)
    candidates=selected['evaluation_documents'][~np.isin(selected['evaluation_documents'],rows)]
    similarities=np.empty(len(candidates))
    for offset in range(0,len(candidates),1024):
        similarities[offset:offset+1024]=(np.asarray(pool.vectors[candidates[offset:offset+1024]],dtype=np.float64) @ np.asarray(pool.vectors[anchors],dtype=np.float64).T).max(axis=1)
    order=np.lexsort((candidates,similarities)); paths={}; diagnostics={}
    for label,chosen in [('near',order[-10000:]),('far',order[:10000])]:
        incoming=np.random.default_rng(1105).permutation(candidates[chosen]); events=[]
        for wave in range(5):
            writes=tuple(Write(10000+i,int(row),0,int(row)%10,int(row)) for i,row in enumerate(incoming[wave*2000:(wave+1)*2000],wave*2000))
            events.append(Event(writes=writes))
        paths[label]=prepare_path(cache,records,events,out/label,pool,queries)
        # Exact sampled cross-neighbor fraction against the union, independent of ANN.
        sample=np.random.default_rng(1140).choice(incoming,100,replace=False)
        from vectorbench.evaluation.metrics import exact
        combined=np.concatenate([rows,incoming]); union_ids=np.arange(len(combined))
        neighbors=exact(union_ids,pool.vectors[combined],pool.vectors[sample],k=11,metric='ip')
        # Self may not be the maximum IP neighbor; explicitly remove its row identity.
        cross=[]
        for src,nn in zip(sample,neighbors):
            nn=[i for i in nn if combined[i]!=src][:10]; cross.append(sum(i<10000 for i in nn)/len(nn))
        diagnostics[label]={'max_ip_quantiles':np.quantile(similarities[chosen],[0,.25,.5,.75,1]).tolist(),
            'norm_quantiles':np.quantile(np.linalg.norm(pool.vectors[incoming],axis=1),[0,.25,.5,.75,1]).tolist(),
            'sample_exact_old_neighbor_fraction':float(np.mean(cross)),'sample_count':100}
    # Separate filter graph, with the same vector history and immutable ID labels.
    basepath=prepared['seeds']['11']['paths']['upsert-r50-w5']
    paths['filter']=basepath
    rng=np.random.default_rng(1117); rank=np.empty(10000,dtype=np.int64); rank[rng.permutation(10000)]=np.arange(10000)
    spatial=np.asarray(pool.vectors[rows],dtype=np.float64) @ np.random.default_rng(1123).normal(size=768)
    spatial_rank=np.empty(10000,dtype=np.int64); spatial_rank[np.lexsort((ids,spatial))]=np.arange(10000)
    unique=np.empty(10000,dtype=np.int64); unique[rng.permutation(10000)]=np.arange(10000)
    head=np.where(rank<1000,0,1+(rank-1000)%999)
    labels={str(int(i)):{'cat10':int(rank[j]//1000),'cat1000':int(rank[j]//10),'spatial1000':int(spatial_rank[j]//10),
                         'head1000':int(head[j]),'unique_rank':int(unique[j]),'uniform_rank':int(rank[j])} for j,i in enumerate(ids)}
    save(out/'filter-labels.json',labels)
    np.savez_compressed(out/'geometry-selection.npz',anchors=anchors,candidates=candidates,max_ip=similarities)
    save(out/'geometry-diagnostics.json',diagnostics)
    return {'paths':paths,'filter_labels':str(out/'filter-labels.json'),'filter_labels_sha256':sha(out/'filter-labels.json'),
            'diagnostics':diagnostics,'initial':prepared['seeds']['11']['initial']}
