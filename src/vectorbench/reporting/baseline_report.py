"""Raw-query paired analysis; report completion requires every declared native result."""
import hashlib
from pathlib import Path
import numpy as np

from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases
from vectorbench.evidence.contracts import load,save,sha


def query_scores(point,k):
    raw=Path(point["selected_read"])/"raw.npz"
    control=Path(point["oracle"]["path"])
    if sha(raw)!=point["raw_sha256"] or sha(control)!=point["oracle"]["sha256"]:
        raise AssertionError("analysis raw response/oracle changed")
    with np.load(raw) as data:
        returned=data["returned"]; lengths=data["returned_lengths"]
    with np.load(control) as data: truth=data["truth"]
    if len(returned)!=len(truth) or np.any(lengths<0) or np.any(lengths>100):
        raise AssertionError("analysis response length mismatch")
    values=[]
    for expected,row,length in zip(truth,returned,lengths):
        target=set(map(int,expected[:k])); got=set(map(int,row[:min(k,int(length))]))
        values.append(len(target & got)/len(target))
    scores=np.asarray(values,dtype=np.float64)
    if not np.isclose(scores.mean(),point[f"recall{k}"],rtol=0,atol=1e-12):
        raise AssertionError("raw per-query score differs from worker summary")
    return scores


def paired_interval(differences,key,draws=2000):
    values=np.asarray(differences,dtype=np.float64)
    if values.ndim!=1 or not len(values) or not np.isfinite(values).all():
        raise ValueError("paired bootstrap requires finite query differences")
    seed=int.from_bytes(hashlib.sha256(key.encode()).digest()[:8],"little")
    rng=np.random.default_rng(seed)
    means=[]
    for start in range(0,draws,100):
        indices=rng.integers(0,len(values),size=(min(100,draws-start),len(values)))
        means.extend(values[indices].mean(axis=1))
    return {"mean_pp":100*float(values.mean()),"lower_pp":100*float(np.percentile(means,2.5)),
            "upper_pp":100*float(np.percentile(means,97.5)),"queries":len(values),"draws":draws,
            "bootstrap_seed":seed,
            "scope":"conditional exploratory query bootstrap; no multiplicity adjustment or seed-level equivalence claim"}


def report(root,manifest,deps,context,out):
    history={k:v for k,v in deps.items() if k.startswith('history:')}
    rebuilt={k:v for k,v in deps.items() if k.startswith('fresh:')}
    if len(history)!=manifest['expected']['histories'] or len(rebuilt)!=manifest['expected']['fresh_points']:
        raise AssertionError('baseline report has incomplete histories/fresh controls')
    seed=context['prepared']['seeds'][str(manifest['seed'])]
    by_state={}; fresh_scores={}
    for key,value in rebuilt.items():
        spec=manifest['nodes'][key]; state=spec['state']; point=value['point']
        if value['state']!=state or point['ledger_sha256']!=state: raise AssertionError('fresh ledger differs')
        if point['status']!='PASS' or point['ann']['status']!='PASS': raise AssertionError('fresh ANN not verified')
        by_state.setdefault((spec['db'],state),[]).append((spec['repeat'],point))
    for (db,state),values in by_state.items():
        wanted=3 if seed['fresh_states'][state]['terminal'] else 1
        if sorted(v[0] for v in values)!=list(range(wanted)): raise AssertionError('fresh build repetitions differ')
        fresh_scores[db,state]={k:np.stack([query_scores(v[1],k) for v in sorted(values)],axis=0) for k in (10,100)}
    rows=[]; finals=[]; baseline_stability=[]; paired_final={}; incrementals=0
    for db in ('postgres','qdrant'):
        for identity,spec in cases().items():
            key='history:'+db+'-'+identity
            if key not in history: raise AssertionError('baseline declared recipe missing')
            actual=history[key]; path=seed['paths'][identity]
            if actual['steps']!=path['observed_steps'] or len(actual['points'])!=spec['waves']+1 or len(actual['writes'])!=spec['waves']:
                raise AssertionError('baseline history wave/checkpoint count differs')
            if actual['final_ledger_sha256']!=path['final_ledger_sha256']: raise AssertionError('history final version differs')
            for trace in actual['writes']:
                if spec['operation']=='read_only':
                    if trace['status']!='NO_WRITE_CONTROL' or trace['requests'] or trace['written'] or trace['deleted']:
                        raise AssertionError('read-only stability control actually wrote')
                elif trace['status']!='ACKNOWLEDGED': raise AssertionError('unreconciled write in report')
            group_file=Path(path['cohorts'])
            if sha(group_file)!=context['input_bindings'].get(str(group_file.resolve())):
                raise AssertionError('prepared cohort labels changed')
            with np.load(group_file) as data: cohorts={step:data[step] for step in data.files}
            first=None
            for step,point in zip(actual['steps'],actual['points']):
                incrementals+=1
                state=path['states'][str(step)]['ledger_sha256']
                if point['ledger_sha256']!=state or point['ann']['status']!='PASS': raise AssertionError('history state/ANN differs')
                fresh_values=by_state[db,state]
                if any(point[f]!=control[f] for _,control in fresh_values for f in ('vector_state_sha256','query_sha256','ledger_sha256')):
                    raise AssertionError('paired comparison changes data/query/version')
                scores={k:query_scores(point,k) for k in (10,100)}
                if len(scores[100])!=manifest['config']['query_rows']: raise AssertionError('evaluation query count differs')
                if first is None: first=scores[100]
                deltas={k:scores[k]-fresh_scores[db,state][k].mean(axis=0) for k in (10,100)}
                row={'dataset':manifest['dataset'],'seed':manifest['seed'],'db':db,'case':identity,'step':step,
                     'count':point['count'],'recall10':point['recall10'],'recall100':point['recall100'],
                     'fresh_repetitions':len(fresh_values),'fresh_mean100':float(fresh_scores[db,state][100].mean()),
                     'fresh_min100':float(fresh_scores[db,state][100].mean(axis=1).min()),
                     'fresh_max100':float(fresh_scores[db,state][100].mean(axis=1).max()),
                     'delta100':paired_interval(deltas[100],key+':'+str(step)+':100'),
                     'delta10':paired_interval(deltas[10],key+':'+str(step)+':10'),
                     'p50_ms':point['p50_ms'],'p95_ms':point['p95_ms'],'p99_ms':point['p99_ms'],
                     'shortfall100':point['shortfall100'],'cohorts':{}}
                codes=cohorts[str(step)]
                for code,name in enumerate(path['cohort_names']):
                    chosen=np.flatnonzero(codes==code)
                    row['cohorts'][name]={'queries':len(chosen),'recall100':float(scores[100][chosen].mean()) if len(chosen) else None,
                        'delta100':paired_interval(deltas[100][chosen],key+':'+str(step)+':'+name) if len(chosen) else None}
                if spec['operation']=='read_only':
                    row['no_write_delta100']=paired_interval(scores[100]-first,key+':readonly:'+str(step))
                    baseline_stability.append({k:row[k] for k in ('dataset','seed','db','step','recall100','no_write_delta100')})
                rows.append(row)
                if step==spec['waves']:
                    finals.append(row); paired_final[db,identity]=(state,scores[100])
    if incrementals!=manifest['expected']['incremental_points']: raise AssertionError('baseline checkpoint total differs')
    comparisons=[]
    for db in ('postgres','qdrant'):
        for identity,spec in cases().items():
            if spec['long'] or spec['waves']!=1: continue
            counterpart=identity[:-2]+'w5'
            a,b=paired_final[db,identity],paired_final[db,counterpart]
            if a[0]!=b[0]: raise AssertionError('one/five do not reach the identical final ledger')
            comparisons.append({'db':db,'one':identity,'five':counterpart,'delta_five_minus_one100':paired_interval(b[1]-a[1],db+':'+identity+':paired')})
    save(out/'checkpoints.json',rows); save(out/'finals.json',finals)
    save(out/'one-vs-five.json',comparisons); save(out/'read-only-stability.json',baseline_stability)
    body=['# 공개 벡터 기본 실험 조각','','범위: '+manifest['dataset']+' / seed '+str(manifest['seed']),
          '',f"이력 {len(history)}개·이력 checkpoint {incrementals}개·fresh {len(rebuilt)}개를 native 선행 조건과 원시 응답으로 검증했다.",
          '이 조각은 전체 연구 또는 세 seed 전체의 완료를 의미하지 않는다. 질의별 bootstrap은 탐색 분석이며 다중 비교 보정이나 동등성 주장이 아니다.','',
          '| DB | 경로 | Recall@100 | fresh 평균 | 차이(%p) | 95% query interval |',
          '|---|---|---:|---:|---:|---:|']
    for v in finals:
        delta=v['delta100']
        body.append(f"| {v['db']} | {v['case']} | {100*v['recall100']:.3f}% | {100*v['fresh_mean100']:.3f}% | {delta['mean_pp']:+.3f} | [{delta['lower_pp']:+.3f}, {delta['upper_pp']:+.3f}] |")
    (out/'report.md').write_text('\n'.join(body)+'\n')
    return {'scope':'UNIFORM_BASELINE_SHARD_ONLY','dataset':manifest['dataset'],'seed':manifest['seed'],
            'histories':len(history),'incremental_points':incrementals,'fresh_points':len(rebuilt),
            'report':str(out/'report.md'),'geometry_filters_online_recovery':'NOT RUN'}
