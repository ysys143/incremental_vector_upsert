"""Recompute every claimed recall from raw IDs, then analyze paired paths."""
from pathlib import Path
import csv
import hashlib
import os
import numpy as np
from vectorbench.evidence.contracts import load,save,sha
from vectorbench.experiments.pilot.model import score,unpacked_results
from .contract import validate_window


def raw_scores(point):
    if point['status']!='PASS' or point['ann']['status']!='PASS' or point['metric']!='ip':
        raise AssertionError('unqualified ANN point')
    raw_path=Path(point['selected_read'])/'raw.npz'
    if sha(raw_path)!=point['raw_sha256']:raise AssertionError('raw returned IDs changed')
    ref=point['oracle'];oracle_path=Path(ref['path'])
    if sha(oracle_path)!=ref['sha256']:raise AssertionError('oracle changed')
    with np.load(oracle_path) as data:
        truth=data['truth'];ids=data['ids']
    indices=np.asarray(ref.get('query_subset_indices',range(len(truth))),dtype=int)
    truth=truth[indices]
    with np.load(raw_path) as raw:rows=unpacked_results(raw)
    actual=score(ids,truth,rows)
    if len(rows)!=point['queries']:raise AssertionError('wrong query count')
    for key,value in actual.items():
        if abs(value-point[key])>1e-12:raise AssertionError('derived recall differs from raw evidence: '+key)
    arrays={k:np.asarray([len(set(map(int,t[:k]))&set(r[:k]))/min(k,len(ids)) for t,r in zip(truth,rows)]) for k in (10,100)}
    return arrays


def interval(values,repeats=2000,seed=1129,alpha=.05):
    values=np.asarray(values,dtype=float)
    if values.ndim!=1 or not len(values):raise ValueError('paired per-query vector required')
    rng=np.random.default_rng(seed);means=[]
    for start in range(0,repeats,256):
        take=min(256,repeats-start);draw=rng.integers(0,len(values),size=(take,len(values)))
        means.extend(values[draw].mean(axis=1))
    low,high=np.quantile(means,[alpha/2,1-alpha/2])
    return {'mean_pp':100*float(values.mean()),'lower_pp':100*float(low),'upper_pp':100*float(high),
            'queries':len(values),'bootstrap_repeats':repeats,'alpha':alpha,'seed':seed,
            'scope':'conditional on fixed data and built indexes; not product-level uncertainty'}


def write_csv(path,rows):
    if not rows:raise AssertionError('empty expected report table')
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def report(root,manifest,deps,out):
    validate_window(manifest)
    histories={k.split(':',1)[1]:v for k,v in deps.items() if k.startswith('history_slot')}
    rebuilds={k.split(':',1)[1]:v for k,v in deps.items() if k.startswith('fresh:')}
    if set(histories)!={h['id'] for h in manifest['matrix']['histories']} or set(rebuilds)!={f['id'] for f in manifest['matrix']['fresh_builds']}:
        raise AssertionError('incomplete approved history/fresh inventory')
    cache={};conditions=[];allpoints=[]
    def read(p,scope,kind,ef):
        location=p['selected_read']
        if location in cache:raise AssertionError('one raw read counted more than once')
        scores=raw_scores(p);cache[location]=scores;allpoints.append(p)
        conditions.append({'scope':scope,'kind':kind,'ef':ef,'queries':p['queries'],'recall10':p['recall10'],
            'recall100':p['recall100'],'p50_ms':p['p50_ms'],'p95_ms':p['p95_ms'],'p99_ms':p['p99_ms'],
            'ledger_sha256':p['ledger_sha256'],'query_sha256':p['query_sha256'],'raw_path':location,'started_at':p['started_at'],'finished_at':p['finished_at'],'ann_status':p['ann']['status'],'shortfall10':p['shortfall10'],'shortfall100':p['shortfall100']})
        return scores
    fresh_scores={};fresh_objects={}
    for identity,v in rebuilds.items():
        group=(v['dataset'],v['db'],v['seed'],v['state']);tuning=deps['tuning:'+v['dataset']+'-'+v['db']]
        a=read(v['point'],identity,'fresh',tuning['ef']);fresh_scores.setdefault(group,[]).append(a)
        fresh_objects.setdefault(group,[]).append(v)
        if len(v['ef_points'])!=(2 if v['state']=='final' else 0):raise AssertionError('fresh ef inventory')
        for extra in v['ef_points']:read(extra['point'],identity,'fresh-extra',extra['ef'])
    trajectories=[];endpoints=[];endpoint_scores={};cohort_rows=[];ef_rows=[]
    for identity,v in histories.items():
        d,db,seed,path=v['dataset'],v['db'],v['seed'],v['path'];expected_steps=list(range(2 if path.endswith('1') else 6))
        if v['steps']!=expected_steps or len(v['writes'])!=len(expected_steps)-1:raise AssertionError('history wave/checkpoint count')
        if sum(w['written'] for w in v['writes'])!=10000 or any(w['status']!='ACKNOWLEDGED' for w in v['writes']):raise AssertionError('write count/ACK mismatch')
        tuning=deps['tuning:'+d+'-'+db];vectors=[]
        for step,p in zip(v['steps'],v['points']):
            scores=read(p,identity+':step'+str(step),'incremental',tuning['ef']);vectors.append(scores)
            state='initial' if step==0 else 'final' if step==v['steps'][-1] else path+':step'+str(step)
            controls=fresh_scores.get((d,db,seed,state))
            row={'id':identity,'step':step,'queries':p['queries'],'recall10':p['recall10'],'recall100':p['recall100'],
                 'matched_initial_delta10_pp':100*float((scores[10][:200]-vectors[0][10][:200]).mean()),
                 'history_gap10_pp':None,
                 'matched_initial_delta100_pp':100*float((scores[100][:200]-vectors[0][100][:200]).mean()),
                 'history_gap100_pp':None,'fresh_repeats':len(controls) if controls else 0,
                 'ledger_sha256':p['ledger_sha256'],'query_sha256':p['query_sha256']}
            if controls:
                fs=fresh_objects[(d,db,seed,state)]
                if any(f['ledger_sha256']!=p['ledger_sha256'] for f in fs):raise AssertionError('fresh live set differs')
                avg=np.mean([x[100][:len(scores[100])] for x in controls],axis=0)
                row['history_gap100_pp']=100*float((scores[100]-avg).mean())
                row['history_gap10_pp']=100*float((scores[10]-np.mean([x[10][:len(scores[10])] for x in controls],axis=0)).mean())
            trajectories.append(row)
        final=v['points'][-1];controls=fresh_scores[(d,db,seed,'final')]
        if len(controls)!=3:raise AssertionError('final fresh repeat count')
        if any(f['point']['query_sha256']!=final['query_sha256'] for f in fresh_objects[(d,db,seed,'final')]):raise AssertionError('paired query bytes differ')
        meanfresh={k:np.mean([x[k] for x in controls],axis=0) for k in (10,100)}
        gaps={k:interval(vectors[-1][k]-meanfresh[k],seed=seed+k) for k in (10,100)}
        endpoints.append({'id':identity,'dataset':d,'db':db,'seed':seed,'path':path,'recall10':final['recall10'],'recall100':final['recall100'],
                          'fresh_mean100':float(meanfresh[100].mean()),'fresh_range100':[float(x[100].mean()) for x in controls],
                          'history_gap10':gaps[10],'history_gap100':gaps[100],'initial_snapshot_sha256':v['initial_snapshot_sha256']})
        endpoint_scores[(d,db,seed,path)]=vectors[-1]
        prepared=deps['inputs:'+d];info=prepared['seeds'][str(seed)]
        with np.load(info['cohort_codes']) as cq:
            masks={name:cq['origin']==i for i,name in enumerate(('old','new','boundary'))}
            masks.update({f'cluster_{i}':cq['dominant_cluster']==i for i in range(5)})
            masks['mixed']=cq['mixed'].astype(bool);masks['no_new_neighbor']=cq['dominant_cluster']<0
        for label,mask in masks.items():
            n=int(mask.sum());a=vectors[-1][100][mask];b=meanfresh[100][mask]
            cohort_rows.append({'id':identity,'cohort':label,'n':n,'recall100':float(a.mean()) if n else None,
                                'fresh100':float(b.mean()) if n else None,'gap_pp':100*float((a-b).mean()) if n else None,
                                'inference_eligible':n>=30,'recall10':float(vectors[-1][10][mask].mean()) if n else None,
                                'gap10_pp':100*float((vectors[-1][10][mask]-meanfresh[10][mask]).mean()) if n else None,
                                'conditional_ci95':interval(a-b,seed=seed) if n>=30 else None})
        if len(v['ef_points'])!=2:raise AssertionError('history extra ef inventory')
        for extra in v['ef_points']:
            scores=read(extra['point'],identity,'incremental-extra',extra['ef'])
            fs=[next(x['point'] for x in f['ef_points'] if x['ef']==extra['ef']) for f in fresh_objects[(d,db,seed,'final')]]
            if any(x['ledger_sha256']!=extra['point']['ledger_sha256'] or x['query_sha256']!=extra['point']['query_sha256'] for x in fs):raise AssertionError('ef contrast is not paired')
            fv=np.mean([cache[x['selected_read']][100] for x in fs],axis=0)
            ef_rows.append({'id':identity,'ef':extra['ef'],'recall100':extra['point']['recall100'],
                            'fresh100':float(fv.mean()),'history_gap100':interval(scores[100]-fv,seed=seed+extra['ef'])})
    contrasts=[]
    for d in ('msmarco','wikipedia'):
        for db in ('postgres','qdrant'):
            for seed in (11,29,47):
                values={p:endpoint_scores[(d,db,seed,p)][100] for p in ('M1','M5','C1','C5')}
                hs=[histories[f'{d}-{db}-s{seed}-{p}'] for p in values]
                if len({x['initial_snapshot_sha256'] for x in hs})!=1 or len({x['final_ledger_sha256'] for x in hs})!=1:raise AssertionError('paired initial/final identity mismatch')
                comparisons={'C5-M5':values['C5']-values['M5'],'C1-M1':values['C1']-values['M1'],
                             'C5-C1':values['C5']-values['C1'],'M5-M1':values['M5']-values['M1'],
                             'interaction':(values['C5']-values['C1'])-(values['M5']-values['M1'])}
                for name,delta in comparisons.items():
                    value={'dataset':d,'db':db,'seed':seed,'contrast':name,'ci95':interval(delta,seed=seed)}
                    if name in {'C5-M5','interaction'}:
                        value['familywise_ci']=interval(delta,100000,seed=90000+seed,alpha=.05/24)
                    contrasts.append(value)
    counts={'histories':len(histories),'incremental_measurements':sum(len(v['points']) for v in histories.values()),
            'fresh_builds':len(rebuilds),'additional_ef_measurements':len(conditions)-192-120,
            'total_quality_conditions':len(conditions),'evaluation_queries':sum(v['queries'] for v in conditions)}
    for key,value in counts.items():
        if value!=manifest['expected'][key]:raise AssertionError('completion count differs: '+key)
    from .diagnostics import resources
    resource_summary=resources(root,out)
    signals=[]
    for d in ('msmarco','wikipedia'):
        for db in ('postgres','qdrant'):
            hits=[]
            for seed in (11,29,47):
                gap=next(e['history_gap100']['mean_pp'] for e in endpoints if (e['dataset'],e['db'],e['seed'],e['path'])==(d,db,seed,'C5'))
                contrast=next(e['ci95']['mean_pp'] for e in contrasts if (e['dataset'],e['db'],e['seed'],e['contrast'])==(d,db,seed,'C5-M5'))
                if gap<=-.5 and contrast<=-.5:hits.append(seed)
            signals.append({'dataset':d,'db':db,'seeds':hits,'investigate':len(hits)>=2,'scope':'preregistered diagnostic signal, not significance'})
    save(out/'investigation-signals.json',signals)
    costs=[{'id':k,'setup':v.get('setup'),'writes':v.get('writes'), 'elapsed_s':v['elapsed_s'], 'settle':[{'wait':load(Path(p['selected_read'])/'wait.json'),'point':p['selected_read']} for p in v.get('points',[v.get('point')]) if p]} for k,v in {**histories,**rebuilds}.items()]
    save(out/'operation-costs.json',costs)
    save(out/'completion.json',{'status':'PASS','counts':counts,'gate':'G4','scope':'CLUSTERED_INSERTION_ONLY; final G5 delivery pending'})
    save(out/'endpoints.json',endpoints);save(out/'contrasts.json',contrasts);save(out/'ef-curves.json',ef_rows)
    save(out/'trajectories.json',trajectories);save(out/'cohorts.json',cohort_rows)
    write_csv(out/'conditions.csv',conditions);write_csv(out/'trajectories.csv',trajectories);write_csv(out/'cohorts.csv',cohort_rows)
    save(out/'prepared-diagnostics.json',{d:{'seeds':{s:{k:v for k,v in info.items() if k in ('diagnostics','cohort_counts','wave_sizes','cluster_diagnostics')} for s,info in deps['inputs:'+d]['seeds'].items()},'sample_overlap':deps['inputs:'+d].get('sample_overlap')} for d in ('msmarco','wikipedia')})
    lines=['# 공개 벡터 클러스터별 삽입 결과','',
           '48이력·120fresh·480검색 조건·211,200평가 질의의 raw 반환 ID와 oracle를 대조했다.',
           '이 파일은 native 측정·분석 산출물이며 최종 정리·제출 G5는 별도 receipt로 확인한다.','',
           '## 고정 운영 설정','', '| 데이터 | DB | M | efConstruction | ef |','|---|---|---:|---:|---:|']
    for d in ('msmarco','wikipedia'):
        for db in ('postgres','qdrant'):
            t=deps['tuning:'+d+'-'+db];lines.append(f"| {d} | {db} | {t['build'][0]} | {t['build'][1]} | {t['ef']} |")
    lines += ['', '## 최종 순서 효과와 상호작용','', '| 데이터 | DB | seed | 대비 | 차이 (%p) | 95% 질의 구간 |','|---|---|---:|---|---:|---|']
    for v in contrasts:
        if v['contrast'] in ('C5-M5','interaction'):
            q=v['ci95'];lines.append(f"| {v['dataset']} | {v['db']} | {v['seed']} | {v['contrast']} | {q['mean_pp']:+.3f} | [{q['lower_pp']:+.3f}, {q['upper_pp']:+.3f}] |")
    lines += ['', '## 클러스터별 5회 삽입과 fresh','', '| 데이터 | DB | seed | C5 R@100 (%) | fresh (%) | 차이 (%p) |','|---|---|---:|---:|---:|---:|']
    for v in endpoints:
        if v['path']=='C5':lines.append(f"| {v['dataset']} | {v['db']} | {v['seed']} | {100*v['recall100']:.3f} | {100*v['fresh_mean100']:.3f} | {v['history_gap100']['mean_pp']:+.3f} |")
    lines += ['', '## 근거와 해석 범위','',
              '- [자원 계측](resource-summary.json), [구축·쓰기 비용](operation-costs.json), [사전 조사 신호](investigation-signals.json).',
              '- RSS는 worker OS 최대값과 DB 프로세스별 RSS 합의 주기 표본을 구분한다. DB 합은 공유 페이지를 중복 계산할 수 있다.',
              '- [전체 endpoint](endpoints.json), [대응 대비와 24개 family 보정 구간](contrasts.json), [ef 곡선](ef-curves.json).',
              '- [현재 데이터 시점별 비교](trajectories.csv), [집단별 표본 수와 차이](cohorts.csv), [검색 조건 전수](conditions.csv).',
              '- 질의 bootstrap은 고정 입력과 구축에 조건부다. 세 재표본 seed는 같은 공개 pool을 공유한다.',
              '- 단순 시간 변화와 같은 상태 fresh 차이를 분리했다. 중간 fresh는 1회이므로 구축 변동의 확정 판정이 아니다.',
              '- 클러스터는 정규화한 복사본에서 만들었고 DB는 원본 IP로 검색했다. 실제 HNSW edge를 관측한 결과가 아니다.',
              '- 한 번/5회 차이에는 commit·중간 확인·optimizer 대기가 포함된다. 효과가 0 근처라고 동등성을 증명하지 않는다.',
              '- 지연은 공유 로컬 환경의 참고치다. 제품 보편적 순위·클라우드 대규모 성능으로 일반화하지 않는다.','']
    (out/'report.md').write_text('\n'.join(lines))
    return {'status':'PASS','counts':counts,'report':str(out/'report.md'),'endpoints':str(out/'endpoints.json'),
            'contrasts':str(out/'contrasts.json'),'finished_at':__import__('time').time(),'scope':'CLUSTERED_INSERTION_MEASUREMENTS; G5 pending'}
