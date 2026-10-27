"""Audit every declared history and raw response before reporting paired comparisons."""
from pathlib import Path
import time
import numpy as np
from vectorbench.evidence.contracts import load,save,sha
from vectorbench.reporting.baseline_report import paired_interval
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


def scores(point,k):
    raw=Path(point['selected_read'])/'raw.npz'; oracle=Path(point['oracle']['path'])
    if sha(raw)!=point['raw_sha256'] or sha(oracle)!=point['oracle']['sha256']:
        raise AssertionError('analysis raw/oracle hash changed')
    with np.load(raw) as f: returned=f['returned'];lengths=f['returned_lengths']
    with np.load(oracle) as f: truth=f['truth']
    if 'query_subset_indices' in point['oracle']: truth=truth[np.array(point['oracle']['query_subset_indices'])]
    if len(returned)!=len(truth) or len(truth)!=point['queries']: raise AssertionError('wrong query subset/oracle mapping')
    values=np.array([len(set(map(int,t[:k])) & set(map(int,r[:min(k,int(n))])))/len(t[:k]) for t,r,n in zip(truth,returned,lengths)])
    if not np.isclose(values.mean(),point['recall'+str(k)],atol=1e-12,rtol=0): raise AssertionError('raw recall differs')
    if point['status']!='PASS' or point['ann']['status']!='PASS': raise AssertionError('unverified ANN point')
    return values


def report(root,manifest,deps,out):
    auth=validate_window(manifest); matrix=manifest['matrix']
    histories={k.split(':',1)[1]:v for k,v in deps.items() if manifest['nodes'][k]['action']=='history'}
    rebuilt={k.split(':',1)[1]:v for k,v in deps.items() if manifest['nodes'][k]['action']=='fresh'}
    if set(histories)!={h['id'] for h in matrix['histories']} or len(rebuilt)!=138: raise AssertionError('missing declared histories/fresh')
    groups={}; total_points=0; total_queries=0; filter_points=[]; curves=[]
    def extra(values,scope):
        nonlocal total_queries
        for v in values.get('filters',[]):
            p=Path(v['folder'])
            if sha(p/'raw.npz')!=v['raw_sha256'] or sha(p/'oracle.npz')!=v['oracle_sha256']: raise AssertionError('filtered raw changed')
            with np.load(p/'raw.npz') as f: returned=f['returned'];lengths=f['returned_lengths']
            with np.load(p/'oracle.npz') as f: truth=f['truth'];eligible=f['ids']
            if len(returned)!=200 or v['queries']!=200: raise AssertionError('filter query count differs')
            for k in (10,100):
                actual=[len(set(map(int,t[:k])) & set(map(int,r[:min(k,int(n))])))/len(t[:k]) for t,r,n in zip(truth,returned,lengths)] if len(eligible) else []
                if actual and not np.isclose(np.mean(actual),v['recall'+str(k)],atol=1e-12,rtol=0): raise AssertionError('filtered raw recall differs')
            if not len(eligible) and np.any(lengths): raise AssertionError('empty filter returned data')
            filter_points.append({'scope':scope,**v});total_queries+=200
        for v in values.get('ef_curves',[]):
            scores(v['point'],10);scores(v['point'],100)
            if v['point']['queries']!=200: raise AssertionError('curve query count differs')
            curves.append({'scope':scope,**v});total_queries+=200
    for group in matrix['fresh_groups']:
        values=[]
        for repeat in range(3):
            key=group['id']+'-r'+str(repeat)
            if key not in rebuilt: raise AssertionError('missing independent fresh build')
            v=rebuilt[key];p=v['point']
            if v['repeat']!=repeat or p['queries']!=600: raise AssertionError('fresh repetition/query count differs')
            if p['ledger_sha256']!=v['state']: raise AssertionError('fresh ledger differs')
            values.append({k:scores(p,k) for k in (10,100)});total_points+=1;total_queries+=600
            extra(v['extra_probes'],key)
        first=rebuilt[group['id']+'-r0']
        scope=(first['dataset'],first['seed'],first['db'],first['family'],first['state'])
        if scope in groups: raise AssertionError('duplicate fresh group')
        groups[scope]={k:np.stack([v[k] for v in values]) for k in (10,100)}
    finals=[]; trajectories=[]; final_scores={}
    for spec in matrix['histories']:
        name=spec['id'];v=histories[name]
        if v['steps']!=spec['quality_steps'] or len(v['writes'])!=spec['waves']: raise AssertionError('history schedule differs')
        if len(v['unmeasured_wave_verifications'])!=spec['waves']+1-len(spec['quality_steps']): raise AssertionError('unmeasured wave validation missing')
        for write in v['writes']:
            if write['status'] not in {'ACKNOWLEDGED','NO_WRITE_CONTROL'}: raise AssertionError('unacknowledged write')
            if any(r['status']!='ACKNOWLEDGED' for r in write['requests']): raise AssertionError('partial write acknowledged as complete')
        subset=np.array(deps['study_context:'+spec['dataset']]['subset_indices']); initial=None
        for step,p in zip(v['steps'],v['points']):
            if p['queries']!=spec['queries_by_step'][str(step)]: raise AssertionError('history query count differs')
            expected=v['path']['states'][str(step)]['ledger_sha256']
            if p['ledger_sha256']!=expected: raise AssertionError('checkpoint ledger differs')
            values={k:scores(p,k) for k in (10,100)}
            matched={k:val[subset] if p['queries']==600 else val for k,val in values.items()}
            if initial is None: initial=matched
            row={'id':name,'step':step,'dataset':spec['dataset'],'db':spec['db'],'seed':spec['seed'],'family':spec['family'],
                 'count':p['count'],'queries':p['queries'],'recall10':p['recall10'],'recall100':p['recall100'],
                 'p95_ms':p['p95_ms'],'shortfall100':p['shortfall100'],'groups':p['groups'],
                 'matched_initial_delta100':paired_interval(matched[100]-initial[100],name+':'+str(step))}
            trajectories.append(row);total_points+=1;total_queries+=p['queries']
            if step==spec['waves']:
                scope=(spec['dataset'],spec['seed'],spec['db'],spec['family'],p['ledger_sha256'])
                fresh=groups[scope]
                row={**row,'fresh_mean100':float(fresh[100].mean()),'fresh_repeat_range100':[float(x.mean()) for x in fresh[100]],
                     'fresh_delta10':paired_interval(values[10]-fresh[10].mean(axis=0),name+':fresh10'),
                     'fresh_delta100':paired_interval(values[100]-fresh[100].mean(axis=0),name+':fresh100')}
                finals.append(row);final_scores[name]=values
            extra(v['extra_probes'].get(str(step),{}),name+':step'+str(step))
    if (len(histories),total_points,len(filter_points),len(curves),total_queries)!=(66,454,64,24,190000):
        raise AssertionError('approved numerical completion criteria differ')
    comparisons=[]
    for spec in matrix['histories']:
        if spec.get('source_case','').endswith('-w1'):
            one=spec['id'];five=one[:-2]+'w5'
            if five not in histories: raise AssertionError('paired batch history missing')
            if histories[one]['final_ledger_sha256']!=histories[five]['final_ledger_sha256'] or histories[one]['initial_snapshot_sha256']!=histories[five]['initial_snapshot_sha256']:
                raise AssertionError('batch split does not share initial graph and final data')
            comparisons.append({'one':one,'five':five,'delta100':paired_interval(final_scores[five][100]-final_scores[one][100],one+':five-minus-one')})
    for name,value in [('finals',finals),('trajectories',trajectories),('batch-comparisons',comparisons),('filters',filter_points),('ef-curves',curves)]: save(out/(name+'.json'),value)
    body=['# 12시간 공개 벡터 증분 갱신 실험 결과','',
          '두 공개 임베딩과 pgvector/Qdrant에서 승인된 66개 이력, 138회 fresh 구축, 542개 검색 조건을 검증했다.',
          '로컬 공유 호스트의 탐색 결과다. seed는 기본 11, 일부 MS MARCO 조건만 29다. 질의 bootstrap 구간은 코퍼스/seed 반복의 대체물이 아니다.',
          '기존 중단 이력은 포함하지 않았다. 시간 변화는 공통 200질의, 최종 inc–fresh는 600질의를 비교했다.','',
          '| 이력 | 최종 R@100 | fresh 평균 | 차이 (%p) | 95% 질의 bootstrap |','|---|---:|---:|---:|---:|']
    for row in finals:
        d=row['fresh_delta100'];body.append(f"| {row['id']} | {100*row['recall100']:.3f}% | {100*row['fresh_mean100']:.3f}% | {d['mean_pp']:+.3f} | [{d['lower_pp']:+.3f}, {d['upper_pp']:+.3f}] |")
    body+=['','필터 결과는 filters.json에 실제 ANN/plain/정확 검색 경로와 함께 기록했다. 빈 후보의 Recall은 N/A다.',
           'batch-comparisons.json은 같은 초기 그래프·최종 원장을 가진 한 번/5회 차이다. trajectories.json은 집단별 Recall과 공통 질의의 시간 변화를 포함한다.',
           '전체 연구의 공간순 삽입·온라인 혼합·회복·전체 요인 교차와 추가 seed는 이번 승인 범위 밖의 미완 항목으로 남는다.']
    (out/'report.md').write_text('\n'.join(body)+'\n')
    finished=time.time()
    if finished>auth['deadline']: raise TimeoutError('report exceeded original twelve-hour deadline')
    result={'status':'PASS','scope':'APPROVED_12H_PLAN_ONLY','histories':66,'history_points':316,'fresh_builds':138,
            'filter_probes':64,'ef_probes':24,'quality_points':542,'queries':190000,'finished_at':finished,
            'elapsed_seconds':finished-auth['t0'],'report':str(out/'report.md'),'remaining_long_term_research':'INCOMPLETE'}
    save(out/'completion.json',result);return result
