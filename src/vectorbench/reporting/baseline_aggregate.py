"""Native-complete six-shard descriptive summary; advanced study remains incomplete."""
from pathlib import Path
import dagster as dg
from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases
from vectorbench.evidence.contracts import load,save
from vectorbench.orchestration.engine import native_receipt


def aggregate(roots,out):
    rows=[]; references=[]; total_points=0
    for root in map(Path,roots):
        manifest=load(root/'manifest.json')
        if manifest['mode']!='real-baseline-run' or manifest['config']['query_rows']!=600:
            raise AssertionError('aggregate input is not the declared baseline')
        if load(root/'controller.json')['status']!='COMPLETE_SELECTED_SCOPE': raise AssertionError('shard controller incomplete')
        with dg.DagsterInstance.from_config(str(root.resolve()/'dagster-home')) as instance:
            receipt=native_receipt(root,'report:all',instance)
            checks=[e.asset_check_evaluation for e in instance.all_logs(receipt['run_id']) if e.asset_check_evaluation]
            if len(checks)!=1 or not checks[0].passed: raise AssertionError('shard report check missing')
        result=receipt['result']
        if result['histories']!=58 or result['incremental_points']!=702:
            raise AssertionError('shard numerical completion criteria differ')
        folder=Path(result['report']).parent
        values=load(folder/'finals.json')
        if len(values)!=58: raise AssertionError('shard final case missing')
        rows.extend(values); references.append({'root':str(root),'native_run':receipt['run_id'],'result':result})
        total_points+=702+result['fresh_points']
    expected={(ds,db,seed,name) for ds in ('msmarco','wikipedia') for db in ('postgres','qdrant') for seed in (11,29,47) for name in cases()}
    actual={(v['dataset'],v['db'],v['seed'],v['case']) for v in rows}
    if actual!=expected or len(rows)!=348 or len(references)!=6:
        raise AssertionError('six-shard baseline has missing or duplicate declared cases')
    grouped=[]
    for ds in ('msmarco','wikipedia'):
        for db in ('postgres','qdrant'):
            for name in cases():
                values=[v for v in rows if v['dataset']==ds and v['db']==db and v['case']==name]
                delta=[v['delta100']['mean_pp'] for v in values]
                grouped.append({'dataset':ds,'db':db,'case':name,'seeds':3,'mean_final_recall100':sum(v['recall100'] for v in values)/3,
                                'mean_fresh_gap100_pp':sum(delta)/3,'seed_gap_range_pp':[min(delta),max(delta)]})
    save(out/'baseline-final-rows.json',rows); save(out/'baseline-seed-summary.json',grouped)
    text=['# 공개 벡터 기본 실험: 세 seed 집계','',
          f'기본 이력 348개·이력 checkpoint 4,212개를 검증했다. fresh를 포함한 품질 측정은 {total_points}개다.',
          '기하·순서·연속 쓰기·유지보수/회복·온라인·재튜닝·필터는 미완이며 전체 연구 완료가 아니다.',
          '아래 값은 세 seed의 기술 통계다. 동일 공개 코퍼스와 질의를 공유하므로 독립 코퍼스 3개 또는 동등성 증명으로 해석하지 않는다.','',
          '| 데이터셋 | DB | 조건 | 평균 Recall@100 | 평균 fresh 차이(%p) | seed 범위(%p) |','|---|---|---|---:|---:|---:|']
    for v in grouped:
        lo,hi=v['seed_gap_range_pp']
        text.append(f"| {v['dataset']} | {v['db']} | {v['case']} | {100*v['mean_final_recall100']:.3f}% | {v['mean_fresh_gap100_pp']:+.3f} | [{lo:+.3f},{hi:+.3f}] |")
    (out/'baseline-report.md').write_text('\n'.join(text)+'\n')
    result={'scope':'UNIFORM_BASELINE_ONLY','histories':348,'incremental_points':4212,'quality_points':total_points,
            'native_shards':references,'advanced_study':'NOT RUN','report':str(out/'baseline-report.md')}
    save(out/'baseline-result.json',result)
    return result
