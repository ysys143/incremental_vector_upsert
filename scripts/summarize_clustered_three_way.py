"""Summarize retained clustered-run conditions; does not execute a database."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e'
OUTPUT = ROOT / 'docs/real-vectors/clustered'
ENDPOINTS = json.loads((SOURCE / 'endpoints.json').read_text())
CONTRASTS = json.loads((SOURCE / 'contrasts.json').read_text())
TRAJECTORIES = json.loads((SOURCE / 'trajectories.json').read_text())
with (SOURCE / 'conditions.csv').open(newline='') as f:
    CONDITIONS = list(csv.DictReader(f))
assert len(ENDPOINTS) == 48 and len(CONDITIONS) == 480 and len(TRAJECTORIES) == 192

def near(a, b):
    assert abs(a-b) < 1e-9, (a,b)

def pct(v): return f'{v:.4f}'
def pp(v): return f'{v:+.4f}'

def interval(g):
    return f'[{g["lower_pp"]:+.4f}, {g["upper_pp"]:+.4f}]'

rows=[]
for dataset in ('msmarco','wikipedia'):
    for db in ('postgres','qdrant'):
        for seed in (11,29,47):
            prefix=f'{dataset}-{db}-s{seed}'
            ends={x['path']:x for x in ENDPOINTS if x['dataset']==dataset and x['db']==db and x['seed']==seed}
            assert set(ends)=={'M1','M5','C1','C5'}
            fresh=[x for x in CONDITIONS if x['kind']=='fresh' and x['scope'].startswith(prefix+'-final-fresh-')]
            assert len(fresh)==3 and {x['scope'] for x in fresh}=={f'{prefix}-final-fresh-{i}' for i in range(3)}
            fresh100=sum(float(x['recall100']) for x in fresh)/3
            fresh10=sum(float(x['recall10']) for x in fresh)/3
            ledger={x['ledger_sha256'] for x in fresh};queries={x['query_sha256'] for x in fresh}
            assert len(ledger)==len(queries)==1 and all(int(x['queries'])==600 for x in fresh)
            record={'dataset':dataset,'db':db,'seed':seed,'fresh_recall100_pct':100*fresh100,'fresh_recall10_pct':100*fresh10}
            initial_snapshots=set()
            for path in ('M1','M5','C1','C5'):
                e=ends[path];step=1 if path.endswith('1') else 5
                found=[x for x in CONDITIONS if x['kind']=='incremental' and x['scope']==f'{prefix}-{path}:step{step}']
                assert len(found)==1
                x=found[0];assert {x['ledger_sha256']}==ledger and {x['query_sha256']}==queries and int(x['queries'])==600
                near(float(x['recall100']),e['recall100']);near(float(x['recall10']),e['recall10']);near(fresh100,e['fresh_mean100'])
                near(100*(float(x['recall100'])-fresh100),e['history_gap100']['mean_pp'])
                near(100*(float(x['recall10'])-fresh10),e['history_gap10']['mean_pp'])
                initial_snapshots.add(e['initial_snapshot_sha256'])
                point=next(y for y in TRAJECTORIES if y['id']==f'{prefix}-{path}' and y['step']==step)
                initial=next(y for y in TRAJECTORIES if y['id']==f'{prefix}-{path}' and y['step']==0)
                assert initial['queries']==200 and point['queries']==600
                record[path+'_recall100_pct']=100*e['recall100']
                record[path+'_gap100_pp']=e['history_gap100']['mean_pp']
                record[path+'_gap100_ci95_low_pp']=e['history_gap100']['lower_pp']
                record[path+'_gap100_ci95_high_pp']=e['history_gap100']['upper_pp']
                record[path+'_recall10_pct']=100*e['recall10']
                record[path+'_gap10_pp']=e['history_gap10']['mean_pp']
                record[path+'_initial_recall100_200_pct']=100*initial['recall100']
                record[path+'_final_recall100_200_pct']=100*initial['recall100']+point['matched_initial_delta100_pp']
                record[path+'_change_initial_200_pp']=point['matched_initial_delta100_pp']
            assert len(initial_snapshots)==1
            for order in ('M','C'):
                a=next(x for x in CONTRASTS if (x['dataset'],x['db'],x['seed'],x['contrast'])==(dataset,db,seed,order+'5-'+order+'1'))
                g=a['ci95'];d=record[order+'5_recall100_pct']-record[order+'1_recall100_pct'];near(d,g['mean_pp'])
                record[order+'_5minus1_pp']=d
                record[order+'_5minus1_ci95_low_pp']=g['lower_pp']
                record[order+'_5minus1_ci95_high_pp']=g['upper_pp']
            rows.append(record)
assert len(rows)==12
OUTPUT.mkdir(parents=True,exist_ok=True)
with (OUTPUT/'three-way-comparison.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');w.writeheader();w.writerows(rows)
name={'msmarco':'MS MARCO','wikipedia':'Wikipedia–Cohere','postgres':'pgvector','qdrant':'Qdrant'}
allchanges=[r[p+'_change_initial_200_pp'] for r in rows for p in ('M1','M5','C1','C5')]
allgaps=[r[p+'_gap100_pp'] for r in rows for p in ('M1','M5','C1','C5')]
assert len(allchanges)==48
lines=['# 재집계: 삽입 전후, 증분 삽입과 fresh 구축', '',
'실험: `clustered-20261027-02` · 재집계 대상: 기존 1만+1만 삽입 실험 · 새로운 DB 실행 없음', '',
'## 비교 방법', '',
'초기 1만 벡터의 인덱스에 신규 1만 벡터를 추가한 네 경로를 동일한 최종 2만 벡터로 구축한 fresh 인덱스와 비교한다. 각 데이터·DB·시드는 동일한 초기 스냅샷에서 분기한다. fresh 값은 같은 최종 데이터의 독립 구축 3회 평균이다.', '',
'| 기호 | 경로 |', '|---|---|', '| F | 최종 데이터로 별도 fresh 구축 3회의 평균 |', '| M1 / M5 | 같은 무작위 혼합 순서로 각각 1회 / 5회 논리적 배치 삽입 |', '| C1 / C5 | 같은 군집별 순서로 각각 1회 / 5회 논리적 배치 삽입 |', '',
'F는 동일 데이터의 별도 fresh 구축이다. 기존 DB에 삽입한 후 그 자리에서 인덱스를 재구축한 실행이나 그 총비용은 아니다. Qdrant는 사용자 지시로 전체 인덱스를 재구축하지 않은 경로에서도 백그라운드 최적화가 발생할 수 있다.', '',
'## 1. 최종 데이터의 fresh와 증분 인덱스 비교', '',
'모든 열은 최종 상태의 동일한 600개 질의에 대한 Recall@100이다. F는 %, 나머지는 각각 F를 뺀 차이(%p)다. 양수는 해당 증분 인덱스의 Recall이 fresh 평균보다 높은 경우다.', '',
'| 데이터 | DB | 시드 | F (%) | M1−F | M5−F | M5−M1 | C1−F | C5−F | C5−C1 |',
'|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:
 lines.append(f"| {name[r['dataset']]} | {name[r['db']]} | {r['seed']} | {pct(r['fresh_recall100_pct'])} | {pp(r['M1_gap100_pp'])} | {pp(r['M5_gap100_pp'])} | {pp(r['M_5minus1_pp'])} | {pp(r['C1_gap100_pp'])} | {pp(r['C5_gap100_pp'])} | {pp(r['C_5minus1_pp'])} |")
lines += ['',f'48개 증분 이력의 fresh 대비 차이는 {pp(min(allgaps))}∼{pp(max(allgaps))}%p였다. 이는 서로 다른 데이터·DB·시드·경로에서 나온 점 추정치의 범위다.', '',
'`M5−M1`과 `C5−C1`은 각 순서를 고정했을 때 논리적 배치 분할에 따른 차이다. 각 열의 12개 기존 **설명용 95% 구간은 모두 0을 포함**했다. 이 구간은 다중 비교 보정이 없고, 고정된 인덱스에 대해 질의를 재표본한 것이다. 0을 포함한다는 사실은 경로가 동등함을 증명하지 않는다. 각 시드의 gap 및 구간과 Recall@10은 [기계 판독용 CSV](three-way-comparison.csv)에 수록했다.', '',
'기존 사전 지정 24개 주 비교는 `C5−M5`와 상호작용이며, 위의 1회·5회 대비와 별개다. 그 주 비교 24개의 다중 비교 보정 구간이 모두 0을 포함했다는 원래 결과를 그대로 유지한다.', '',
'## 2. 같은 질의의 삽입 전후 변화', '',
'초기 1만 개에서 검색한 200개 질의를 최종 상태에서도 똑같이 사용해 Recall@100의 전후 차이를 계산했다. 초기와 최종의 정답 이웃은 각각의 현재 데이터에서 계산한다. 아래는 각 데이터·DB 조합의 세 시드에서 나온 범위(%p)다.', '',
'| 데이터 | DB | M1 전후 | M5 전후 | C1 전후 | C5 전후 |', '|---|---|---:|---:|---:|---:|']
for dataset in ('msmarco','wikipedia'):
 for db in ('postgres','qdrant'):
    group=[r for r in rows if r['dataset']==dataset and r['db']==db]
    values=[]
    for path in ('M1','M5','C1','C5'):
        vals=[r[path+'_change_initial_200_pp'] for r in group]
        values.append(f'{pp(min(vals))}∼{pp(max(vals))}')
    lines.append(f"| {name[dataset]} | {name[db]} | "+' | '.join(values)+' |')
lines += ['',f'전체 48개 이력 중 {sum(x<0 for x in allchanges)}개에서 관측된 전후 차이가 음수였으며, 범위는 {pp(min(allchanges))}∼{pp(max(allchanges))}%p였다. 이 차이에는 데이터가 1만에서 2만 개로 늘면서 정답 이웃과 검색 난도가 변한 효과가 포함된다. 따라서 이를 전부 인덱스 열화로 해석할 수 없다.', '',
'표 1의 최종 대조는 600개 질의이고 이 절의 전후 비교는 양쪽 모두 200개 질의다. 서로 다른 표의 숫자를 직접 빼서 새로운 효과로 해석하지 않는다.', '',
'## 3. 결과와 한계', '',
'- 기존 실험에는 최종 fresh, 1회 증분 삽입, 5회 증분 삽입 결과가 모두 있으며 혼합 순서와 군집 순서를 각각 대조할 수 있다.',
'- 같은 200개 질의의 실제 전후 Recall은 48개 이력 모두 감소했다. 같은 최종 데이터의 fresh와 비교하면 위의 작은 양수·음수 차이가 나타났다. 두 비교는 서로 다른 질문에 답한다.',
'- 검색 설정은 데이터·DB별로 보정했으며 ef가 다르다. 이는 공통 ef 또는 공통 Recall·비용에서의 제품 성능 순위가 아니다.',
'- 새 DB에서 구축한 fresh와 실제 삽입 후 같은 DB에서 재구축하는 작업은 비용과 구축 이력이 다를 수 있다. 실제 재구축 비용은 이 재집계로 구할 수 없다.',
'- 원본의 설명용 구간은 완성된 인덱스에 조건부다. 시드 간 문서가 일부 겹치고 평가 질의를 공유하며, 반복 구축·새 모집단의 불확실성을 모두 반영하지 않는다. 동등성 검정도 수행하지 않았다.',
'- 신규 ID 삽입만 측정했다. 삭제, 반복 churn, 동일 ID 벡터 교체, 필터 및 대규모 실험에 적용할 수 없다.', '',
'## 근거와 재현', '',
'- [원래 결과](results.md) · [기술보고서](technical-report.md) · [상세 CSV](three-way-comparison.csv)',
'- [원래 집계의 conditions.csv](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/conditions.csv), [endpoints.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/endpoints.json), [contrasts.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/contrasts.json), [trajectories.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/trajectories.json)',
'- `python3 scripts/summarize_clustered_three_way.py`는 기존 집계에서 12개 조합을 다시 계산하고 각 fresh 3회 및 네 경로의 최종 ledger·query 해시 일치, Recall·차이, 1회/5회 대비를 확인한다. 신뢰구간과 200개 질의 전후 차이는 기존 원시 분석 결과를 사용한다.',
'- 원시 검색 반환 ID는 외장 백업에 보관돼 있다. 이번 재집계는 이미 산출된 조건별 결과를 사용했으며 DB 재실행이나 원시 배열로부터 통계 구간 재계산을 수행하지 않았다.', '']
(OUTPUT/'three-way-comparison.md').write_text('\n'.join(lines))
print('PASS: 12 combinations; 36 final fresh conditions; 48 histories; 200-query changes; same ledger/query hashes')
print('fresh gap range (pp):',round(min(allgaps),4),round(max(allgaps),4))
print('matched 200-query before/after negative:',sum(x<0 for x in allchanges),'/',len(allchanges),'range:',round(min(allchanges),4),round(max(allchanges),4))
