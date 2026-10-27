# 로컬 삽입 품질 실험 결과

실행 `campaign-20261025-01`의 native 셀 541/541개와 최종 asset check가 통과했다.
경로 132개, 증분 시점 504개, fresh 측정 384개, 총 888개 품질 측정을 검증했다.
이번 로컬 삽입 연구의 완료이며, 보관된 전체 다중 DB·클라우드·필터·삭제/upsert 연구의 완료는 아니다.

## 주요 관측

- 전체 질의 기준 가장 낮은 평균 최종 history gap@100은 `qdrant/bridge/r100/w5/ordered/settled`의 -5.810 [-8.328, -4.446]%p다.
- 중간·최종 시점 전체에서 사전 -0.5%p 기준을 만족하고 fresh 최소값보다 낮은 결과가 2개 이상 seed에 나타난 조건은 38개다. 0개여도 동등성이 입증된 것은 아니다.
- 아래 평균과 대괄호 범위는 데이터 seed 11·29·47의 평균 [최솟값, 최댓값]이다. 신뢰구간이 아니다.

## 1. 삽입 전후 품질과 같은 데이터의 재구축

최대 삽입량(10K+2K×5), 무작위 순서, 매 wave 인덱싱·측정 대기 조건이다.
삽입 전 값은 미래 신규·경계 질의도 D0만 검색한 값이다. fresh는 각 최종 상태의 3회 재구축 평균을 seed별로 구한 뒤 집계했다.

| DB | 신규 geometry | 삽입 전 R@100 (%) | 삽입 후 R@100 (%) | fresh R@100 (%) | history gap (%p) |
|---|---|---:|---:|---:|---:|
| postgres | bridge | 97.267 [97.153, 97.432] | 98.195 [98.128, 98.278] | 98.201 [98.135, 98.295] | -0.006 [-0.044, 0.043] |
| postgres | far | 93.549 [92.895, 93.990] | 95.839 [95.335, 96.125] | 95.398 [94.885, 96.100] | 0.441 [-0.042, 1.240] |
| postgres | iid | 98.356 [98.317, 98.395] | 95.254 [95.235, 95.263] | 95.284 [95.227, 95.352] | -0.030 [-0.088, 0.037] |
| postgres | near | 98.248 [98.192, 98.295] | 95.688 [95.582, 95.768] | 95.710 [95.523, 95.881] | -0.022 [-0.113, 0.058] |
| qdrant | bridge | 97.426 [97.318, 97.510] | 98.203 [98.160, 98.268] | 98.269 [98.199, 98.360] | -0.066 [-0.092, -0.019] |
| qdrant | far | 93.548 [93.272, 93.933] | 96.064 [95.978, 96.197] | 95.941 [95.769, 96.235] | 0.123 [-0.217, 0.377] |
| qdrant | iid | 98.409 [98.363, 98.440] | 95.974 [95.855, 96.063] | 95.983 [95.871, 96.050] | -0.008 [-0.022, 0.013] |
| qdrant | near | 98.322 [98.270, 98.352] | 96.319 [96.118, 96.420] | 96.262 [96.021, 96.388] | 0.057 [0.032, 0.098] |

현재 데이터량 증가와 난도 변화는 fresh에도 영향을 준다. 삽입 전후 하락 전체를 갱신 이력의 손상으로 해석하지 않는다.
10% 삽입, Recall@10, 모든 집단·시점의 값은 아래 CSV 및 aggregates.json에 함께 남겼다.

## 2. 같은 최종 데이터: 한 번과 다섯 번

같은 초기 인덱스와 최종 ID·벡터 집합을 확인한 대응 비교다. 양수는 한 번 경로의 R@100이 더 높다는 뜻이다.
API 요청/커밋의 분할과 중간 대기를 포함한 경로 비교다. 제품 내부 ANN 구축 단위를 직접 제어한 결과는 아니다.

| DB | geometry | 신규/초기 (%) | 순서 | 한 번−다섯 번 R@100 (%p) |
|---|---|---:|---|---:|
| postgres | bridge | 10 | random | 0.003 [0.002, 0.005] |
| postgres | bridge | 100 | ordered | -0.002 [-0.028, 0.013] |
| postgres | bridge | 100 | random | 0.003 [0.000, 0.007] |
| postgres | far | 10 | random | -0.002 [-0.003, -0.002] |
| postgres | far | 100 | ordered | 0.002 [-0.013, 0.013] |
| postgres | far | 100 | random | 0.086 [0.007, 0.242] |
| postgres | iid | 10 | random | 0.002 [-0.002, 0.005] |
| postgres | iid | 100 | random | 0.006 [-0.005, 0.015] |
| postgres | near | 10 | random | -0.001 [-0.010, 0.013] |
| postgres | near | 100 | random | -0.006 [-0.022, 0.017] |
| qdrant | bridge | 10 | random | 0.016 [-0.020, 0.043] |
| qdrant | bridge | 100 | ordered | 5.764 [4.452, 8.260] |
| qdrant | bridge | 100 | random | 0.019 [0.010, 0.027] |
| qdrant | far | 10 | random | 0.007 [-0.010, 0.023] |
| qdrant | far | 100 | ordered | -0.129 [-0.342, 0.102] |
| qdrant | far | 100 | random | -0.111 [-0.337, 0.163] |
| qdrant | iid | 10 | random | -0.009 [-0.023, 0.000] |
| qdrant | iid | 100 | random | 0.008 [-0.135, 0.163] |
| qdrant | near | 10 | random | -0.012 [-0.067, 0.017] |
| qdrant | near | 100 | random | 0.003 [-0.078, 0.050] |

## 3. 순서 및 중간 관찰·대기

동일 초기 인덱스·최종 데이터 쌍이다. ordered−random, continuous−settled의 R@100 차이다.
초기 그래프를 복제했어도 이후 삽입 시 backend PRNG를 동일 seed로 제어하지 못한다. 작은 차이를 순서·분할만의 인과 효과로 확정하지 않는다.

| 비교 | DB | geometry | wave | 차이 (%p) |
|---|---|---|---:|---:|
| 순서 | postgres | bridge | 1 | 0.008 [-0.038, 0.072] |
| 순서 | postgres | bridge | 5 | 0.014 [-0.040, 0.058] |
| 순서 | postgres | far | 1 | -0.049 [-0.095, -0.010] |
| 순서 | postgres | far | 5 | 0.034 [-0.075, 0.185] |
| 순서 | qdrant | bridge | 1 | 0.001 [-0.010, 0.015] |
| 순서 | qdrant | bridge | 5 | -5.744 [-8.237, -4.427] |
| 순서 | qdrant | far | 1 | -0.016 [-0.050, 0.022] |
| 순서 | qdrant | far | 5 | 0.003 [-0.030, 0.027] |
| 관찰·대기 | postgres | far | 5 | 0.055 [-0.132, 0.230] |
| 관찰·대기 | postgres | iid | 5 | -0.003 [-0.015, 0.005] |
| 관찰·대기 | qdrant | far | 5 | 0.027 [-0.037, 0.077] |
| 관찰·대기 | qdrant | iid | 5 | 0.041 [0.037, 0.050] |

## 4. 집단별 확인과 기하 진단

기존·신규·경계 질의를 각 200개씩 동일 가중 집계했다. 실제 서비스의 질의 비율이 아니다.
아래는 전체 조건 중 집단별 가장 낮은 평균 최종 history gap이다. 조건이 서로 다르므로 집단 간 직접 순위가 아니다.

| DB | 질의 집단 | R@10 history gap 최솟값 (%p) | R@100 history gap 최솟값 (%p) |
|---|---|---:|---:|
| postgres | old | -0.144 | -0.071 |
| postgres | new | -0.261 | -0.034 |
| postgres | boundary | -0.050 | -0.080 |
| qdrant | old | -0.100 | -0.029 |
| qdrant | new | -8.317 | -7.983 |
| qdrant | boundary | -9.750 | -9.456 |

고정 입력의 exact 20-NN 그래프에서 연결 성분·교차 이웃·conductance와 MMD를 측정했다.
추가 진단은 기존·신규 문서 집단별 NN1/NN20 거리와 LID 분포, 신규→D0 거리 / D0 내부 거리를 보존했다.
이 값은 데이터의 이웃 구조이며 제품 내부 HNSW 그래프 관측값이 아니다. 거리·밀도·위상을 독립 조작한 완전 요인 실험도 아니다.

## 5. 튜닝·검색 경로·자원

- 별도 seed=7의 D0 및 네 최대 삽입 상태에서 공개 후보 중 Recall@10·100≥0.95를 충족하는 구성을 선택했다. 평가 중에는 동결했다.
- PostgreSQL 17.11 / pgvector 0.8.7: M=32, efConstruction=200, efSearch=100. 초기 COPY 배치 1,000, 증분 wave 전체를 COPY/커밋 한 번으로 전송했다.
- Qdrant 1.17.1: M=32, efConstruction=200, ef=128. 초기 전송 배치 1,000, 증분 wave마다 batch upsert 한 번이다.
- PostgreSQL 모든 검색에서 seqscan off 및 지정 HNSW 계획을 검증하고 시점별 실제 실행도 확인했다. Qdrant는 HNSW 카운터 증가, plain/exact 증가 0, 안정된 segment를 확인했다.
- M2 24GB, Colima 4 CPU·약 7.7GiB VM에서 DB별 2 CPU·4GiB, 한 번에 한 DB만 실행했다. 품질은 현재 상태 exact 정답에 대한 측정이다. 지연·처리량은 공유 호스트와 서로 다른 클라이언트 경로의 영향을 받으므로 제품 성능 순위로 해석하지 않는다.
- 입력·원시 결과·파생 분석의 캠페인 보존 크기는 약 1000.5MiB, 보고 시 호스트 여유는 35.33GiB였다. 기존 이미지·VM·과거 실험은 이 보존 크기에 포함하지 않았다.
- 선언된 캠페인 컨테이너·볼륨 잔존 수는 0개다. 기존 자원과 소유가 불명확한 자원을 전역 prune하지 않았다.
- 단계별 실측 시간(분): SETUP 4.9, A 30.2, B 83.8, C 24.2, D 7.5, REPORT 0.8. 파생 기하 분석 시간은 별도 receipt에 기록했다.
- 추가 유료 API·클라우드 인스턴스를 사용하지 않았다. WDL 프로젝트 설정은 기존 without-setup 선택을 유지했다.

## 6. 완료 증거와 재현

- native summary run: `95b94fc5-7bca-45c6-97f9-806d84a31374`; `summary/complete_evidence=PASS`.
- [전체 측정 보고서](../../study-results/campaign-20261025-01/cells/summary__all/attempt-1/REPORT.md)
- [시점·집단별 CSV](../../study-results/campaign-20261025-01/cells/summary__all/attempt-1/curves.csv), [최종 CSV](../../study-results/campaign-20261025-01/cells/summary__all/attempt-1/endpoints.csv), [대응 경로 CSV](../../study-results/campaign-20261025-01/cells/summary__all/attempt-1/paired-paths.csv)
- [seed별 집계·모든 시점 경고](../../study-results/campaign-20261025-01/analysis/quality-summary/aggregates.json)
- [집단별 거리·LID 진단](../../study-results/campaign-20261025-01/analysis/geometry-cohorts/cohorts.csv)
- [native run·asset check 감사](../../study-results/campaign-20261025-01/analysis/native-audit.json), [정리·자원 감사](../../study-results/campaign-20261025-01/analysis/final-audit.json)
- [동결된 실행 계획](plan.md), [스모크·실패 조치 기록](execution.md)
- [제품 벤치마크 원문·제약](../research/product-benchmarks.md), [연구 수치 원문 확인](../research/literature-audit.md)

```sh
.venv/bin/python -m study.launch study-results/campaign-20261025-01
```
완료 셀은 native run·ack·파일 해시를 검증하고 건너뛴다. 실패한 쓰기를 자동 재전송하지 않는다.

실제 임베딩·더 큰 규모·장기 삭제/동일 ID upsert·필터/선택도/카디널리티·추가 DB·클라우드 비교는 [기존 전체 계획](../archive/full-plan-2026-10-25.md)의 미실행 범위로 남아 있다.
이 결과만으로 그 범위의 품질이나 비용을 확정하지 않는다.
