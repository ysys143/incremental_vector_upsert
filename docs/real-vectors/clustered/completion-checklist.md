# 클러스터별 삽입 완료 감사표

이 표의 완료 판정은 실제 실행 산출물로 갱신한다. 계획·코드 존재만으로 실험 완료를 표시하지 않는다.
2026-10-27 사용자 지시 `디스크 검사 빼라고`로 디스크 입장 한도는 철회됐다.
아래 이전 디스크 한도 항목은 역사적 기준이며 현재 실행을 차단하지 않는다. 사용량 계측은 관찰만 한다.
최초 준비 증거는 `runs/clustered-20261027-01`, 재개 실행은 `runs/clustered-20261027-02`다.
후속 manifest의 채택 참조로 원본 준비 증거를 추적한다. 원래 비용 예측 BLOCK은 보존한다.

## 승인·입력·튜닝

| 요구사항 | 확인할 근거 | 현재 판정 |
|---|---|---|
| 12시간 고정 시계, 재시도 포함, 로컬·비용 경계 | authorization.json, manifest.json, controller/guardian 종료 시각 | 시계 동결 확인; 종료는 미완 |
| 이전 결과·원본 source 보존 | source_refs의 원본 hash, source producer receipt, 독립 감사 | 시작 시 hash 확인; 종료 재검증 대기 |
| 구조 개편 후 두 Python 환경, CLI, 218노드 DAG | g0-definitions.json, g0-final-tests.log, g0-receipt.json | 실행 전제 PASS; 기존 스모크 집계 변이 검사 5개는 G5 전 별도 실행 대기 |
| source 변이 후 worker 시작 차단 | g0-source-gate/receipt.json와 native 양성·음성 run | PASS |
| MS MARCO·Wikipedia 원본 768D float32 IP | inputs producer receipt, 원본 file/row hash | PASS |
| calibration/평가 비중복과 3 seed 겹침률 | prepared.json, input-review.json | PASS |
| A만 spherical k-means K=5, 3회 초기화, 최대 50회, tol 1e-4 | clustering 진단·cluster npz·source snapshot | PASS |
| 0 norm/비유한값/빈·중복 centroid 거부 | G0 검사와 실제 input producer 성공 | PASS |
| M1/M5·C1/C5 전체 순서 동일, M5/C5 wave 크기 동일 | event npz·state 해시·G1 validate_prepared | PASS |
| 네 경로 동일 D0·최종 원장, DB 간 입력 동일 | 동일 input parent와 source/ledger hash | 입력 PASS; 실제 DB 대조 대기 |
| 현재 D_t float64 exact top100와 scalar 표본 교차검증 | oracle receipt와 G1 검증 | PASS |
| ID·벡터·질의 순서·다른 상태 oracle 변이 거부 | test_clustered_data.py 실행 로그 | PASS |
| 기존 calibration 기반 ±0.5%p 검출 한계 진단 | inputs의 prior-sensitivity.json | PASS; 실제 clustered 효과의 검정력으로 해석하지 않음 |
| 8개 구축 후보 × 5상태 × 5 ef, 200질의 | calibration 8개 receipt, 25조건/후보, g2-review.json | PASS: 200조건·40,000 calibration 질의 |
| 전 상태 R@10/100 ≥0.99, 비용 frontier·5% tie 규칙 | tuning-candidates.json, tuning.json, g2-review.json | PASS: 네 운영 설정 동결 |
| 평가 입력을 보고 튜닝하지 않음·운영/추가 ef 동결 | seed7 calibration 의존성, tuning parent hash | PASS: 운영 ef와 추가 ef 100/128 동결 |

## 실제 DB·시간·자원

| 요구사항 | 확인할 근거 | 현재 판정 |
|---|---|---|
| PG 초기 COPY→CREATE INDEX→ANALYZE, 증분 기존 HNSW COPY | 동결 adapter, build·write receipt, OID | 실제 스모크 대기 |
| PG wave당 commit 1회, chunk를 wave로 계산하지 않음 | write-N.json 요청/commit 수, G0 실패 주입 | 단위 검사 PASS; 실제 wave 대기 |
| Qdrant 고정 1000 전송 batch, wait=true, wave말 안정화 | write-N.json, wait.json, telemetry | 실제 스모크 대기 |
| 매 wave 전체 ID·벡터·속성·원장 대조 | point/receipt, state·oracle hash, runtime verify_snapshot | 실제 스모크 대기 |
| PG 모든 평가 질의 HNSW EXPLAIN, 시점별 ANALYZE | query-plans.json.gz, path.json, ANN receipt | 첫 후보 통과; 전체 검증 미완 |
| PG seqscan off·indexscan on·cost/parallel/iterative 설정 | effective-settings.json와 모든 query guard | 첫 후보 통과; 전체 검증 미완 |
| PG 인덱스 제거·seqscan on·다른 index 음성 대조 | path_guard:postgres positive·negative_controls, g3-path-review.json | PASS |
| Qdrant exact=false·완전 HNSW·plain/exact delta=0 | telemetry-before/after, ANN receipt, 설정 | 미완 |
| Qdrant 실제 exact·plain 검색 및 telemetry 누락 음성 대조 | path_guard:qdrant positive·negative_controls, g3-path-review.json | PASS |
| 같은 physical snapshot 복원·파일/graph 동일성 | 초기 archive·restore.json·재시작 전후 point | 미완 |
| 4조합 C5/M1 스모크 8이력·4fresh·4초기 | smoke 역할 native receipt | 미완 |
| 1회/5회·build·settle·restore·cleanup 실측 비용, 1.5배 보수 계수 | smoke native timing·point timing·cost-gate.json | 미완 |
| 전 범위 비용 예측 입장 | 원래 cost-gate.json, 사용자 예외 승인, 후속 cost_gate:all | 원래 BLOCK; 사용자가 예측 초과 상태의 진행을 지시. 실제 마감 준수는 별도 미완 |
| DB 동시성1·CPU2, DB와 CPU heavy 작업 격리 | native lease/check 종료 구간의 독립 감사 | 현재 단일 DB 확인; 전 기간 감사 미완 |
| 2CPU/4GiB, 새 소유 증가+예약≤8GiB, 호스트 예약 후≥10GiB | container.json, 자원 admission, resource samples | 현재 실행 입장 통과; 전 기간 감사 미완 |
| worker peak RSS·DB sampled RSS/disk·지연 분리 | worker receipt, raw resource-samples, resources.json, conditions.csv | 계측 동작 확인; 최종 집계·delivery 해시 대조 미완 |
| 실패 쓰기 자동 재전송 금지·실패 시도 보존 | write intent/ACK, failure, native registry, retry disposition | 단위 검사 PASS; 실제 실패 여부 종료 감사 |

## 본 실험·분석·제출

| 요구사항 | 확인할 근거 | 현재 판정 |
|---|---|---|
| 12 physical 초기 snapshot | initial 12개 native receipt | 미완 |
| 48 이력·192 증분 측정 | history_slot0..3 전체 catalogue/native receipt | 미완 |
| 초기36·최종36·중간48 = fresh120 실제 구축 | fresh 120개 native receipt·구축 순서 hash | 미완 |
| 추가 ef168·전체480조건·211200질의 | report completion과 독립 inventory 감사 | 미완 |
| 원시 ID/길이로 R@10/100 재계산, 중복·비생존 ID 거부 | raw.npz, oracle, report raw_scores | 미완 |
| 같은 200질의 시간 변화와 같은 D_t fresh gap 분리 | trajectories.json/csv | 미완 |
| 최종 순서·분할·상호작용 대비와 세 seed 각각의 결과 | endpoints.json, contrasts.json | 미완 |
| 최종 fresh3회 평균·범위, 중간 fresh1회 한계 | endpoints/trajectories와 보고서 | 미완 |
| old/new/boundary·주된 cluster·혼합 집단, n<30 제한 | cohorts.json/csv, frozen cohort codes | 미완 |
| 대응 bootstrap2000, 주 대비24개 family의 별도100000 bootstrap | contrasts.json, 고정 seed·alpha·반복 수 | 미완; 불안정 꼬리로 유의성을 확정하지 않음 |
| C5 fresh gap 및 C5−M5 ≤−0.5%p가 2/3 seed인지 | investigation-signals.json | 미완; 유의성 검정과 구분 |
| 운영 ef·추가 ef의 같은 완성 인덱스 및 fresh 대비 | ef-curves.json, 조건별 원장/질의 해시 | 미완 |
| 공간 분리·질의 coverage·천장효과·seed 의존성·로컬 한계 | prepared-diagnostics, prior-sensitivity, 최종 해석 | 입력 한계 기록; 결과 해석 미완 |
| 모든 native check·source/artifact hash·소유 자원 정리 | independent-completion-audit.json, guardian receipt | 미완 |
| 마감 전 markdown 결과·실행 기록·근거 링크 제출 | 최종 문서와 delivery receipt·시각/hash | 미완 |

삭제·churn·같은 ID upsert·필터·K/상대 규모 변화는 이번 승인 480조건 밖의 후속 연구다.
이번 완료 판정으로 이 후속 연구까지 완료했다고 표시하지 않는다.

## 최종 자동 검증 (G5)

독립 native 감사 PASS, 48 이력·120 fresh·480 검색 조건·211,200 평가 질의, 레거시 receipt 변이 검사 5건, 원시 자원 표본 해시 및 보고 자원 요약 대조를 확인했다. 원래 12시간 완료 목표는 MISSED이며 후속 승인으로 전체 범위를 완주했다. 최종 수치와 한계는 [결과](results.md), 전달 증거는 [clustered-20261027-02/delivery.json](../../../runs/clustered-20261027-02/delivery.json)에 있다.
