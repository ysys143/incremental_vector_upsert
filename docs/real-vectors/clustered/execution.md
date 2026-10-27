# 클러스터별 삽입 실행 기록

## 실행 범위와 시계

- 사용자 실행 지시: `clustered-insertion-plan 완수.`
- 실행 폴더: [`runs/clustered-20261027-01`](../../../runs/clustered-20261027-01)
- T0: 2026-10-27 11:56:53 KST. 절대 마감: 같은 날 23:56:53 KST.
- 공개 임베딩 2종, DB 2종, seed 3개. 48이력·120fresh·480검색 조건·211,200평가 질의.
- 준비·구현·튜닝·스모크·보고 시간을 포함한다. 과거 실험의 결과 수량은 합산하지 않는다.
- 기존 results/study-results/real-results 및 docs/archive는 읽기 전용 입력이다.

## 2026-10-27 G0와 시작

G0 실행 전제 PASS. 전체 단위 검사 120개 중 115개 통과, 기존 실제 스모크 증거를 입력으로
받는 집계 변이 검사 5개는 `PILOT_SMOKE_ROOT`가 지정되지 않아 건너뛰었다. 새 G3의 DB 경로
검사는 이 5개와 별개다. 에이전트는 2026-10-27 DB 측정 종료 후 G5 제출 전에 원본을
보존한 사본에서 5개를 재실행한다. 현재는 CPU 작업과 DB 측정의 경쟁을 피하기 위해
대기 중이며 이 추가 검증은 미완이다. G0 결과만으로 실제 ANN 실행을 주장하지 않는다.

- [최종 검사 로그](../../../runs/clustered-20261027-01/g0-final-tests.log)
- [218개 DAG 정의 및 CLI 검사](../../../runs/clustered-20261027-01/g0-definitions.json)
- [native source 변이 거부](../../../runs/clustered-20261027-01/g0-source-gate/receipt.json)
- [G0 receipt](../../../runs/clustered-20261027-01/g0-receipt.json)
- [승인과 고정 시계](../../../runs/clustered-20261027-01/authorization.json)
- [동결 실행 manifest](../../../runs/clustered-20261027-01/manifest.json)

제어기와 측정기는 서로 다른 Python 환경을 쓴다. 제어기의 NumPy 의존성을 분리했다.
최초 manifest의 가상환경 경로가 symlink 해석으로 base Python이 된 점을 실행 전에 수정했다.
수정 전 manifest/source는 `prelaunch-revisions/r0`에 보존했고 T0·설계·수량은 바꾸지 않았다.

12:23:41 KST native 제어기 시작. 이 기록 작성 시 G1~G5는 미완이며 실제 튜닝 수치는 아직 없다.

## 재현과 관측

```sh
PYTHONPATH="$PWD/src" .venv/bin/python -m vectorbench.experiments.real_vectors.clustered.campaign runs/clustered-20261027-01 --catalogue-only
```

최초 실행 명령은 아래와 같다. 이미 실행 중인 캠페인에 두 번째 제어기를 시작하지 않는다.

```sh
PYTHONPATH="$PWD/src" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 .venv/bin/python -m vectorbench.orchestration.control runs/clustered-20261027-01 --seconds 43200
```

제어기 상태 파일뿐 아니라 그 PID의 실제 프로세스와 native 상태를 함께 확인한다.
실패 시 자동 재시작하지 않고 실패 출력·원장·소유 자원을 먼저 조사한다.
`finalize_clustered`는 동일 제어기를 관측하고 완료 후 독립 감사를 수행한다. 최종 문서 제출은 별도다.

## 게이트 현황

| 게이트 | 현재 확인 |
|---|---|
| G0 | PASS: 코드·CLI·DAG·source 변이·시계·이미지 |
| G1 | 두 입력 producer PASS: 원본 벡터·4경로 원장·현재 상태 oracle 검증 완료 |
| G2 | PASS: 8개 구축 후보·200조건·40,000 calibration 질의, 네 운영 설정 동결 |
| G3 | 스모크·경로 검사 PASS, 원래 비용 예측 BLOCK. 사용자 지시에 따른 예측 입장 예외로 재개 준비 |
| G4 | 미완: 전체 측정 수량과 원시 근거 대기 |
| G5 | 미완: 대응 분석·독립 감사·최종 제출 대기 |

## G1 입력 확인

두 입력 producer는 각각 약 145초에 완료했다. calibration 문서는 평가 문서와 겹치지 않았다.
평가 seed끼리의 문서 겹침률은 MS MARCO 24.895~25.055%, Wikipedia 24.795~25.375%다.
세 seed를 완전히 독립된 실제 서비스 표본으로 해석하지 않는다.

M5/C5의 wave 크기는 해당 seed의 자연 클러스터 크기로 일치시켰다.
MS MARCO 평가 seed 11은 918/1,259/3,393/2,992/1,438개, Wikipedia seed 11은
1,411/2,781/1,792/1,264/2,752개다. 각 합계는 10,000개다.

일부 기존/신규 중심 또는 개별 클러스터 집단은 n<30이므로 기술 통계와 n만 제시한다.
질의를 추가하거나 교체하지 않는다. [입력 검토 요약](../../../runs/clustered-20261027-01/input-review.json)에
전체 seed별 크기·질의 집단 수·겹침률을 기록했다.

### 공간 진단의 해석 경계

평가 seed 전체에서 centroid 사이 cosine 유사도 범위는 MS MARCO 0.8727~0.9622,
Wikipedia 0.9598~0.9812였다. 공개 임베딩의 공통 방향 성분이 포함된 수치이며,
이 값 하나로 클러스터 분리의 강약이나 ANN 그래프 연결성을 판정하지 않는다.

고정 anchor 표본의 신규 이웃 중 같은 클러스터에 속하는 비율은 MS MARCO 약
0.683~0.708, Wikipedia 약 0.532~0.629였다. 이 진단의 old/new anchor 수는
512/256으로 다르므로 전체 DB의 교차 edge 비율로 해석하지 않는다.
이번 실험은 같은 pool에서 선정한 신규 집합의 삽입 순서 조작이며, D0에 없던
완전히 새로운 모드의 유입을 증명한 실험이 아니다.

## G2 첫 운영 설정 선택

MS MARCO·PostgreSQL은 M=16, efConstruction=100, ef=1000으로 동결됐다.
calibration 다섯 상태의 최저 R@10은 99.9%, R@100은 99.735%였다.

다른 적격 설정 M=32, efConstruction=200, ef=512의 최악 p95는 17.661ms이고
선택된 설정은 17.747ms였다. 5% 이내이므로 사전 tie 규칙을 적용했다.
선택된 설정의 구축·쓰기 합계 71.739초는 비교 설정의 224.500초보다 작았다.
이 수치는 제한된 calibration 후보와 공유 로컬 환경의 실측이다.
최대 Recall만으로 운영 설정을 선택한 결과가 아니다.

[선택표와 비용 근거](../../../runs/clustered-20261027-01/cells/tuning__msmarco-postgres/cbf7e02b-d21b-46e1-8ef5-cdf1385b3ca0/tuning.json). 다른 세 데이터셋/DB 조합의 선택과 G2 전체 게이트는 아직 미완이다.

## G2 전체 통과와 G3 시작

8개 구축 후보·200조건·40,000 calibration 질의를 모두 측정했다. 각 후보는 다섯 상태의 다섯 ef를 포함한다.
G2 첫 후보 시작부터 native barrier 종료까지 52.2분으로 90분 예약 안에 완료했다.
이 질의 수는 본 실험의 211,200평가 질의에 합산하지 않는다.

| 데이터 | DB | M | efConstruction | 운영 ef | 추가 ef | 최저 R@10 | 최저 R@100 |
|---|---|---:|---:|---:|---|---:|---:|
| msmarco | postgres | 16 | 100 | 1000 | [100, 128] | 0.99900 | 0.99735 |
| msmarco | qdrant | 48 | 300 | 256 | [100, 128] | 0.99800 | 0.99000 |
| wikipedia | postgres | 16 | 100 | 512 | [100, 128] | 0.99950 | 0.99755 |
| wikipedia | qdrant | 48 | 300 | 256 | [100, 128] | 0.99950 | 0.99670 |

[선정 근거](../../../runs/clustered-20261027-01/g2-review.json). 13:20 KST G3의 실제 스모크가 시작됐다.

## G3 검색 경로 양성·음성 대조

양 DB의 별도 임시 인스턴스에서 경로 게이트를 통과했다.

- PostgreSQL: 정상 4질의에 seqscan off와 지정 HNSW scan을 확인하고 대표 ANALYZE의 Actual Loops≥1을 확인했다. 다른 인덱스 계획 주입, seqscan on, 실제 HNSW 인덱스 제거를 각각 거부했다.
- Qdrant: 정상 4질의의 HNSW delta=4, plain/exact 등 나머지 delta=0이었다. 별도 실제 exact 4회와 plain 4회에서 해당 카운터 증가를 확인했고 ANN 게이트가 둘 다 거부했다. telemetry 누락도 거부했다.

[대조 결과와 원시 receipt 연결](../../../runs/clustered-20261027-01/g3-path-review.json).
이는 경로 검사의 실동작 증거다. G3 전체 통과에는 나머지 paired 삽입 스모크와 비용 입장이 필요하다.

## G3 비용 예측 중단과 사용자 재개 지시

13:40:18 KST, 최초 실행은 34/218 native 작업을 완료한 뒤 비용 예측 게이트에서 중단됐다.
4개 초기 snapshot·4개 fresh·8개 M1/C5 스모크 이력·2개 경로 대조는 모두 완료됐다.
본 실험의 48이력·120fresh는 이때 아직 시작하지 않았다.

보수적 잔여 예측은 40,763.63초(11.32시간), 당시 남은 시간은 37,132.57초(10.31시간)였다.
1.5배 안전 계수와 보고·예약·native 오버헤드를 포함해 약 60.5분 초과했다.
디스크와 검색 품질 실패가 아니라 예측 시간 입장 실패다.
[실패한 원래 비용 예측](../../../runs/clustered-20261027-01/cells/cost_gate__all/69710dc0-23f8-4690-b3dd-a5b20049d1ea/cost-gate.json)을 보존한다.
제어기는 종료됐고 guardian은 소유 자원을 RECONCILED로 정리했다.

사용자 지시 `그냥 진행해.`에 따라 **비용 예측 적합성만 예외로 허용**한다.
원래 T0·23:56:53 KST 마감, 마지막 1시간 보고 예약, 데이터·튜닝·수량,
HNSW 경로 검증과 디스크 입장 기준은 유지한다. 원래 비용 예측을 PASS로 고쳐 쓰지 않는다.

후속 실행 `runs/clustered-20261027-02`는 성공한 준비 34개를 원본 native check와
receipt/artifact/source 해시로 확인해 채택하고, 나머지 184개 작업을 동일 DAG에서 실행한다.
채택 작업의 재검증 시간과 원래 DB 측정 시간을 구분한다. 실패한 비용 작업은 채택하지 않는다.
예측 예외와 별개로, 실제 전 범위 완료 및 마감 준수는 종료 후에만 판정한다.

중단 구간에 기존 스모크 집계 변이 검사 5개를 실행해 모두 통과했다.
원본 증거는 수정하지 않았고, 임시 사본의 checkpoint 누락·raw 손상·거짓 요약·인덱스 식별 변경을 거부했다.

## 후속 실행 시작

13:54:31 KST 후속 제어기 PID 70044와 Dagster daemon PID 70400의 실제 실행을 확인했다.
준비 34개 원본 작업은 30개 native run에 속했다. 하나의 native run이 여러 asset을 실행할 수 있으므로,
작업별 asset·partition·evidence check·materialization run ID를 연결해 각각 검증한다.
재개 전 검증에서 발견한 기존 단일-check 가정은 수정했고, 수정 전 -02 manifest/source는
`prelaunch-revisions/r0`에 보존했다. 이때 본 실험 측정은 시작하지 않았다.

- [34개 원본 준비의 채택 검증](../../../runs/clustered-20261027-02/continuation-preflight.json)
- [재개 관련 검사 23개 PASS](../../../runs/clustered-20261027-02/continuation-tests.log)
- [기존 증거 변이 검사 5개 PASS](../../../runs/clustered-20261027-02/legacy-receipts.log)
- [새 환경의 source 변이 차단 PASS](../../../runs/clustered-20261027-02/g0-source-gate/receipt.json)
- [사용자 예측 입장 예외 기록](../../../runs/clustered-20261027-02/forecast-exception.json)
- [후속 manifest](../../../runs/clustered-20261027-02/manifest.json)
- [제어기 현황](../../../runs/clustered-20261027-02/controller.json)

원본 34개 작업의 DB 측정/다른 heavy 작업 구간에는 겹침이 없었다.
후속 전체 기간의 격리·수량·마감은 종료 감사 대상이며 아직 완료를 주장하지 않는다.

## 외부 컨테이너 차단 제거와 기존 실행 재개

14:09:12 KST, GitHub MCP 서버 컨테이너가 있다는 이유로 첫 이력의 DB 시작이 차단됐다.
초기 인덱스 12개와 앞선 준비를 합친 48개 작업은 완료돼 있었고, 삽입은 시작하지 않았다.
사용자 지시 `빨리 재개해. 그런 조건 없애고.`에 따라 DB 시작의 전역 컨테이너 차단을 제거했다.
14:24 재개 뒤 실제 측정 전·후 경로에도 같은 차단 조건이 남아 있는 것을 발견해 모두 제거했다.
기존 실패 2건과 정리 중 중단된 재시도 1건은 삭제하지 않고 명시적 retry 원장으로 보존한다.
실패 시도는 품질 측정 수량에 포함하지 않는다. 완료된 초기 인덱스 12개를 재사용한다.

외부 컨테이너 목록은 측정 전·후 관찰 기록으로만 남긴다. 실험 소유 DB 작업의 동시성 1,
HNSW 경로 검사, 디스크 한도 및 원래 23:56:53 마감은 유지한다.
공유 호스트의 다른 작업이 지연에 영향을 줄 수 있으므로 로컬 지연을 독립 클라우드 성능으로 해석하지 않는다.

실제 재개 중 디스크 예약 입장에서 대기가 생겨, 활성 Go 빌드/컴파일/링크 프로세스가 없는 것을 확인한 후
재생성 가능한 Go 빌드 캐시만 `go clean -cache`로 정리했다. 이후 호스트 여유 공간이 3.75 GiB 증가했다.
실험 데이터·소스·실행 바이너리와 기존 결과는 보존했다. 다음 Go 빌드에서는 캐시를 다시 만든다.

- [시작 차단의 수정 전후 경로 검사](../../../runs/clustered-20261027-02/container-gate-regression.json)
- [측정 전후 차단의 수정 전후 변이 검사](../../../runs/clustered-20261027-02/measurement-gate-mutation.json)
- [관련 검사 4개 PASS](../../../runs/clustered-20261027-02/measurement-container-regression.log)
- [캐시 정리와 실제 호스트 여유 공간](../../../runs/clustered-20261027-02/cache-recovery.json)
- 수정 전 manifest/source는 `prelaunch-revisions/r1`, `r2`에 보존했다.

## 사용자 지시에 따른 디스크 차단 제거

15:43 KST 사용자 지시 `디스크 검사 빼라고`에 따라 디스크 입장 조건을 철회했다.
공통 용량 함수, registry의 잔여 공간·호스트 증가량·합산 예약, 소유 결과 용량,
비용 게이트의 디스크 조건은 실행을 막지 않는다. 공간·RSS·DB 크기는 관측값으로만 남긴다.
기존의 10 GiB 하한과 8 GiB 증가 상한은 이 지시 이후 실행 조건이 아니다.

대기 중이던 slot1/seed47/C1은 삽입 전에 정리한 뒤 명시적 retry로 연결했다.
완료된 60개 작업과 본 실험 이력 12개는 재실행하지 않는다. 실패·중단 4개는 원장에 보존한다.
원래 23:56:53 KST 마감과 전체 48이력·120fresh·480조건은 유지한다.
15:48 KST 제어기를 재개했고 실제 실험 DB의 시작을 확인했다.

- [디스크 제거 정책과 재개 전 검증](../../../runs/clustered-20261027-02/disk-policy-preflight.json)
- [기존 차단 재현과 제거 후 실행 확인](../../../runs/clustered-20261027-02/disk-policy-regression.json)
- [관련 검사 29개 PASS](../../../runs/clustered-20261027-02/disk-policy-tests.log)
- 변경 전 manifest/source와 실행 상태는 `prelaunch-revisions/r3`에 보존했다.

## 19:00 프로세스 종료 후 독립 재개

19:00:35 KST native 실행 로그에 termination signal과 DagsterExecutionInterruptedError가 남았다.
guardian은 부모 제어기 소실을 확인했고, 보고 타이머도 이후 종료돼 19:30 이후 보고를 보내지 못했다.
종료 신호 발신자는 확인하지 못했다. 19:01의 종료 예상은 실제 프로세스 생존을 확인하지 않은 잘못된 보고였다.

사용자 지시 `재개해.`에 따라 23:29:30 KST 부모 PID=1인 독립 감독 프로세스로 재개했다.
완료된 182개 native 작업(48이력·85fresh)은 보존하고, 중단된 fresh 1회만 재구축한다.
미완 fresh의 부분 원시 결과는 보존하되 완료 수량에 합산하지 않는다. 실패·중단 기록은 총 5개다.
보고용으로 남겨 둔 1시간은 측정에 재배정했으며 원래 절대 마감 23:56:53은 유지했다.
마감 이후 실행 허용 여부는 별도로 사용자에게 질문했고, 이 기록 시점에는 승인되지 않았다.

- [native 고아 실행 정리](../../../runs/clustered-20261027-02/native-orphan-reconciliation.json)
- [재개 전 점검](../../../runs/clustered-20261027-02/detached-resume-preflight.json)
- [독립 감독 프로세스 상태](../../../runs/clustered-20261027-02/supervisor.json)
- 수정 전 manifest/source와 실행 기록은 `prelaunch-revisions/r4`에 보존했다.

## 시간 목표 미달을 보존한 원래 실험 범위 완주

23:55:19 KST 다음 작업의 예상 시간이 마감을 넘는다는 시간 입장 검사로 실행이 중단됐다.
당시 48이력·118fresh·478조건이 완료됐다. 원래 12시간 목표는 미달(`MISSED`)이다.
사용자가 반복 중단을 거부한 정정 지시에 따라 남은 2회와 분석·감사를 완료하는 후속 단계로 전환했다.
원본 승인 파일의 T0·마감·계획·실험표는 변경하지 않는다. 후속 측정은 원래 마감 이후 실행한 것으로 표시한다.

후속 단계의 시간 예측 입장 검사는 실행을 막지 않는다. 기존 프로세스 최대 실행 시간은 기술적 감시용이며
새로운 연구 완료 목표 시각이 아니다. 데이터·동결 튜닝·검색 경로·질의·수량·통계 계산은 유지한다.
실패 6개와 미완 결과를 보존하고, 해당 실패 시도를 성공 수량에 포함하지 않는다.

- [원래 범위 완주 전제와 원본 마감 미달](../../../runs/clustered-20261027-02/full-scope-completion-preflight.json)
- 원래 중단 당시 manifest/source와 실행·보고 상태는 `prelaunch-revisions/r5`에 보존했다.

## 측정 완주와 보고서 정리 오류 복구

2026-10-28 00:49 KST, 48이력·192증분시점·120fresh·168추가ef·480조건·211,200질의 측정을 모두 마쳤다.
보고서 worker는 수치 집계까지 성공했지만, 파생 자원 요약을 `resources.json`에 저장해 소유 자원 정리 원장과
이름이 충돌했다. 그 결과 정리 단계에서 `KeyError('resources')`가 발생했고 native 보고서 작업은 실패했다.

요약 이름을 `resource-summary.json`으로 분리하고 보고서 시도 자체는 자원 요약에서 제외했다.
원래 실패 요약과 정리 오류는 보존했으며, 요약을 별도 경로로 옮긴 후 실제 정리 경로가 정상 종료되는 것을 확인했다.
보고서 코드의 변경은 자원 링크 파일명뿐이며 통계 계산식은 바뀌지 않았다. DB 측정은 재실행하지 않는다.
종료된 worker의 과거 PID를 정리 때 다시 종료하지 않도록 guardian도 수정했다.

- [실제 충돌 재현과 수정 후 정리 검증](../../../runs/clustered-20261027-02/resource-collision-regression.json)
- [보고서만 재시도하는 전제](../../../runs/clustered-20261027-02/report-recovery-preflight.json)
- 원래 실패 요약·manifest/source·상태 기록은 `prelaunch-revisions/r6`에 보존했다.

## 최종 자동 검증 (G5)

독립 native 감사 PASS, 48 이력·120 fresh·480 검색 조건·211,200 평가 질의, 레거시 receipt 변이 검사 5건, 원시 자원 표본 해시 및 보고 자원 요약 대조를 확인했다. 원래 12시간 완료 목표는 MISSED이며 후속 승인으로 전체 범위를 완주했다. 최종 수치와 한계는 [결과](results.md), 전달 증거는 [clustered-20261027-02/delivery.json](../../../runs/clustered-20261027-02/delivery.json)에 있다.
