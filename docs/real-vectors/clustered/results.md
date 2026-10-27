# 공개 벡터의 클러스터별 증분 삽입: 최종 결과

## 범위와 측정 완료

원래 승인 시계는 2026-10-27 11:56:53 KST부터 2026-10-27 23:56:53 KST까지다. 12시간 내 완주 목표는 MISSED다. 사용자 지시에 따른 후속 완주에서 승인 범위 전체를 2026-10-28 00:57:56 KST에 완료했다. 기술 실행 watchdog은 2026-10-28 12:47:48 KST이며 새로운 12시간 목표가 아니다. 원본 승인·시계 파일은 보존했다. 독립 native 감사가 PASS인 측정만 아래에 실었다.

| 이력 | 증분 검색 | fresh 구축 | 추가 ef 검색 | 품질 조건 | 평가 질의 | native 노드 | 채택된 원본 노드 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 48 | 192 | 120 | 168 | 480 | 211,200 | 218 | 34 |

같은 공개 벡터 집합의 경로별 결과다. 질의 bootstrap 구간은 고정된 데이터와 구축된 인덱스에 조건부이며, 세 seed는 하나의 공개 pool을 공유한다. 구간이 0을 포함하거나 효과를 검출하지 못해도 동등성은 입증되지 않는다.

## 수치 판단 요약

12개 C5 최종 이력의 같은 현재 집합 fresh 대비 Recall@100 차이는 -0.082~+0.062%p다. C5−M5 및 상호작용 24개 중 familywise 구간에서 0을 제외한 것은 0개(양의 방향 0개, 음의 방향 0개)다. 이는 고정 데이터·인덱스의 질의 재표본 결과이며 제품 일반화나 효과 부재의 증명이 아니다.

사전 지정 조사 신호(해당 seed 2개 이상, 유의성 판정 아님): msmarco/postgres: seed 없음 (0/3; 조사 비해당); msmarco/qdrant: seed 없음 (0/3; 조사 비해당); wikipedia/postgres: seed 없음 (0/3; 조사 비해당); wikipedia/qdrant: seed 없음 (0/3; 조사 비해당).

## 동결된 검색 설정

고정된 M, efConstruction, efSearch는 아래와 같다. 경로 M1/M5는 혼합, C1/C5는 클러스터 묶음이며 1/5는 쓰기 wave 수다. A 원본 float32의 IP 검색을 사용하고, 클러스터 지정만 정규화 복사본에서 했다.

| 데이터 | DB | M | efConstruction | ef |
|---|---|---:|---:|---:|
| msmarco | postgres | 16 | 100 | 1000 |
| msmarco | qdrant | 48 | 300 | 256 |
| wikipedia | postgres | 16 | 100 | 512 |
| wikipedia | qdrant | 48 | 300 | 256 |

초기 튜닝은 별도 calibration 입력에서 최대 200조건×200질의=40,000질의를 사용했다. 이는 위 211,200개 평가 질의에 포함되지 않는다. 지정된 다섯 상태의 Recall@10/100 ≥99%를 통과한 제한 후보 중 비용 frontier로 선택했다. 전역 최적 설정이라는 뜻은 아니다.

## C5의 같은 최종 집합 fresh 대비

아래 차이는 같은 시점의 동일 현재 집합으로 새로 구축한 인덱스와의 Recall@100 차이다. 시간에 따른 Recall 변화와 다른 추정량이다.

| 데이터 | DB | seed | C5 R@100 (%) | fresh 평균 (%) | C5−fresh 차이 및 95% 구간 (%p) |
|---|---|---:|---:|---:|---|
| msmarco | postgres | 11 | 99.700 | 99.706 | -0.006 [-0.041, +0.029] |
| msmarco | postgres | 29 | 99.797 | 99.735 | +0.062 [+0.020, +0.099] |
| msmarco | postgres | 47 | 99.788 | 99.746 | +0.043 [+0.004, +0.079] |
| msmarco | qdrant | 11 | 98.915 | 98.997 | -0.082 [-0.172, +0.005] |
| msmarco | qdrant | 29 | 98.975 | 98.954 | +0.021 [-0.077, +0.119] |
| msmarco | qdrant | 47 | 99.000 | 98.941 | +0.059 [-0.033, +0.159] |
| wikipedia | postgres | 11 | 99.732 | 99.743 | -0.012 [-0.052, +0.024] |
| wikipedia | postgres | 29 | 99.702 | 99.717 | -0.015 [-0.053, +0.023] |
| wikipedia | postgres | 47 | 99.725 | 99.709 | +0.016 [-0.022, +0.057] |
| wikipedia | qdrant | 11 | 99.700 | 99.703 | -0.003 [-0.061, +0.048] |
| wikipedia | qdrant | 29 | 99.715 | 99.695 | +0.020 [-0.028, +0.070] |
| wikipedia | qdrant | 47 | 99.742 | 99.696 | +0.046 [-0.008, +0.100] |

## 삽입 경로 대비와 상호작용

두 비교는 각 데이터·DB·seed의 대응 질의를 사용한다. 95% 질의 구간과 24개 사전 지정 비교에 대한 familywise 구간을 함께 표기한다.

| 데이터 | DB | seed | 대비 | 차이·95% 구간 (%p) | 24개 familywise 구간 (%p) |
|---|---|---:|---|---|---|
| msmarco | postgres | 11 | C5-M5 | +0.008 [-0.030, +0.047] | +0.008 [-0.052, +0.070] |
| msmarco | postgres | 11 | interaction | -0.010 [-0.020, +0.000] | -0.010 [-0.027, +0.005] |
| msmarco | postgres | 29 | C5-M5 | +0.032 [-0.007, +0.070] | +0.032 [-0.025, +0.092] |
| msmarco | postgres | 29 | interaction | +0.000 [-0.015, +0.017] | +0.000 [-0.027, +0.027] |
| msmarco | postgres | 47 | C5-M5 | +0.018 [-0.015, +0.052] | +0.018 [-0.035, +0.072] |
| msmarco | postgres | 47 | interaction | +0.000 [-0.013, +0.013] | +0.000 [-0.020, +0.020] |
| msmarco | qdrant | 11 | C5-M5 | -0.048 [-0.127, +0.030] | -0.048 [-0.170, +0.072] |
| msmarco | qdrant | 11 | interaction | -0.065 [-0.173, +0.033] | -0.065 [-0.235, +0.102] |
| msmarco | qdrant | 29 | C5-M5 | -0.028 [-0.105, +0.045] | -0.028 [-0.152, +0.093] |
| msmarco | qdrant | 29 | interaction | -0.003 [-0.092, +0.080] | -0.003 [-0.138, +0.132] |
| msmarco | qdrant | 47 | C5-M5 | -0.053 [-0.132, +0.022] | -0.053 [-0.170, +0.060] |
| msmarco | qdrant | 47 | interaction | -0.098 [-0.187, -0.008] | -0.098 [-0.235, +0.040] |
| wikipedia | postgres | 11 | C5-M5 | +0.010 [-0.027, +0.048] | +0.010 [-0.052, +0.072] |
| wikipedia | postgres | 11 | interaction | +0.000 [-0.010, +0.010] | +0.000 [-0.017, +0.017] |
| wikipedia | postgres | 29 | C5-M5 | +0.030 [-0.017, +0.078] | +0.030 [-0.043, +0.107] |
| wikipedia | postgres | 29 | interaction | +0.003 [-0.010, +0.018] | +0.003 [-0.018, +0.027] |
| wikipedia | postgres | 47 | C5-M5 | +0.013 [-0.023, +0.052] | +0.013 [-0.047, +0.073] |
| wikipedia | postgres | 47 | interaction | +0.000 [-0.015, +0.017] | +0.000 [-0.025, +0.028] |
| wikipedia | qdrant | 11 | C5-M5 | +0.007 [-0.043, +0.055] | +0.007 [-0.070, +0.083] |
| wikipedia | qdrant | 11 | interaction | -0.007 [-0.067, +0.052] | -0.007 [-0.098, +0.082] |
| wikipedia | qdrant | 29 | C5-M5 | -0.032 [-0.080, +0.012] | -0.032 [-0.105, +0.037] |
| wikipedia | qdrant | 29 | interaction | -0.052 [-0.110, +0.007] | -0.052 [-0.145, +0.037] |
| wikipedia | qdrant | 47 | C5-M5 | +0.017 [-0.025, +0.060] | +0.017 [-0.048, +0.082] |
| wikipedia | qdrant | 47 | interaction | -0.010 [-0.058, +0.040] | -0.010 [-0.088, +0.067] |

## 네 경로의 최종 seed 범위

아래 범위는 seed 11·29·47의 관측값 범위다. seed 간 독립 반복의 신뢰구간이 아니다.

| 데이터 | DB | 경로 | 최종 R@100 범위 (%) | 같은 집합 fresh 차이 범위 (%p) |
|---|---|---|---:|---:|
| msmarco | postgres | M1 | 99.685–99.773 | -0.021–+0.028 |
| msmarco | postgres | M5 | 99.692–99.770 | -0.014–+0.030 |
| msmarco | postgres | C1 | 99.703–99.795 | -0.003–+0.060 |
| msmarco | postgres | C5 | 99.700–99.797 | -0.006–+0.062 |
| msmarco | qdrant | M1 | 98.918–99.005 | -0.079–+0.061 |
| msmarco | qdrant | M5 | 98.963–99.053 | -0.034–+0.113 |
| msmarco | qdrant | C1 | 98.935–99.047 | -0.062–+0.106 |
| msmarco | qdrant | C5 | 98.915–99.000 | -0.082–+0.059 |
| wikipedia | postgres | M1 | 99.675–99.725 | -0.042–+0.003 |
| wikipedia | postgres | M5 | 99.672–99.722 | -0.045–+0.003 |
| wikipedia | postgres | C1 | 99.702–99.735 | -0.015–+0.016 |
| wikipedia | postgres | C5 | 99.702–99.732 | -0.015–+0.016 |
| wikipedia | qdrant | M1 | 99.687–99.732 | -0.017–+0.037 |
| wikipedia | qdrant | M5 | 99.693–99.747 | -0.010–+0.052 |
| wikipedia | qdrant | C1 | 99.700–99.758 | -0.003–+0.063 |
| wikipedia | qdrant | C5 | 99.700–99.742 | -0.003–+0.046 |

## 시간 변화, 질의 집단, 자원

192개 시점 기록의 초기 시점 대비 변화와 같은 현재 집합 fresh 차이는 [trajectories.csv](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/trajectories.csv)에서 별도 열이다. 중간 시점 fresh는 1회여서 구축 변동을 분리해 확정할 수 없다. 추가 ef의 96개 행은 [ef-curves.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/ef-curves.json)에 있다.

집단별 원시 행은 [cohorts.csv](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/cohorts.csv)에 있다. 전체 480행 가운데 n<30이 80행, 추론 부적격이 80행이다. n<30 집단은 기술통계로만 읽는다. 높은 centroid 간 cosine은 공통 임베딩 방향을 포함하므로 클러스터 분리나 HNSW 그래프 단절의 증거가 아니다. 신규 벡터도 같은 공개 pool에서 뽑았으므로 별도 원천의 OOD 유입 실험이 아니다. 클러스터 진단과 집단 coverage는 [prepared-diagnostics.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/prepared-diagnostics.json)에 있다.

480개 로컬 조건의 개별 p95 범위는 3.539–36.553 ms다. 이 범위는 조건별 지연의 관측치이며 제품 간 독립 클라우드 처리량 순위가 아니다. worker RSS 최대 관측값 1.691 GiB, DB 프로세스 RSS 합 표본 최대 0.597 GiB, DB 디스크 표본 최대 0.397 GiB다. RSS 합은 공유 페이지를 중복 계산할 수 있고, 디스크 표본은 연속 최대치가 아니다. 자원 원장 [resource-summary.json](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/resource-summary.json)의 SHA-256은 `b0afcae2943edf6deb97c78b3a07d253e5099c4ab8ff4ac817b9676ac029b3a4`다. 원시 resource-samples 214개 파일을 native artifact 해시에 대조했다.

## 승인 예외와 해석 한계

선행 비용 forecast는 BLOCK이었다. 사용자의 ‘그냥 진행해.’ 승인으로 동일 T0·원래 마감·과학 설정의 첫 후속 실행을 허용했다. [forecast exception](../../../runs/clustered-20261027-02/forecast-exception.json)과 [원래 BLOCK cost gate](../../../runs/clustered-20261027-01/cells/cost_gate__all/69710dc0-23f8-4690-b3dd-a5b20049d1ea/cost-gate.json)를 참조한다. 원본 native 34개 성공 노드는 해시와 원래 실행 시각을 확인하여 채택했다.

사용자의 ‘빨리 재개해. 그런 조건 없애고.’ 명령에 따라 외부 컨테이너의 존재만으로 차단하던 시작 단계와 측정 전·후의 veto를 제거하고, 관찰 JSON은 보존했다. 동시에 실행 중인 외부 컨테이너가 없었다는 주장은 아니다. 이전 native 실패·중단 7건은 삭제하지 않고 명시적 재시도 원장에 남겼다. 독립 감사는 총 225개 native 시도에서 218개 노드의 최신 성공 시도와 기존 실패·중단 7건을 대조했다. [재시도 사전 점검](../../../runs/clustered-20261027-02/retry-followup-preflight.json)과 [승인된 여섯 재시도](../../../runs/clustered-20261027-02/manifest.json), [독립 완료 감사](../../../runs/clustered-20261027-02/independent-completion-audit.json)를 참조한다.

19:00:35에 원래 제어기와 daemon이 종료되면서 Wikipedia/PostgreSQL seed 47 final fresh 1회가 중단됐다. 부분 point·read·extra 기록은 보존했으나 완료 수량에 포함하지 않았다. 사용자의 ‘재개해.’ 지시에 따라 그 시점에는 원래 절대 마감을 유지하고 CAMPAIGN 내부 예약 시간만 재배정하여 독립 감독 프로세스로 남은 작업을 재개했다. [고아 실행 정리](../../../runs/clustered-20261027-02/native-orphan-reconciliation.json), [독립 감독](../../../runs/clustered-20261027-02/supervisor.json), [중단 직전 guardian](../../../runs/clustered-20261027-02/prelaunch-revisions/r4/guardian-receipt.json)를 참조한다.

원래 마감 직전 Wikipedia/Qdrant seed 47의 initial fresh 1회는 다음 셀과 정리 시간이 남은 마감 안에 들어오지 않아 입장 단계에서 실패했다. 사용자의 후속 완주 지시로 동일한 데이터·측정 범위를 끝까지 실행했다. 원래 12시간 목표 미달은 MISSED로 유지하며 기술 watchdog은 최대 실행 시간 제한이다. [후속 완주 정책](../../../runs/clustered-20261027-02/manifest.json)과 [원래 마감 실패](../../../runs/clustered-20261027-02/cells/fresh__wikipedia-qdrant-s47-initial-fresh-1/7fa6b500-4ba1-4530-a04f-d38cb6c2c595/failure.json)를 참조한다.

사용자의 ‘디스크 검사 빼라고’ 명령에 따라 디스크 잔여 용량·증가량·예약·소유 용량의 실행 차단 조건을 제거했다. `disk_policy=OBSERVE_ONLY`에서 사용량 계측 JSON은 관찰 증거로 남기며, 원래 승인 시계 파일과 과학 실험 수량은 유지했다. 재개 중 기록된 800개 HNSW 질의 PASS와 9,102개 벡터 쓰기 ACK는 정책 변경 뒤 실제 경로의 부분 증거이며 전체 완료 증거는 아니다. [디스크 정책](../../../runs/clustered-20261027-02/manifest.json)과 [재개 중 실제 경로](../../../runs/clustered-20261027-02/disk-policy-live-proof.json), [개정 직전 manifest](../../../runs/clustered-20261027-02/prelaunch-revisions/r3/manifest.json)를 참조한다.

재생성 가능한 Go 빌드 캐시 정리 뒤 호스트 여유 공간의 관측 증가량은 4.03 GB였다. 이 값은 용량 입장 조건이 아니라 관측값이다. [캐시 정리 영수증](../../../runs/clustered-20261027-02/cache-recovery.json)을 참조한다.

이 실험은 공개 벡터의 신규 삽입 순서와 분할을 비교한다. 삭제만, 삭제+삽입 churn, 동일 ID 임베딩 교체 upsert, 필터 선택도·카디널리티, 다른 클러스터 수, 장기 운영 열화는 이 결과의 측정 범위 밖이다. HNSW 내부 edge를 직접 관측하지 않았다. 효과 부재나 좁은 구간을 제품 간 보편적 동등성으로 일반화하지 않는다.

## 근거 파일

- [승인 당시 계획 원문](../../../runs/clustered-20261027-02/approved-plan.md) (SHA-256 `12dddc253e760c3cd7dff7a29fa7f015765f8b360b750e7bdafee48010335e37`), [실행 중 개정 표시가 붙은 계획](../clustered-insertion-plan.md) (SHA-256 `4ac374147146a7d15a4623b8f5827fddb23dc8082b3beedfafc82e9b726c0778`), [실험 matrix](../clustered-insertion-matrix.json), [manifest](../../../runs/clustered-20261027-02/manifest.json), [독립 완료 감사](../../../runs/clustered-20261027-02/independent-completion-audit.json), [레거시 5검사 로그](../../../runs/clustered-20261027-02/legacy-receipts.log).
- [원래 native 보고서](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/report.md), [endpoint 원자료](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/endpoints.json), [대응 대비와 CI](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/contrasts.json), [검색 조건 480행](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/conditions.csv), [구축·쓰기 비용](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/operation-costs.json), [사전 조사 신호](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/investigation-signals.json).
- [중간 경로](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/trajectories.csv), [질의 집단](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/cohorts.csv), [ef 곡선](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/ef-curves.json), [자원 표본 요약](../../../runs/clustered-20261027-02/cells/report__all/948e89fb-cd1c-4c27-a692-cdb7b751396e/resource-summary.json), [공개 원본 바인딩](../../../runs/clustered-20261027-02/manifest.json).
