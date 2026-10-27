# 12시간 계획 실행 기록

## 승인과 고정 기한

- 사용자 지시: `12시간 계획을 완수.` / `승인한다.`
- T0: 2026-10-26 19:55:05 KST.
- 최종 기한: 2026-10-27 07:55:05 KST. 재개·작업 분할로 초기화하지 않는다.
- 현재 결과 루트: `real-results/twelve-hour-20261026-03`.
- 최초 구현·안전 검사·중단된 준비 실행: `real-results/twelve-hour-20261026-01`.
- 승인 원본: `approved-plan.md`, `approved-matrix.json`, `authorization.json`.
- 선언 범위: 66개 이력, 이력 측정 316회, fresh 138회, 필터 64조건, ef 24조건.

## 실행 전 확인

`prelaunch-verification.json`은 아래 검증 산출물의 해시를 기록한다.

| 검사 | 결과 | 근거와 한계 |
|---|---|---|
| 실행 요청 프로세스 종료 후 작업 지속 | PASS | `owner-probe.json`: tmux 서버의 자식이 20초 작업 완료. 서버는 기존 프로세스이고 부모 PID 1. 실제 장기 실험 완주 증거는 아님 |
| 실제 부모 종료 후 watchdog 정리 | PASS | `real-results/20261026T111000Z-watchdog-checks/receipt.json`: 새로 만든 작은 helper 컨테이너·볼륨 부재와 lease 해제 확인 |
| 작업 실패·timeout 정리 | PASS | `safety/receipt.json`: 실제 소유 helper 종료, 컨테이너·빈 볼륨 회수 |
| 기존 cleanup 결함 재현 | EXPECTED DEFECT | 동결된 이전 guardian 함수에서 ended/failed 작업이 정리 목록에서 빠지는 것을 재현 |
| 새 cleanup·부분 쓰기 기록·원장 경계 | PASS | `/opt/homebrew/bin/python3 -m unittest discover -s tests -p 'test_realstudy*.py'`: 66건 통과, `offline-tests-final.log` |
| native Dagster 실행·차단 검사 | PASS | `real-results/20261026T110918Z-engine-checks/receipt.json`: 실제 native 4건 성공, 중복 실행·변조 증거 거부 |
| 새 실행 그래프 | PASS | 실제 `Definitions.validate_loadable`: 현재 233개 작업. 66개 이력·138개 fresh 외에 준비·calibration·필터 스모크·입장 검사·단계 barrier·보고 작업 포함 |
| 실제 DB 비용 스모크 | PASS | 2026-10-26 21:03:31 KST 비용 입장 검사 통과. 양 DB 50회 이력과 각 5k/10k/20k fresh 완료 |
| 12시간 계획 완수 | PASS | native 233개 성공, 독립 감사·집단 보고 보완 PASS. 아래 최종 완료 절 참조 |

처음 `.venv/bin/python`으로 NumPy 검사를 실행한 호출은 `ModuleNotFoundError: numpy`로
실패했다. 이 저장소는 Dagster용 `.venv`와 측정용 `/opt/homebrew/bin/python3`를 분리한다.
위 66건 통과는 측정용 Python으로 다시 실행한 결과다.

## 구현 변경

- `realstudy/twelve_*`: 승인 목록의 native DAG, 공개 입력 재사용 검증, near/far 입력,
  고정 필터 라벨, 비용 입장 검사, 대응 분석과 완료 수 검증.
- `workload.py`: 검색 측정 시점과 고정 200개 부분 질의 적용. 검색하지 않는 wave도
  안정화·전체 벡터·ID·메타데이터·버전을 검사한다.
- `writes.py`: DELETE와 COPY/HTTP 요청의 INTENT·ACK를 각각 영속 기록한다.
  staging COPY의 ACK와 트랜잭션 COMMIT ACK를 구분하며 실패한 쓰기를 재전송하지 않는다.
- `guardian.py`, `engine.py`: 작업 종료 여부와 별도로 소유 자원 정리 결과를 기록한다.
- `control.py`: 승인된 12시간 모드만 고정 전역 기한을 사용한다. 기존 실행의 기한은 변경하지 않는다.
- 필터는 PostgreSQL btree/HNSW 실제 계획과 Qdrant telemetry의 실제 경로를 분류한다.
  필터의 정확 검색을 HNSW 품질로 표시하지 않으며 PostgreSQL Seq Scan은 거부한다.

## 실행 소유자와 조회

기존 tmux 서버에 독립 세션 `ivu-12h-20261026-03`을 생성했다. 이 세션은 Python controller를
실행하고 native Dagster가 의존성과 자원 슬롯을 관리한다. 에이전트가 개별 실험을 순차
호출하는 루프는 사용하지 않는다. 실제 controller PID 생존을 확인했다.

```sh
.venv/bin/python -m realstudy.twelve_status real-results/twelve-hour-20261026-03
```

이 조회는 controller PID와 native 실행 상태를 확인한다. 죽은 프로세스의 캐시가
RUNNING이어도 `ORPHANED/STOPPED_INCOMPLETE`로 표시한다.
원시 로그는 `owner.log`, `daemon.log`, 각 `cells/` 아래에 보존한다.

기존 실패 컨테이너 `ivu-fabe16e51e90`은 정지했다. 컨테이너와
`ivu-fabe16e51e90-data` 볼륨은 삭제하지 않았으며 `prior-quarantine.json`에 기록했다.
기존 부분 이력은 새 성공 결과에 포함하지 않는다.

## 준비 barrier 정체 수정

첫 실행은 입력 작업 2개만 성공하고 DB 작업 전 준비 barrier에서 멈췄다.
partitioned asset에 붙인 evidence check가 unpartitioned여서, 입력 두 개가 시작 순서와
다르게 완료되면 전역 최신 check가 다른 partition의 이전 materialization을 가리켰다.
native 입력 결과 자체의 run별 검사 2건은 통과했으며 DB 결과에 미친 영향은 없다.
`orchestration-defect.json`과 첫 controller 종료 기록을 보존했다.

`asset_check`에도 해당 asset과 같은 `partitions_def`를 적용했다. 설치된 Dagster는 이 기능을
preview로 표시한다. 버전 변경 없이 실제 native queue 검사
`real-results/20261026T111803Z-queue-checks/receipt.json`에서 7개 작업 완료,
CPU 최대 2개 동시 실행, DB blocking check 중 독점 슬롯 유지, 최종 lease 해제를 확인했다.

수정 전 동결 소스·결과는 첫 루트에 남기고 새 루트 `twelve-hour-20261026-02`에
수정된 소스를 동결했다. `successor-disposition.json`은 수정 이유·검사 해시·이전 실행을
기록한다. 두 루트의 승인 원본·T0·최종 기한은 동일하다. 입력 준비만 다시 실행하며,
실패한 DB 쓰기의 재전송은 없다.

## 본 실험 실행 순서 고정과 준비 결과 재사용

두 번째 실행에서 공개 입력 2건, 튜닝 재검증 4건, 필터 실제 경로 스모크 2건,
초기 physical snapshot 6건과 barrier 2건, 총 16개 native 작업이 성공했다.
네 튜닝 조합은 초기·성장·삭제·교체 calibration 상태 모두 Recall@10/100 ≥99%를 통과했다.
본 실험 이력은 이 시점에 아직 시작하지 않았다.

기존 DAG는 비용 입장 검사 뒤 여러 실험군을 동시에 대기열에 넣을 수 있었다.
승인 계획의 짧은 이력 → 장기 이력 → 공간 → 필터 순서를 의존성으로 보장하기 위해
새 예약만 멈추고 이미 대기·실행 중인 준비 작업 16건을 끝까지 완료했다.
모든 native SUCCESS와 lease 0개를 확인한 뒤 controller를 정상 종료했다.

`twelve-hour-20261026-03`은 과학적 조건과 입력·질의·측정 구현을 유지한다.
새 단계별 history/fresh barrier 8개로 전체 작업은 233개다.
`twelve_adopt.py`는 변경 없는 준비 작업 14개만 재사용하며, 이전 native SUCCESS,
partition별 필수 검사, 모든 원시 artifact 해시를 다시 검증한다.
이전 artifact는 새 receipt에서도 외부 참조 해시로 계속 검증한다.
seed 변경과 기한 연장 변이를 거부한 결과는 `adoption-preflight.json`에 남겼다.
이전 결과를 삭제·덮어쓰거나 실패한 쓰기를 다시 보내지 않는다.

세 번째 실행도 T0와 07:55:05 KST 종료 기한을 계승한다.
최종 수치만으로 완료를 선언하지 않도록 독립 감사 명령을 추가했다.

```sh
.venv/bin/python -m scripts.audit_twelve_completion real-results/twelve-hour-20261026-03
```

이 검사는 native 성공·분할별 필수 검사·66개 이력·138개 fresh·필터 64조건·ef 24조건의
정확한 조합·19만 질의·자원 정리·원래 기한을 확인한다. 실행 중인 캠페인에 대한 거부 경로는
`twelve-hour-20261026-02/audit-negative-incomplete.json`으로 확인했다.
최종 감사도 완료됐으며 아래 최종 완료 절에 결과를 기록했다.

독립 tmux 세션 `ivu-12h-audit-03`에서 `scripts.finalize_twelve`가 현재 controller를
관찰한다. 실행 명령과 시작 시각은 현재 결과 루트의 `finalizer-launch.json`에 있다.
controller의 전체 목록 완료와 guardian의 자원 정리가 모두 확인된 뒤 위 감사를 실행한다.
감사 성공 증거는 `independent-completion-audit.json`, 감시·감사 종료 상태는
`finalizer.json`에 기록한다. 감시 프로세스가 살아 있다는 사실은 감사 통과 증거가 아니다.
이 프로세스는 실험을 재시작하거나 12시간 기한을 연장하지 않는다.

## 비용 입장 검사와 본 실험 시작

2026-10-26 21:03:31 KST, `cost_gate:all`이 native SUCCESS로 완료됐다.
정본은 `twelve-hour-20261026-03/cells/cost_gate__all/01515795-073e-400d-862e-7a4f6237314b/cost-gate.json`이다.
남은 비용에 20% 여유와 보고 시간을 포함한 예상 종료는 **2026-10-27 04:51:59 KST**다.
입장 기준 06:25:05와 고정 종료 기한 07:55:05 이내다. 이는 예측이며 실제 완주 증거는 아니다.

| 실측 구성요소 | PostgreSQL | Qdrant |
|---|---:|---:|
| 200질의 시점 비용 추정 | 15.37초 | 8.15초 |
| 600질의 시점 비용 추정 | 43.32초 | 10.15초 |
| 검색 없는 wave의 상태 검증 비용 추정 | 1.86초 | 6.91초 |
| 5k 적재+구축 | 3.86초 | 4.13초 |
| 10k 적재+구축 | 8.29초 | 10.65초 |
| 20k 적재+구축 | 18.23초 | 31.25초 |

질의 시점 비용은 검색 지연만이 아니라 워밍업·현재 상태 검증 등 point 전체 비용이다.
작업별 표본이 5개 미만이면 최대, 그 이상이면 p90을 사용한다.
미측정 교체 작업에는 관측 쓰기 비용의 2배를 적용하고 필터도 보수적인 비용 대용값을 사용한다.
노드당 시작·복원·정리 등에는 30초를 할당했다. 독립 실측
`native-overhead-audit.json`의 native 외부 비용은 21개 DB 작업에서 p90 약 3.01초였고,
`component-overhead-audit.json`의 8개 스모크 전체 잔여 비용 최대값은 13.90초였다.
후자는 쓰기·검색·상태 검증·구축·추가 ef 측정을 제외한 시작·복원·정리 비용을 포함한다.

양 DB의 50회 churn 이력은 승인된 66개 중 2개의 완료로 포함한다.
각각 최종 600질의 Recall@100은 PostgreSQL 98.9683%, Qdrant 99.6983%였다.
두 DB 모두 shortfall@100은 0이었다. 같은 최종 데이터의 fresh 대조군 비교가 아직 남았으므로
이 수치만으로 갱신 이력의 효과나 제품 우열을 확정하지 않는다.
현재 이후 본 실험은 단계별 native barrier 순서대로 실행한다.

### 22:09 KST 잔여 비용 점검

2026-10-26 22:09:01 KST에 이력 39/66개, fresh 6/138회의 native 성공을 확인했다.
`twelve-hour-20261026-03/progress-forecast-20261026T220901.json`은 기존 스모크 모델을
유지하고 native SUCCESS인 작업의 비용만 차감한 관측 기록이다. 실행 중 작업은 전액,
추가 필터·ef 측정은 이미 끝난 부분도 포함한 원래 예산, 보고 30분과 20% 여유를 유지했다.
잔여 예측은 약 6시간, 예상 종료는 2026-10-27 04:08:58 KST다.
입장 검사나 실행 설정을 변경하지 않았으며, 이 예측은 실제 완주 보장이 아니다.

### 23:14 KST 잔여 비용 점검

2026-10-26 23:14:45 KST에 이력 58/66개, fresh 61/138회의 native 성공을 확인했다.
`twelve-hour-20261026-03/progress-forecast-20261026T231445.json`에 같은 방식의
비용 계산과 현재 공간을 기록했다. 잔여 예측은 약 3.845시간, 예상 종료는
2026-10-27 03:05:27 KST다. 진행 중 작업을 전액 남기고, 완료된 ef 측정의 비용도
차감하지 않은 보수적 계산이다. 원래 마감 07:55:05 KST는 변경하지 않았다.
호스트 여유는 약 39.9GiB, 소유 결과 3.39GiB와 활성 예약 2GiB의 합계는 약 5.39GiB다.
추가 ef 측정 24조건은 native 성공 결과에서 ANN 검사 PASS를 확인했다.
본 필터 측정 64조건과 최종 완료 감사는 이 시점에 아직 실행 전이다.

## 집단별 표본 수 보고 보완 — 완료

완료 체크리스트와 보고 코드를 대조하면서 `twelve_report.py`가 집단별 Recall은
보존하지만 집단별 질의 수를 출력하지 않는 점을 확인했다. 영향 범위는 이력 316개
측정 시점과 66개 최종 이력의 집단별 보고다. 원시 검색 결과와 입력에 고정한 집단
라벨은 보존돼 있어 DB 재측정 없이 재분석할 수 있다.

`scripts/supplement_twelve_report.py`는 독립 완료 감사 PASS와 활성 lease 부재를
선행 조건으로, 집단별 표본 수·Recall·최종 inc–fresh 대응 차이·질의 bootstrap·
fresh 3회 범위를 별도 `cohort-supplement/`에 기록하도록 작성했다.
동결된 측정 소스나 원시 결과는 변경하지 않는다. 전체 질의 600개와 중간 200개
부분집합의 위치, 집단 라벨의 native 해시, 대응하는 데이터·질의·원장 일치를 검사한다.

담당은 현재 실험 에이전트다. DB 측정 종료 뒤 보완 검사 4개를 포함한 관련 검사
14개를 실행해 모두 통과했다. 실제 완료 결과로 보완 보고도 실행했고,
316개 시점·66개 최종 이력·89,600질의의 집단 수와 원시 결과 일치를 확인했다.
보완 코드를 작성한 시점에는 미검증이었으며, 최종 실행 결과는 아래에 구분해 기록한다.

```sh
/opt/homebrew/bin/python3 -m unittest discover -s tests -p 'test_twelve_report_supplement.py'
/opt/homebrew/bin/python3 -m scripts.supplement_twelve_report real-results/twelve-hour-20261026-03
```

## 최종 완료 — 2026-10-27

본 측정과 native 집계는 **00:51:01 KST**, 원래 T0부터 **4시간 55분 56초**에 끝났다.
controller는 00:51:09 KST에 COMPLETE_SELECTED_SCOPE로 종료했고 guardian은
233개 작업을 오류 없이 RECONCILED 처리했다. 독립 감사는 00:51:34 KST에 PASS였다.
고정 마감 07:55:05 KST는 변경하지 않았다.

이후 집단별 보완 보고·오류 주입 검증·[해석 보고서](results.md)를 완료했다.
최종 문서 검증 시각과 해시는 현재 결과 루트의 `delivery-receipt.json`에 기록했다.

| 계획 체크 | 실행 명령 / 실제 출력 | 증거 |
|---|---|---|
| 실행 소유자·부모 사망·timeout 정리 | 사전 실제 helper 실행·종료 주입 PASS; 세부 명령은 사전 receipt | root01 `prelaunch-verification.json`, `owner-probe.json`, `safety/receipt.json`, `20261026T111000Z-watchdog-checks/receipt.json` |
| 양 DB 비용 스모크·입장 | native `cost_gate:all`: PASS, 21:03:31 KST; 예측 종료 04:51:59 KST | `cells/cost_gate__all/01515795-073e-400d-862e-7a4f6237314b/cost-gate.json` |
| 정확한 전체 목록·native 증거 | `.venv/bin/python -m scripts.audit_twelve_completion real-results/twelve-hour-20261026-03`: PASS | `independent-completion-audit.json`: 233 native, 66 histories, 316 history points, 138 fresh, 64 filter, 24 ef, 190000 queries |
| oracle 열·PG seqscan·Qdrant fallback·결과 누락 | 측정 Python의 unittest runner로 아래 지정 모듈/케이스 실행: 14 tests, 0 failures/errors/skips | `postmeasurement-tests.log`, `postmeasurement-tests.json` |
| 동결 source hash 변경 | `.venv/bin/python -m scripts.check_twelve_source_gate real-results/twelve-hour-20261026-03`: PASS | `source-gate-diagnostic/receipt.json`: 정상 native 성공, 변조 native 의도된 실패, worker 등록·결과 생성 없음 |
| 집단별 수·inc/fresh 대응 | `/opt/homebrew/bin/python3 -m scripts.supplement_twelve_report real-results/twelve-hour-20261026-03`: PASS | `cohort-supplement/supplement-receipt.json`: 316 points, 66 finals, 89600 queries |
| 한 번/5회 비교·결과 해석 | native report PASS; 해석 문서와 링크·출처 수치 확인 | `cells/report__all/28a40ac9-c93e-4e56-b81d-840cc46f5851/`, `docs/real-vectors/12h/results.md` |
| 원래 기한·문서 완료 | 최종 delivery 검사 PASS 및 종료 시각 < 1793055305 | `delivery-receipt.json` |

추가 14개 검사는 `unittest.defaultTestLoader.loadTestsFromNames`로 다음을 선택했다.
`OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 /opt/homebrew/bin/python3`에서 실행했으며
전체 suite의 불필요한 재실행이나 DB 측정과의 겹침은 없었다.

```text
test_twelve_report_supplement
test_twelve_completion_gates
test_realstudy_twelve_safety.FilterBoundaryTests
test_realstudy.AdmissionTests.test_wrong_metric_seqscan_and_fallback_plan_rejected
test_study_contracts.TelemetryContract
test_realstudy_oracle_identity
```

소스 변경 검사에서는 본 측정 소스를 바꾸지 않았다. 별도 fixture의 source 내용만
변경한 두 번째 native 실행에서 `frozen workload sources changed` 오류를 확인했다.
이 의도된 실패는 본 캠페인 native 실패 0개와 별도다. Dagster partitioned asset check의
preview 경고는 남아 있으며, 실행 실패로 분류하지 않았다.

집단 보완과 해석 보고서용 코드·문서만 후속 작성했다. 동결된 `realstudy/`, `study/`,
`pilot/` 측정 소스 및 native 원시 결과는 바꾸지 않았다.
완료 뒤 호스트 여유는 약 36.3GiB, `real-results/` 전체 논리 크기는 약 3.96GiB였다.
현재 캠페인의 활성 lease는 0이고 이전 실패 컨테이너·볼륨은 격리 증거로 보존한다.
기존 전체 연구의 연기 항목은 승인 계획대로 미완이다.
