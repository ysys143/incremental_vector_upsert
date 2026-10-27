# 공개 벡터 증분 갱신 연구 실행 기록

## 현재 실행: 기본 실험 준비와 자동 드라이버

`baseline-setup-20261026-02`가 실행 중이며, 공통 calibration을 통과하면
`baseline-campaign-20261026-02` 드라이버가 데이터셋/seed 조각 6개를 순서대로 실행한다.
기본 조건은 seed 11·29·47, 평가 질의 600개, 이력 348개·이력 checkpoint 4,212개다.
fresh 수는 준비한 고유 원장의 최종/중간 상태 기준으로 확정한다.
기하·순서·연속 쓰기·회복·온라인·재튜닝·필터 확장은 미완이며 이 기본 단계가 전체 완료는 아니다.
세부 실행표는 `real-vector-baseline-phase-plan.md`를 따른다.

첫 setup은 m=64,ef_construction=400의 20,000개 인덱스에서 120초 DDL timeout으로
실패했다. 입력·정답 2개 native 작업과 barrier는 성공했다. 이 입력은 재사용하고 부분
calibration 65개 값은 새 최종 선택에 쓰지 않는다. 재개 setup은 원래 실행 창을 유지하며
64개 native 구축/상태 그룹으로 나눴다. DDL 600초·검색 120초 및 실제 병렬 구축/공유 메모리
설정을 분리했다. 수정 후 오프라인 검사 61건, 실제 resource sampler probe가 통과했다.

## 추가 공간 반환

기본 SSH CLI는 이 실행 환경에서 `No user exists for uid 501`, UI 터미널 도구는 service
startup 실패였다. 설치된 libssh2와 같은 기존 loopback Colima 프로필·저장된 identity로
접속해 VM이 살아 있음을 확인했다. 파일시스템은 /dev/vdb1, 사용 약 8.3GiB·가용 약 47.4GiB였다.
`sync && sudo -n fstrim -av`는 게스트 미사용 블록 40.9GiB trim을 보고했고, 호스트 여유는
15,529,467,904 → 51,961,249,792 bytes로 약 33.93GiB 증가했다. 파일·볼륨·이미지·서비스·권한을
변경하지 않았다. 원시 기록은 `colima-unused-block-reclaim-verified-20261026.json`이다.
기존 호스트 기준선을 유지하고, 별도로 real-results의 소유 파일과 활성 예약량에도 8GiB
상한을 적용해 trim 이후 호스트 여유 증가가 실험 상한 초기화로 해석되지 않게 했다.

## 2026-10-26 실제 수집 재개

`source-20261026-02`는 두 native 작업과 필수 검사를 통과해 완료했다. MS MARCO 기본
100,000개·질의 9,376개, Wikipedia 기본 100,000개·질의 5,000개를 원본 768차원 float32로
확보했다. `smoke-20261026-01`은 71개 native 노드·필수 검사·112개 품질 측정을 모두
완료했다. 현재 `request-smoke-20261026-03`에서 N=10,000으로 다시 튜닝하고 삽입
10,000개 및 기타 변경 5,000개를 실제 단일 wave로 검증한다. 이 상태는 전체 본실험 완료가 아니다.

`source-20261026-01`은 Wikipedia block 12에서 기대한 9,600,000 bytes 대신
392,794 bytes만 저장돼 실패했다. 당시 curl stderr가 보존되지 않아 그 응답의 원인은
확정하지 못한다. controller가 다른 수집 작업도 종료했으며 DB 실험은 시작하지 않았다.

`source-20261026-02`는 같은 source deadline과 최초 공간 기준선을 계승한다.
MS MARCO의 검증 완료 기본 파일은 새 producer가 전체 해시와 구간 원장을 독립 검증해
raw cache로 재사용한다. 이전 native FAILURE를 성공으로 차용하지 않는다.
HTTP 조각을 1MiB로 제한하고 전송 오류·일부 일시 HTTP 오류에만 최대 3회 GET을 허용한다.
각 응답 상태·바이트 수·ETag·범위·curl stderr를 보존하고 원본 식별자가 달라지면 재시도하지 않는다.
이는 HTTP 전송 단위 변경이며 이후 DB에 넣는 wave 크기를 바꾸지 않는다.

재시도 로그에는 `curl: (56) Recv failure: Connection reset by peer`가 기록됐으며,
해당 조각은 다음 시도에서 검증됐다. 이 로그만으로 연결을 초기화한 서버나 중간 장비를
식별하지는 못한다. 변경 후 오프라인 전송 오류 검사 포함 48건이 통과했다.

## 현재 판정

전체 실험은 미완이다. 기존 합성 데이터 실험은 보존하며 공개 임베딩 실험의 결과로 사용하지 않는다.

2026-10-26 사용자 `proceed anyway` 지시로 실행을 재개했다. `realstudy`의 여유 하한을
10GiB로 변경하고 추가 사용량 상한 8GiB를 유지했다. 변경 후 오프라인 46건 통과.
원래 수집 시간 창은 만료됐으며 새 `source-20261026-01`에서 4시간 상한으로 수집한다.
새 manifest와 `execution-authorization.json`에 재개 지시·이전 종료 시각·새 실행 창을
기록했다. 아래 공간 차단 기록은 재개 전 이력이며 현재 실행 상태와 구분한다.

최근 공간 상태(2026-10-26 사용자 `prunce cache` 요청 후): `uv cache clean --offline`은
170,619개 파일·5.3GiB 제거를 보고했고, `npm cache verify`는 미참조 12개·50,190,254 bytes를
제거했다. uv 캐시 경로가 사라진 것을 확인했다. Dagster 및 NumPy import는 모두 통과했다.
현재 호스트 여유는 약 19.74GiB이며 기존 수집·스모크 공간 목표까지 최소 약 7.87GiB가
추가로 필요하다. 논리 제거량과 호스트 회수량은 구분한다. 명령·원시 출력·전후 용량은
`real-results/readiness-20261025/cache-clean-user-20261026-summary.json`에 보존했다.

| 단계 | 상태 | 근거와 한계 |
|---|---|---|
| 공개 원본 확인 | PASS | `real-results/readiness-20261025/{msmarco,wikipedia}/probe.json`: 공식 원본의 헤더·ETag·차원 확인 |
| 거리 함수 확인 | 제한적 확인 | `prefix-diagnostics.json`: 각 원본의 첫 64개만 비교. IP와 L2의 결과 차이를 확인했으나 대표성 있는 품질 실험은 아님 |
| 소스 수집 1차 | STOPPED_INCOMPLETE | `real-results/source-20261025-01`: 2개 native attempt 실패로 종료, lease 0개. 데이터 파일 작성 0개. 코드 경계 수정 사유와 원래 상한은 `owner-stop-disposition.json`에 보존 |
| 공개 표본 수집 재시도 | PASS | `source-20261026-02`: 2개 native SUCCESS·필수 검사·원본 표본/질의 파일 검증 |
| 실데이터 DB 진입 스모크 | PASS | `smoke-20261026-01`: 71개 native SUCCESS·필수 검사, 28개 경로·112개 품질 측정 |
| 10,000개 규모 요청 스모크 | PASS | `request-smoke-20261026-03`: 71개 native 노드·84개 품질 측정, 변경 없는 입력/PG 13개 결과 재사용, 원래 시간 창 내 종료 |
| 기본 이력 실험 | WAITING FOR CALIBRATION | seed 3개·질의 600개 입력 준비 완료, 분할 calibration 진행, 6개 실행 조각 드라이버 대기 |
| 전체 계획 완료 | 미완 | 기본 이력 실행과 기하·순서·연속 쓰기·회복·온라인·재튜닝·필터 확장 미완 |
| 필터·선택도·카디널리티 확장 | NOT RUN | 실제 필터 경로와 ANN 증거를 아직 검증하지 못함 |

## 구현·검증 경계

`realstudy/`에 원본 표본 수집, IP exact oracle, 현재 live set 및 변경 이력,
COPY 적재·staging COPY upsert, 리소스 입장 제어와 native Dagster 실행 경로를 구현했다.
`realstudy/actions.py`에 진입 스모크 작업을 연결했다. 전체 5회·50회 연구와 필터 작업은
아직 연결하지 않았으며, 이 모드로 호출하면 명시적으로 거부한다.
오프라인 adapter 검사는 실제 PostgreSQL 또는 Qdrant 실행 증거가 아니다.

수정한 실행 경계는 다음과 같다.

- 의존 데이터의 대용량 해시 검증은 해당 worker의 자원 잠금을 획득한 다음 수행한다.
- native blocking check의 해시 검증이 끝날 때까지 worker의 자원 잠금을 유지한다.
- controller 상태 조회는 receipt와 native 결과를 확인하며 매번 대용량 원시 파일을 읽지 않는다.
- 모든 선택 작업이 종료되고 잠금이 없는 것을 확인한 뒤 최종 원시 파일을 검증한다.
- controller 종료 정리는 blocking check 도중 남은 잠금도 회수한다.

검증 명령:

```sh
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 python3 -m unittest tests/test_realstudy.py tests/test_realstudy_registry.py tests/test_realstudy_consumers.py tests/test_realstudy_writes_storage.py tests/test_realstudy_oracle_identity.py tests/test_realstudy_read_retry.py tests/test_realstudy_smoke.py -q
.venv/bin/python -m realstudy.engine_checks
.venv/bin/python -m realstudy.queue_checks
```

스모크 단계 수정 후 첫 명령은 40건 통과했다. 명령·출력·코드 해시는
`real-results/readiness-20261025/smoke-offline-verification-02.json`에 보존한다.
앞선 39건 검증은 `smoke-offline-verification.json`에 별도로 보존한다.
native consumer 검사는
`real-results/20261025T145700Z-engine-checks/receipt.json`에 PASS를 기록했다.
이 검사는 4개 실제 native run과 blocking check, 중복 실행 거부 및 변조된 의존 데이터 거부를
확인했으며 DB 시험은 수행하지 않았다.
추가 코드 수정 후 재실행도 `real-results/20261025T151000Z-engine-checks/receipt.json`에
PASS를 기록했다.
조기 잠금 해제를 주입해 새 검사가 실패하는 증거는
`real-results/readiness-20261025/lease-boundary-mutation.json`에 보존했다.
수정 후 자동 큐 검사는 `real-results/20261025T150000Z-queue-checks/receipt.json`에
PASS를 기록했다. 실제 native run 7개가 완료됐고 CPU worker 최대 2개 병렬 실행,
준비→DB→보고 의존성, DB blocking check를 3초 지연한 동안 다른 무거운 worker의
비중첩, 모든 잠금 해제와 소유 daemon 종료를 확인했다. DB 작업은 fixture이므로
실제 PostgreSQL·Qdrant 측정으로 세지 않는다.
공간 기준선 처리 추가 후 재실행도 `real-results/20261025T151027Z-queue-checks/receipt.json`에
7개 native run PASS와 guardian RECONCILED를 기록했다. 이후 변경한 입력 중복 처리의
증거는 오프라인 consumer 검사이며 실제 데이터 판정은 아니다.

직전 검사 `20261025T145729Z-queue-checks`는 실행 중 `source_campaign.py`를 수정해
동결 소스 조건이 깨졌으며 native run `b5b17eb3-aeab-4cc2-a425-d180306a259b`가
실패했다. 해당 실행을 중단하고 원래 실패 기록을 보존했다. 이를 PASS로 대체하지 않는다.

## 공간·시간 경계

호스트 여유 공간 하한은 25GiB, 추가 사용량 상한은 8GiB다.
최근 실제 스모크 진입에서 관측한 여유는 약 19.26GiB로 하한 미달이다.
`smoke-space-admission.json`은 실제 진입 함수가 예약량 2GiB를 포함해 거부했으며
출력 디렉터리를 만들지 않았다는 증거다. DB 스모크 성공을 의미하지 않는다.
처음에는 `uv cache prune` 승인을 요청했으나, 사용자 공용 규칙의 재생성 가능한 캐시 예외가
적용됨을 재확인했다. 2026-10-26 `uv cache prune --offline`을 강제 옵션 없이 실행했고
35,213개 파일, 130.0MiB 제거를 보고했다. 설치 환경·원본 증거·소스·이미지·DB 볼륨은
삭제하지 않았다. 명령 전후 호스트 여유는 20,702,203,904 → 20,719,550,464 bytes로
약 17MiB 증가했다. 공유 블록과 동시 쓰기를 분리하지 못했으므로 논리 제거량을 실제 회수량으로
해석하지 않는다. `uv-prune-20261026-01.json`에 원시 출력을 보존했다.
원래 소스 단계 deadline은 Unix UTC `1792948869.4848042`이며 재개로 연장하지 않는다.
`real-results/source-20261025-02`는 원래 입력 버전과 종료 시각을 계승해 준비했으나
공간 하한 미충족으로 시작하지 않았다. 이후 소스가 추가 수정됐으므로 이 디렉터리는
소스 동결 조건상 그대로 실행할 수 없다. 실제 재개 시 새 디렉터리에서 원래 종료 시각을
계승해야 한다.

공간 원장은 첫 입장 성공 시의 호스트 여유를 SQLite 트랜잭션 안에서 한 번만 기록한다.
입장이 거부된 낮은 공간 상태는 기준선으로 삼지 않는다. 예약 공간을 포함한 호스트 사용량
증가를 8GiB와 비교하고, 스모크는 소스 수집 단계의 원래 기준선을 계승한다.
따라서 새 캠페인을 만든다는 이유로 데이터 수집의 사용량을 제외하거나 상한을 초기화하지 않는다.
외부 프로세스의 디스크 변화도 호스트 관측값에 섞이므로 이것은 보수적 입장 제어이며,
실험 소유 데이터의 순수 물리 사용량 측정이라고 해석하지 않는다.

## 연결한 진입 스모크

두 데이터셋 × 두 DB × 삽입·삭제·churn·동일 ID 벡터 교체·동일 벡터 새 ID 재삽입·동일 ID
동일 벡터·메타데이터 변경의 7개 경로를 실행한다. 초기 1,000개, 768차원, wave당 100개,
2회 갱신, 평가 질의 60개이며 전체 연구의 5회·50회 요구를 줄이는 변경은 아니다.

데이터별 표본 및 모든 checkpoint oracle 준비 → 준비 barrier → DB별 튜닝 → 튜닝 barrier
→ 초기 물리 snapshot 및 재시작 검사 → snapshot barrier → 28개 갱신 경로 → 이력 barrier
→ 28개 fresh → 보고서의 71개 노드를 정의했다. 84개 이력 checkpoint와 28개 fresh를
합친 112개 품질 측정이며 튜닝 및 snapshot 재시작 검사는 이 수와 별개다.

튜닝에는 disjoint한 calibration 문서·질의, 초기 배치 100/1,000, HNSW build
(16,100)/(32,200), ef 100/128/256/512를 사용한다. Recall@10/100 0.95/0.99 조건별
측정 p95·구축 시간의 Pareto 후보를 기록한다. 0.99 조건을 만족하지 못하면 진입을 거부한다.
1,000개 설정을 본실험 10,000개에 승계하지 않는다.

스모크 준비 경로에는 공통 checkpoint 저장소도 연결했다. 같은 ID·벡터·질의의 exact
정답은 공유하고, 메타데이터·버전이 다른 live-set 원장은 따로 보존한다. 원본 row 대응도
cache identity에 포함한다. 이미 만든 oracle 또는 receipt가 바뀌면 재사용을 거부한다.

분리는 원본 float32 바이트의 SHA256을 기준으로 한다. 문서 풀의 중복 벡터는 한 row만
선정하고, 중복 질의 및 문서 풀과 동일한 벡터 값을 가진 질의는 제외한다. calibration
문서는 모든 평가 쓰기에서 등장하는 문서와도 분리한다. 원본 row ID와 벡터는 유지한다.
유일한 문서·질의가 부족하면 진입을 거부한다. 실제 진단 수는 수집 후 기록하며, 이 검사로
의미적 중복이나 근접 중복이 없다고 주장하지 않는다.

스모크의 메타데이터는 row 번호를 10으로 나눈 실험용 라벨이다. 자연 메타데이터나
실제 문서 편집 이력이라고 부르지 않는다. 입력 및 모든 버전의 ID·벡터·메타데이터는
실행 중 전체 readback과 현재 live-set oracle로 확인한다.

`real-results/readiness-20261025/smoke-native-definition.json`은 Dagster의 실제 정의 로딩
검증 PASS이며, DB 실행 증거가 아니다. 첫 로딩에서는 orchestration 가상환경에 NumPy가
없어 실패했으며, 순수 설정 모듈을 분리한 뒤 같은 환경에서 통과했다. 추가 도구를 설치하지 않았다.

실제 소스 수집이 native SUCCESS와 blocking check를 통과한 뒤 다음 경로를 사용한다.
현재는 데이터·공간 전제 미충족으로 NOT RUN이다.

```sh
.venv/bin/python -m realstudy.smoke_campaign --out real-results/<새-smoke-ID> --source-root real-results/<성공한-source-ID>
.venv/bin/python -m realstudy.control real-results/<새-smoke-ID>
```

전체 본실험의 최종 공간 견적·과학 작업 DAG 연결, 실제 튜닝과 실데이터 스모크,
삽입 재실험 및 삭제·churn·동일 ID 교체·제어군·필터 확장은 미완이다.
기존 합성 데이터 결과와 오프라인 검사 통과로 이 항목들의 완료를 대체하지 않는다.

### 현재 실행 차단 판정

여유 25GiB 하한 미충족이 세 번 이상의 연속 goal turn에서 유지됐다. 실행 중인 수집·DB
작업은 없고 실제 수집한 `.fbin` 파일도 0개다. 원래 source controller 파일의 finished가
false로 남은 것은 성공/실행 중의 증거가 아니다. 해당 PID·daemon PID가 없고 guardian이
종료를 기록한 것을 함께 확인했다.

원본 벡터 파일은 두 데이터셋의 100,000개 표본 및 모든 질의 합계 658,563,104 bytes,
약 0.613GiB다. 현재 스모크의 예약량 2GiB와 하한을 함께 적용하면 수집 전 필요 여유는
적어도 27.613GiB다. 현재 약 19.3GiB에서 추가로 약 8.3GiB가 필요하다. 로그·receipt·snapshot과
다른 프로세스의 쓰기는 제외한 하한 계산이며 최종 전체 연구 공간 견적은 아니다.
현재 uv 캐시 약 5.76GiB, npm 다운로드 캐시 약 0.85GiB, Homebrew 캐시 약 0.04GiB는
디렉터리 측정값이다. 전체를 안전하게 회수할 수 있다는 뜻이 아니며 uv의 unused prune으로
공간 전제가 회복되지 않았다.

`space-block-audit-20261026.json`에 용량·선행 조건·미완 범위·보존 경계를 기록했다.
실제 입력의 집단 구조·튜닝·요청 크기·실행 시간은 실데이터 스모크가 다음 필수 전제이며,
추가 오프라인 검사로 그 전제를 충족했다고 처리하지 않는다. 추가 공간 확보 또는 사용자의
자원 조건 변경 결정 전에는 수집/DB 실행을 시작하지 않는다.

## 5회·50회 입력 이력과 checkpoint 준비

`realstudy/history_plan.py`는 원본 row 순서와 대상 슬롯을 받아 다음 변경을 실제 event
원장으로 만든다. 새 임베딩이나 벡터 변환은 생성하지 않는다.

- 삽입 10/25/50/100%, 삭제 10/25/50%의 한 번 대 5회 분할.
- churn·동일 ID upsert·동일 벡터 새 ID 재삽입·동일 ID 같은 벡터·메타데이터 제어군의
  10%씩 5회 대 한 번 50% 변경.
- 10%씩 50회 반복하는 교체 이력의 무작위 대상 및 고정 집중 대상.
- 외부에서 선정한 군집/연결 영역 대상 슬롯의 순서 보존.
- settled의 매 checkpoint와 continuous의 최초/최종 관찰 구분.

`Checkpoints.path`는 변경 원장과 현재 상태를 다시 재생해 최종 digest가 같은지 확인한다.
continuous 준비에서는 중간 정답을 계산하지 않으며, 준비 파일과 소비 측의 oracle identity를
검증한다. 지리적/위상적 집단 선정은 별도 미완 항목이다. 대상 슬롯의 입력을 받는다는
것은 실제 데이터에서 그 집단이 성립함을 확인했다는 뜻이 아니다.

다음 명령은 오프라인 46건을 통과했다.

```sh
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 python3 -m unittest tests/test_realstudy.py tests/test_realstudy_registry.py tests/test_realstudy_consumers.py tests/test_realstudy_writes_storage.py tests/test_realstudy_oracle_identity.py tests/test_realstudy_read_retry.py tests/test_realstudy_smoke.py tests/test_realstudy_histories.py -q
```

명령·출력·코드 해시는 `real-results/readiness-20261025/history-checkpoint-verification.json`에
보존했다. N=10,000에서 50회 × 1,000개, 총 50,000개 쓰기와 생존 수 10,000개 유지,
집중 upsert 대상 1,000개의 최종 version 50을 실제 event 재생으로 확인했다.
같은 최종 상태의 한 번/5회 비교와 중복·변조 입력 거부도 포함한다.
품질 계산은 작은 테스트 벡터로 검증했으며 실제 임베딩·DB 실험은 NOT RUN이다.
본실험 실행 DAG·최종 교차 조합표와 이 입력 준비 모듈을 연결하는 작업은 아직 남아 있다.
