# 기본 실험 중단 원인 조사 — 2026-10-26

## 판정

확인: 첫 pgvector 장기 집중 churn은 17:58:29 KST에 Dagster가 종료 신호를 수신하면서
중단됐다. 해당 시점은 controller/worker의 시간 제한 전이다. signal 번호와 발신 프로세스는
로그에 없어 확정하지 못했다. Codex 세션 초기화와 같은 시각에 발생해 실행 환경의
수명 전환으로 실험 프로세스들이 종료됐을 가능성이 높다. 이것은 시각 상관에 근거한 추론이다.

확인: 이후 PostgreSQL이 받은 COPY 입력은 실제 원본 벡터 문자열의 잘린 접두부였다.
49번째 갱신의 삭제만 반영된 부분 상태가 남았고, worker의 중단·정리 경로에도 결함이 있다.

## 시간순 증거

| KST | 관측 |
|---|---|
| 17:58:04.871 | point-48 측정 PASS. 초기 포함 49개 품질 point, ACK 완료 48회 |
| 17:58:05.077 | write-49 INTENT. 1,000개 삭제 후 1,000개 COPY 삽입 예정 |
| 17:58:29.558 | 현재 Codex session_meta의 초기화 시각 |
| 17:58:29.790 | Dagster: `Multiprocess executor: received termination signal - forwarding to active child processes` |
| 17:58:29.812 | admission 원장에 해당 시나리오 failed 기록 |
| 17:58:47.767 | PostgreSQL: invalid input syntax for type vector, COPY line 689 |
| 20:37:31.757 | 원래 controller 제한. 중단 시각보다 약 2시간 39분 뒤 |

## COPY 오류와 현재 DB 상태

COPY line 689에 대응하는 source pool row 44573의 벡터는 유한한 float32 768차원이다.
기존 인코더로 직렬화한 완전한 문자열은 9,743 bytes다. 서버 오류에 기록된 문자열은
그 값의 정확한 앞 9,598 bytes이며, 끝 145 bytes와 닫는 대괄호가 없다.
이 비교는 원본 벡터 자체가 잘못됐다는 설명을 지지하지 않는다.
종료 신호 뒤 입력이 잘려 COPY가 실패했다는 설명과 일치한다.

DB를 읽기 전용으로 재조회한 결과:

- items 행 수 9,000개.
- ID/version/category/ordinal 전체가 step-48 원장에서 wave-49 삭제 1,000개를 뺀
  집합과 정확히 일치한다.
- wave-49에 삽입할 신규 ID 1,000개는 하나도 없다.
- 현재 남은 9,000개의 벡터 값은 이번 진단에서 다시 전수 확인하지 않았다.
- 따라서 step-48도 step-49도 아닌 부분 반영 상태다. 실패한 쓰기를 자동 재전송하거나
  해당 volume에서 그대로 다음 wave를 시작해서는 안 된다.

## 종료 및 정리 결함

- controller/driver의 실제 PID는 사라졌지만 JSON은 RUNNING을 가리킨다.
  native runs도 SUCCESS 4, STARTED 1, QUEUED 57로 남아 있다.
- guardian receipt는 parent_alive=false, deadline_exceeded=false,
  log_cap_exceeded=false, cleanup=[]다.
- engine.py는 supervised 예외에서 attempt를 failed로 마감하고 finally에서
  watch.finished=true로 표시한다. 이 경우 supervision.json과 실제 자원 정리의
  완료 증거가 없는데도 watchdog이 끝날 수 있다.
- guardian.py는 ended가 있는 attempt를 건너뛰며, 현재 failed attempt는 lease도
  해제돼 자원 journal을 다시 확인하지 않는다. 그 결과 소유 PostgreSQL container와
  volume이 남았다. Docker State.OOMKilled=false이며 DB container는 계속 실행 중이다.
- 이 코드는 engine의 종료 신호를 발생시킨 원인으로 확인된 것은 아니다.
  중단 뒤 정리가 누락된 별도 결함이다.

## 영향 범위

실패 대상은 MS MARCO / seed 11 / pgvector / churn-long-hot 한 시나리오다.
48회까지의 ACK와 초기 포함 49개 point 파일은 남지만, 전체 시나리오 native SUCCESS와
필수 검사가 없어 완주한 결과로 승격하지 않는다. 49회 쓰기는 부분 반영, 50회는 미실행이다.
나머지 57개 기본 시나리오는 이 묶음에서 미실행이고 나머지 다섯 묶음도 시작하지 않았다.
source 수집, 기존 두 스모크, 튜닝 76개 작업의 성공을 이 실패로 무효화할 근거는 없다.

## 확인하지 못한 점

직접 signal을 보낸 프로세스와 signal 번호는 확인 불가다. macOS 시스템 로그 조회는
`getpwuid_r did not find a match for uid 501` 및 `Connection to logd failed`로 실패했다.
동시각 다른 원격 호스트의 Codex 연결 오류는 이 로컬 실험 중단과 연결할 근거가 없어
원인으로 사용하지 않았다. 사용자 네트워크·권한·데이터 오류로 단정하지 않는다.

## 조사 중 수행한 작업

Dagster/admission SQLite 읽기, 프로세스 생존 확인, Docker 상태/로그 읽기,
PostgreSQL SELECT, 입력 npz/fbin과 잘린 벡터 문자열 대조만 수행했다.
실험 재시작·DB 쓰기·코드 변경·기한 변경·container/volume 삭제는 수행하지 않았다.
보고서와 아래 진단 파일은 새로 작성한 조사 산출물이며 기존 성공 receipt를 대체하지 않는다.

원시 참조·해시와 판정: `real-results/readiness-20261025/baseline-failure-diagnosis-20261026.json`.
