# 후속 삽입 연구 실행 기록

## 실행 범위

승인된 로컬 계획은 2 DB·3 데이터 seed·132개 경로, 증분 checkpoint 504개와 fresh
대조군 384개다. 현재 본 캠페인은 `study-results/campaign-20261025-01`이며,
완료 여부는 native Dagster run·ledger·ack·최종 검증 보고서로 판단한다.
이 문서의 실행 예고나 준비 완료는 측정 완료의 증거가 아니다.

## just-dag-it 적용

- Dagster 1.13.26을 로컬 캐시에서 Python 3.13 가상환경에 설치했다.
- 측정은 기존 Python 3.14.7·NumPy 2.5.2에서 실행한다. 별도 API 과금 LLM은 호출하지 않는다.
- fixture 검증·튜닝·초기 snapshot·증분 경로·fresh 대조군은 partitioned asset이다.
  명시적 partition mapping으로 의존성을 연결하며 동시 DB 실행 수는 1이다.
- 상시 daemon·스케줄·추가 에이전트는 실행하지 않는다. controller와 독립 watchdog이
  deadline 및 부모 프로세스 사망 후 owned Docker 자원을 정리한다.

## 사전 검증

- 독립 단위·증거 검사는 `PILOT_SMOKE_ROOT=results/20261024T185358Z-smoke-7376a5
  python3 -m unittest discover -s tests -v`로 최종 31개 통과했다.
- `results/20261024T205108Z-study-engine-checks`: native materialization, 완료 셀
  재실행 방지, 실제 작업 실패의 자동 재시도 금지, 실패 셀만 명시적 재개, 미선택 셀
  NOT_STARTED 보존, 다른 셀의 native 성공 run 차용 거부, 실제 timeout, 실패·중단 후
  독립 Docker 정리, 반복 정리의 멱등성 등 10개 검사 PASS.
- `results/20261024T204034Z-parent-death`: 부모 controller를 종료한 뒤 별도 watchdog이
  실제 owned container와 volume을 정리하는 경로 PASS.
- `results/20261024T204750Z-study-io-checks`: 양쪽 DB의 초기 인덱스 복원,
  PostgreSQL OID와 seqscan off, Qdrant segment UUID·그래프 파일 해시, 손상 archive
  거부, HNSW 카운터 증가 및 실제 plain/exact 양성 대조군 거부 PASS.
- Qdrant 상세도 3은 segment 정보를 주지 않았다. 상세도 4와 고정 버전의 unused
  counter 생략 규칙을 공식 소스·실제 응답으로 확인했다. 구조 누락은 0으로 보지 않는다.

## 준비 중 발견한 실패와 조치

1. 첫 orchestration timeout 검사에서 SIGTERM 후 이미 종료된 자식 group에 SIGKILL을
   보내 PermissionError가 발생했다. 종료 확인 후에만 추가 신호를 보내도록 수정했다.
   실패 증거는 `results/20261024T203635Z-study-engine-checks`에 보존했다.
2. Qdrant의 정상 SIGTERM 종료 코드 143을 공통 exit=0 조건이 거부했다. flush 대기·
   graceful 로그·OOM/종료 상태와 복원 후 UUID·그래프 해시까지 확인하도록 보강했다.
   5초 keep-alive 뒤의 새 읽기에는 명시적으로 연결을 갱신한다. 쓰기 재시도는 하지 않는다.
3. 실제 스모크가 48개 셀 완료 후 PostgreSQL 임시 init 서버 종료와 COPY가 겹쳐 실패했다.
   `pg_isready`에 더해 PID 1이 최종 postgres인지 확인하도록 수정했다.
   실패 run `17cf4458-6ac2-4ad6-8bf0-c3c6e6dd7f2d`와 최초 stdout/stderr는 보존했다.

세 번째 조치는 데이터·정답·검색 설정 및 실험 handler AST 본문을 바꾸지 않았다.
`study-results/entry-20261025-02/amendments/20261024T210740Z-6e1a62/amendment.json`에
정확한 변경 파일·이전/현재 소스 snapshot·보존한 48개 성공 run과 ack/receipt hash를
기록했다. 과거 manifest와 원시 결과를 덮어쓰지 않았다. 실패한 fresh 셀만 새 owned DB로
재시도했고 나머지 완료 셀은 건너뛰었다. 단계의 원래 시간 한도도 유지했다.

## 새 스모크 완료

`study-results/entry-20261025-02`의 native summary run은
`88796f4e-97ac-4754-a6fe-2e6ea6755d6f`이며 PASS다.

- native 셀 85/85 PASS. 이전 실패 attempt는 이력으로 남아 있다.
- 증분 경로 16개, checkpoint 64개, fresh 56개, 총 120개 품질 측정 검증 완료.
- native summary의 소스 revision은
  `15eefe73d3700a4a1dbe184510ae7251223f71359cfb317ebdfb9d06b1d7f252`다.
- 본 캠페인은 이 summary의 native 성공·입력 scope·effective source hash를 확인해야 시작한다.

## 본 캠페인

입력·oracle·geometry 준비 결과가 계획 개수와 일치했다.
데이터는 초기 10,000건·128차원이며 모든 고유 상태의 exact 정답을 DB 실행 전에 계산했다.
준비 직후 본 캠페인 파일은 약 177MiB, 호스트 여유는 약 37GiB였다.
이후 실측·완료 수는 campaign ledger 및 최종 보고서에 기록한다.

```sh
python3 -m study.prepare --out study-results/campaign-20261025-01 --smoke-root study-results/entry-20261025-02
STUDY_ROOT="$PWD/study-results/campaign-20261025-01" .venv/bin/dagster definitions validate -f study/definitions.py
.venv/bin/python -u -m study.launch study-results/campaign-20261025-01
```

WDL 프로젝트 전용 검사는 사용자의 기존 `without-setup` 선택에 따라 NOT RUN이다.
전체 연구의 다른 DB·실제 임베딩·필터·장기 삭제/upsert·클라우드 비교는 별도 보류 항목이다.

## 자동 후처리와 읽기 전용 상태

원시 측정과 코드 동결 범위 밖의 `analysis/`에 후처리를 분리했다.
스모크 결과로 native run·최종 asset check 감사 및 파생 CSV 집계 경로를 실행했다.
`results/20261024T224229Z-postprocessing-checks/receipt.json`의 4개 검사에서는
실패·불완전 종료·native 감사 command 실패·보호된 CSV 변조가 결과 발행을 차단했다.

본 캠페인의 `analysis/finalization.json`에 후처리 프로세스가 실제 admission되었다.
전체 측정 종료 전에 기하 진단을 계산하거나 최종 결과를 발행하지 않는다.
상태와 생성 조건은 [진행 상태 확인·자동 후처리](operations.md)를 따른다.
