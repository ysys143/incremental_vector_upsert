# 실행 상태 확인과 자동 후처리

## just-dag-it을 선택한 이유

단발 명령이 아니라 541개 셀의 의존성·완료 원장·부분 실패·시간 한도가 필요한
유한 실험이므로 Dagster를 선택했다. just-dag-it은 실행 라이브러리가 아니라 이런
반복 작업을 Dagster로 옮기는 스킬이며, 로컬 저장소의 스킬과 템플릿을 참고했다.
프로젝트의 실제 실행 코드는 `study/`에 있다.

상시 daemon이나 LLM 반복 호출은 없다. 입력 검증 16개, 튜닝 2개, 초기 snapshot 6개,
증분 경로 132개, fresh 384개, 최종 집계 1개가 native asset으로 실행된다.
원래 측정 로직은 별도 worker 명령이며, Dagster wrapper에서 다시 구현하지 않았다.

## 현재 실행을 읽는 방법

```sh
python3 -m analysis.status study-results/campaign-20261025-01
```

이 명령은 파일을 읽기만 한다. 새 실행·재개·정리를 하지 않는다.
`ledger_status`는 완료 원장이고, 최종 native 성공·asset check 감사는 아래 후처리의
`native-audit.json`으로 별도 확인한다. 제출된 작업 수를 완료 수로 세지 않는다.

- 캠페인: `study-results/campaign-20261025-01`
- 측정 controller: `controller.json`
- 완료·실패·미실행 원장: `ledger.json`
- 원래 시간 한도: `stage-clocks.json`
- native 이벤트: `dagster-native.log`, `dagster-home/`
- 후처리 상태: `analysis/finalization.json`
- 후처리 출력·오류: `analysis/finalization.log`

실패 셀은 자동 재시도하지 않는다. `NOT_STARTED`는 완료가 아니며, 성공한 셀은
native run·scope·ack·파일 해시를 검증한 뒤 건너뛴다. 단계의 원래 시간 한도는
중단·재개로 연장하지 않는다.

## 종료 후 자동 처리

`analysis.finish_campaign`을 별도 로컬 프로세스로 시작했다. 측정 중에는 상태만
읽고 대기한다. 전체 541개 셀과 controller의 전체 선택 완료를 확인한 뒤에만
다음 명령을 순서대로 실행한다.

1. `analysis.native_audit`: 모든 native SUCCESS와 최종 `complete_evidence` 검사 통과,
   scope·source revision·ack·artifact hash를 확인한다.
2. `analysis.geometry_cohorts`: 고정 입력의 기존·신규 집단별 NN1/NN20·LID 분포와
   신규→기존 거리 / 기존 내부 거리 비를 계산한다. DB 측정과 CPU를 경합하지 않는다.
3. `analysis.quality_summary`: 시점별·집단별 삽입 전후, fresh 대비, 한 번/다섯 번,
   삽입 순서 및 중간 관찰 정책 차이를 seed별로 집계한다.
4. `analysis.final_report`: 선언된 캠페인 Docker 자원의 잔존 여부까지 읽기 전용으로
   확인하고 `docs/synthetic-insertion/results.md`를 생성한다.

전체 후처리 시간 상한은 측정 완료 뒤 30분이다. 후처리 코드의 admission hash를
고정했으며, 측정·입력·원시 증거를 수정하지 않는다. 측정 실패·불완전 종료·controller
사망·기존 deadline 초과·검증 실패·코드 변경은 `FAIL`로 남기고 멈춘다.
후처리는 실험을 다시 실행하거나 실패한 쓰기를 재전송하지 않는다.

## 실제 확인한 경로

- 완료된 스모크의 native 감사 PASS: `study-results/entry-20261025-02/analysis/native-audit.json`
- 완료된 스모크 CSV의 파생 집계 PASS: `study-results/entry-20261025-02/analysis/quality-summary/receipt.json`
- `results/20261024T224229Z-postprocessing-checks/receipt.json`: 4개 경계 검사 PASS.
  불완전 controller 종료, 실패 측정, 실제 native 감사 subprocess 실패 및 ack로
  보호된 CSV 변조가 후처리를 차단했다. 의도적인 native 실패의 원본 로그도 보존했다.

이 검증은 본 캠페인 전체 완료의 증거가 아니다. 최종 보고서가 생성되기 전에는
측정·후처리 상태를 각각 확인해야 한다. 실제 데이터·필터·다른 DB·클라우드 등
보관된 전체 연구 범위를 완료했다고 해석하지 않는다.
