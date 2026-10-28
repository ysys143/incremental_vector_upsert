# 실험 문서 목차

- [클러스터별 증분 삽입 기술보고서 — 문제·방법·결과·한계](real-vectors/clustered/technical-report.md)


문서는 데이터와 실험 단계별로 구분한다. 각 단계 안에서 계획, 실행 기록, 결과를 함께 찾을 수 있다.
최신 공개 벡터 실험은 [클러스터별 삽입 결과](real-vectors/clustered/results.md)다.
48이력·120fresh·480조건을 완주했으며, 원래 12시간 완료 목표는 미달했다.
기존 [12시간 실험 결과](real-vectors/12h/results.md)는 별도 실험이다. 어느 결과도 전체 연구 범위의 완료를 뜻하지 않는다.

## 공개 벡터 실험 — real-vectors

### 클러스터별 삽입 후속 실험

- [재집계 — fresh·1회·5회 삽입 및 삽입 전후 비교](real-vectors/clustered/three-way-comparison.md)

- [대규모 증분 갱신 후속 과제 — 리소스 확보 후 재개](real-vectors/clustered/large-scale-follow-up.md)

- [완료 결과 — 수치·한계·원시 증거](real-vectors/clustered/results.md)
- [클러스터별 삽입 최초 계획과 실행 중 변경](real-vectors/clustered-insertion-plan.md)
- [실험표 — 실행 manifest 생성의 기준](real-vectors/clustered-insertion-matrix.json)
- [실행 기록과 게이트 상태](real-vectors/clustered/execution.md)
- [요구사항별 완료 감사표](real-vectors/clustered/completion-checklist.md)

### 완료된 12시간 실험

| 문서 | 내용 |
|---|---|
| [결과](real-vectors/12h/results.md) | 관측값, 해석의 한계, 원시 증거 링크 |
| [계획](real-vectors/12h/plan.md) | 승인 범위와 완료 기준 |
| [실행 기록](real-vectors/12h/execution.md) | 실행·실패 조치·최종 완료 검증 |
| [시나리오 목록](real-vectors/12h/matrix.json) | 계획의 66개 시나리오 목록; 실행 설정 파일은 아님 |

### 전체 설계와 선행 실행

| 문서 | 내용 |
|---|---|
| [전체 연구 계획](real-vectors/plan.md) | 공개 임베딩, 삽입·삭제·upsert, 필터, DAG 설계 |
| [선행 실행 기록](real-vectors/execution.md) | 데이터 준비부터 기본 실험 중단·계획 개정까지 |
| [진입 스모크 결과](real-vectors/entry-smoke.md) | 본 실험 이전의 검증과 한계 |
| [기존 기본 실험 계획](real-vectors/baseline-plan.md) | 12시간 개정 이전의 실행표 |
| [기본 실험 실패 조사](real-vectors/baseline-failure-20261026.md) | 중단 원인과 근거 |

선행 문서의 승인 대기·미실행 표기는 작성 당시 상태다. 이후 완료 여부는 위 12시간 실험 문서를 따른다.

## 합성 데이터 삽입 실험 — synthetic-insertion

- [계획](synthetic-insertion/plan.md)
- [결과](synthetic-insertion/results.md)
- [실행·검증 기록](synthetic-insertion/execution.md)
- [진행 상태 확인·자동 후처리](synthetic-insertion/operations.md)

합성 데이터에서 측정한 결과다. 공개 임베딩 실험과 구분해서 해석한다.

## 최소 로컬 파일럿 — pilot

- [계획](pilot/plan.md)
- [결과](pilot/results.md)
- [실행 기록](pilot/execution.md)
- [검증 명령과 한계](pilot/verification.md)

## 이론·선행연구 — research

- [이론과 판정 기준](research/theory.md)
- [참고문헌](research/references.md)
- [선행연구 수치 원문 확인](research/literature-audit.md)
- [제품 수준 벤치마크](research/product-benchmarks.md)

## 아카이브와 원시 증거

- [이전 계획과 문서 이동 전 원본](archive/README.md)
- 원시 실험 결과는 저장소 루트의 `results/`, `study-results/`, `real-results/`에 보존한다. 이 문서 디렉터리에는 복사하지 않는다.
- 2026-10-27에 문서 경로와 참조 코드만 정리했다. 과거 제출 receipt와 source snapshot은 갱신하지 않았다. 이전 해시와 경로는 [이동 기록](archive/layout-before-20261027/README.md)에서 확인한다.
