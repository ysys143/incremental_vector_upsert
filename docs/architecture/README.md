# 저장소 구조와 실행 경로 이관

이 구조는 앞으로 시작할 실행에 적용한다. 이전 실행의 `results/`, `study-results/`, `real-results/`는 기존 절대경로와 해시를 가진 증거로 보관한다. 이전 manifest, receipt, source snapshot, 승인 계획을 다시 쓰지 않는다.

```text
src/vectorbench/
  backends/                 PostgreSQL·Qdrant 어댑터와 서버
  datasets/                 공개 벡터 소스·pool·상태 저장·선택
  workloads/                이벤트·삽입/삭제·이력
  evaluation/               검색 결과·oracle·필터 측정
  orchestration/            프로세스·Dagster·자원 감독
  evidence/                 manifest 계약·registry·소스 동결
  reporting/                결과 집계·보고서
  experiments/
    pilot/                  초기 로컬 파일럿
    synthetic_insertion/    합성 삽입 실험 정의와 실행
    real_vectors/           공개 벡터 공통 실험 정의
      baseline/             초기 기준선과 튜닝
      twelve_hour/          승인된 12시간 범위의 정의와 검사
requirements/              기존 버전 고정 의존성의 정본
configs/                   새 실행 설정을 둘 위치
scripts/                   운영·검증 진입점
tests/                     기능·계약 검사
docs/                      과학 문서와 과거 계획 아카이브
runs/                      새 실행의 기본 출력 위치
results/                   이전 파일럿 증거
study-results/             이전 합성 실험 증거
real-results/              이전 공개 벡터 실험 증거
```

기존 코드 파일 81개의 전체 경로는 [모듈 이관표](module-map.md)에 있다. 실험별 판정 조건은 각 `experiments/`에 남겼다. 공통 소스 동결은 `evidence/source_manifest.py`에서 새 Python 트리와 운영 스크립트, 의존성 파일을 열거한다. 합성 실험은 계획과 참고 문헌도 동결한다. 새 manifest의 소스 키는 새 경로이며, 읽기·실행 시 동일 해시를 검사한다.

## 명령 변경

저장소 루트에서 `PYTHONPATH="$PWD/src"`를 설정하거나 `pyproject.toml`의 editable 패키지를 설치한다. 측정 Python과 Dagster Python이 다르므로 두 프로세스 모두 패키지를 읽을 수 있어야 한다.

| 이전 명령의 모듈·파일 | 새 모듈·파일 |
|---|---|
| `pilot.run`, `pilot.report` | `vectorbench.experiments.pilot.run`, `vectorbench.experiments.pilot.report` |
| `study.prepare`, `study.launch`, `study.worker` | `vectorbench.experiments.synthetic_insertion.prepare`, `.launch`, `.worker` |
| `study/definitions.py` | `src/vectorbench/experiments/synthetic_insertion/definitions.py` |
| `analysis.status`, `analysis.finish_campaign` | `vectorbench.reporting.synthetic.status`, `.finish_campaign` |
| `realstudy.source_campaign`, `.smoke_campaign` | `vectorbench.experiments.real_vectors.source_campaign`, `.smoke_campaign` |
| `realstudy.baseline_campaign`, `.baseline_shard` | `vectorbench.experiments.real_vectors.baseline.baseline_campaign`, `.baseline_shard` |
| `realstudy.twelve_campaign`, `.twelve_status` | `vectorbench.experiments.real_vectors.twelve_hour.twelve_campaign`, `.twelve_status` |
| `realstudy.control`, `.engine`, `.task` | `vectorbench.orchestration.control`, `.engine`, `.task` |

`scripts.*` 모듈 이름은 유지했다. 내부 import, 하위 프로세스의 `-m` 대상, Dagster 정의 파일 경로, 보고서 생성 진입점은 새 경로를 사용한다. 신규 기본 출력은 `runs/`이며, 명시적 `--out` 또는 위치 인자는 호출자가 지정한 새 디렉터리여야 한다.

## 이전 실행의 재개

이전 manifest의 소스 키는 이동 전 경로와 해시다. 새 코드는 그 키를 허용 목록에서 지우거나 해시 검사를 생략하지 않는다. 따라서 이전 실행을 이 코드로 재개하면 동결 소스 검사에서 거부된다. 이전 실행을 분석할 때는 보관된 `source/` 사본과 receipt를 근거로 삼고, 새로운 측정은 새 manifest와 새 실행 디렉터리를 만든다. 과거 문서에 적힌 당시 명령은 실행 이력으로 읽고, 현재 명령은 위 표와 루트 README를 따른다.

## 검증 기록

- AST 구문 검사: `src/`, `scripts/`, `tests/` Python 파일 129개 통과. `vectorbench` 내부 import 대상이 모두 존재함.
- 하위 프로세스의 리터럴 `-m` 대상 19개가 모두 실제 새 모듈 또는 유지한 `scripts.*`에 연결됨.
- `PYTHONPATH=src /opt/homebrew/bin/python3 -m unittest discover -s tests -q`: 104개 중 99개 통과, 실제 스모크 증거를 요구하는 5개 건너뜀.
- 양쪽 Python에서 대표 CLI 12개의 `--help` 경로 확인. `pilot.report`는 `--help`를 받지 않아 import 경로로 확인했다.
- 임시 디렉터리의 source snapshot 113개 키와 원본·사본 SHA-256 일치.
- 공개 벡터 소스 캠페인 CLI를 임시 probe fixture로 호출해 source snapshot 111개 키와 원본·사본 SHA-256 일치 확인. 실제 데이터 전송은 실행하지 않음.
- 임시 manifest로 `dagster definitions validate -f src/vectorbench/experiments/synthetic_insertion/definitions.py` 성공.
- 임시 Dagster fixture에서 정상 소스를 native 성공으로 처리하고, 변조한 소스를 worker 등록·materialization 전에 거부함. DB는 실행하지 않음.
- `.venv`의 editable 설치와 패키지 import 성공. 일반 격리 빌드는 PyPI DNS 실패로 완료되지 않았고, 설치 검증에는 로컬에 있던 setuptools를 사용했다. 의존성 버전은 변경하지 않았다.

실제 DB 스모크, 전체 연구 재실행, 이전 캠페인 재개는 이 구조 변경에서 수행하지 않았다.
