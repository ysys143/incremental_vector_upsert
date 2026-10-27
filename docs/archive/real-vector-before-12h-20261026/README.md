# 벡터 DB 증분 업데이트 파일럿

기존 인덱스를 유지하며 삽입·삭제를 누적하고 exact oracle 및 최종 재구축 대조군과 비교한다.
기존 합성 실험과 공개 임베딩의 실제 DB 스모크 결과를 구분한다. 현재 로컬 스모크는
측정·갱신 경로 검증이며 제품 성능 순위나 장기 열화의 결론이 아니다.

- [로컬 파일럿 계획](docs/pilot-plan.md)
- [삽입 품질 변화 후속 실험 계획](docs/insertion-study-plan.md)
- [후속 실험 실행·검증 기록](docs/insertion-study-execution.md)
- [공개 벡터 재실험·삭제·upsert·DAG 개정 계획](docs/real-vector-update-study-plan.md)
- [공개 벡터 실험 실행 상태와 검증 한계](docs/real-vector-update-execution.md)
- [공개 벡터 진입 스모크 결과](docs/real-vector-entry-smoke-results.md)
- [공개 벡터 기본 실험 실행표](docs/real-vector-baseline-phase-plan.md)
- [진행 상태 확인·자동 후처리](docs/insertion-study-status.md)
- [선행연구 수치 원문 확인](docs/insertion-study-literature-audit.md)
- [제품 수준 선행 벤치마크](docs/insertion-study-product-benchmarks.md)
- [기존 전체 계획 아카이브](docs/archive/full-plan-2026-10-25.md)
- [이론과 판정 기준](docs/theory.md)
- [실행 결과](docs/pilot-results.md)
- [검증 명령과 한계](docs/verification.md)

```sh
docker pull --platform linux/arm64 pgvector/pgvector@sha256:7a7e9f22015b67edb4bef5c59daeebcd7e74bfa570df6ce60ae01237c8648a84
docker pull --platform linux/arm64 qdrant/qdrant@sha256:94728574965d17c6485dd361aa3c0818b325b9016dac5ea6afec7b4b2700865f
python3 -m unittest discover -s tests -v
python3 -m pilot.run --stage smoke
PILOT_SMOKE_ROOT=results/<스모크ID> python3 -m unittest discover -s tests -v
python3 -m pilot.run --stage pilot
python3 -m pilot.report results/<실행ID>
```

Python 3.11 이상, NumPy, Docker가 필요하다. DB는 ARM64 이미지를 digest로 고정하고
순차 실행한다. PostgreSQL 클라이언트는 컨테이너의 지속 psql 세션, Qdrant는 표준
라이브러리 HTTP 연결을 사용한다. 서로 다른 전송 경로의 지연을 제품 우열로 해석하지 않는다.
새 실행은 새 디렉터리를 만들며 실패한 변경을 자동 재전송하지 않는다.
결과·로그는 `results/` 아래 보존한다. Docker 전체 prune은 사용하지 않는다.
기본 단위 검사만 실행하면 실제 스모크 증거가 필요한 5개 검사는 명시적으로 skip한다.
`PILOT_SMOKE_ROOT`를 지정한 검사는 실제 증거 사본을 변조해 집계 경계가 실패하는지 확인한다.
자동 프로젝트 검사 설정은 사용자의 명시적 선택에 따라 사용하지 않는다.

## 후속 삽입 연구: just-dag-it / Dagster

`study`는 같은 초기 인덱스에서 경로를 분기하고 시점별 fresh·질의 집단·기하 조건을
비교한다. Dagster의 partition mapping으로 입력 검증 → 튜닝 → 초기 snapshot →
증분/fresh → 검증 보고서를 연결한다. 상시 daemon 없이 순차 materialize하며,
별도 프로세스가 시간 초과·controller 사망 뒤 owned Docker 자원을 정리한다.

```sh
uv venv --python 3.13 .venv
uv pip install -r requirements-orchestration.txt
python3 -m scripts.verify_study_engine
python3 -m scripts.verify_study_parent
python3 -m scripts.verify_study_io
python3 -m study.prepare --smoke --out study-results/entry-01
STUDY_ROOT="$PWD/study-results/entry-01" .venv/bin/dagster definitions validate -f study/definitions.py
.venv/bin/python -m study.launch study-results/entry-01
python3 -m study.prepare --out study-results/campaign-01 --smoke-root study-results/entry-01
.venv/bin/python -m study.launch study-results/campaign-01
```

기존 출력 디렉터리는 덮어쓰지 않고 새 이름을 사용한다.
`--stages A`는 SETUP과 A만 선택하며, 재개할 때 검증된 PASS는 다시 실행하지 않는다.
실패 셀의 명시적 재개는 `--select kind:key --retry kind:key`로 지정한다.
원래 단계의 4시간 상한은 재개로 연장하지 않는다. 완료된 ack나 원시 파일이 바뀌면 실패한다.
현재 실행의 읽기 전용 상태는 다음 명령으로 확인한다. 결과 보고서는 모든 측정과 native
검사가 통과한 뒤 `docs/insertion-study-results.md`에 자동 생성된다.

```sh
python3 -m analysis.status study-results/campaign-20261025-01
```

실행 중 코드 수정이 필요하면 원래 manifest·결과를 덮어쓰지 않는다.
`scripts.amend_study_sources`는 정확한 변경 파일 허용 목록과 이유, 보존할 native run·ack·
receipt hash를 기록한다. 데이터·oracle·측정·설정 및 실험 함수가 달라지면 기존 측정을
재사용하지 못하도록 거부한다. 원래 실패 attempt와 시간 상한은 유지된다.
