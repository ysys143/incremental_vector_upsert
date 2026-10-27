# 검증 명령과 증거

## 독립 방법론·증거 경계 검사

```sh
PILOT_SMOKE_ROOT=results/20261024T185358Z-smoke-7376a5 python3 -m unittest discover -s tests -v
```

이번 세션의 실제 최종 출력:

```text
Ran 13 tests in 0.324s

OK
```

검사 대상은 독립 exact oracle·동점 순서, 누적 이벤트, 삭제/중복 ID,
변경 누락/벡터 변조, 오래된 oracle, k 미만/빈 응답 저장,
실제 스모크 증거의 수용·checkpoint 누락·허위 점수·원시 파일 변조·인덱스 교체다.
결함 주입은 원시 증거의 임시 사본에만 적용했다.
처음 증거 경계 검사의 OID 변조 코드가 문자열에 정수를 더해 TypeError를 냈고,
문자열 OID를 명시적으로 변환한 뒤 위 명령을 다시 실행해 13개 모두 통과했다.

## 실제 DB 스모크

```sh
python3 -u -m pilot.run --stage smoke
```

현재 파일럿 선행 증거:
`results/20261024T185358Z-smoke-7376a5/manifest.json`.

```text
RESULT_ROOT=results/20261024T185358Z-smoke-7376a5 STATUS=passed
```

이력 6개, 증분 checkpoint 18개, 재구축 대조군 6개.
PostgreSQL·Qdrant 모두 재시작 후 데이터 보존을 검사했다.
이전 실패 및 다른 소스 버전의 스모크는 `docs/pilot/execution.md`에 구분했다.

## 실제 로컬 파일럿

```sh
python3 -u -m pilot.run --stage pilot
python3 -m pilot.report results/20261024T185508Z-pilot-676b93
```

실행/검증 상태와 실제 개수는 해당 디렉터리의 manifest 및 REPORT를 따른다.
이 문서의 명령 나열만으로 파일럿 완료를 주장하지 않는다.
manifest의 passed는 증거 경계 검증 완료이며 모든 평가 질의에서 품질 목표를
유지했거나 대규모 제품 성능이 검증되었다는 뜻이 아니다.

## 명시적 한계

- WDL 프로젝트 전용 검사: 사용자 선택 `without-setup`에 따라 NOT RUN.
- 공유 ChatGPT 대화: ego-browser 앱 시작 실패로 UNVERIFIED.
- 클라우드·분산 실행, 실제 임베딩, 필터·카디널리티, 온라인 동시 부하: 이번 파일럿 범위 밖.
- PostgreSQL checkpoint는 commit 가시성 및 스냅샷을 확인하며 VACUUM 완료를 기다렸다는
  주장은 하지 않는다. dead tuple 관찰을 포함해 기본 유지보수 상태의 결과로 해석한다.
- Qdrant는 optimizer 상태·인덱싱 카운터와 HNSW 설정을 확인했다. 개별 질의의 내부
  traversal trace나 모든 segment의 exact fallback 부재를 증명하지 않는다.
- 파일럿 시간/디스크 한도는 단계·질의 배치 경계에서 검사한다. 강제 종료·시간 초과에
  대한 별도의 실제 장애 주입은 이번 세션에 실행하지 않았다.

## 완료 및 정리 확인

실제 종료 출력:

```text
RESULT_ROOT=results/20261024T185508Z-pilot-676b93 STATUS=passed
```

실측 시간 1115.6초, 이력 18/18·checkpoint 108/108·대조군 18/18이다.
manifest의 cleanup_errors는 빈 배열이며, inventory 전후의 기존 컨테이너 목록과
볼륨 목록이 각각 동일함을 확인했다. 종료 후 실행 중인 컨테이너는 없었다.
새 두 이미지는 캐시로 보존했다. 현재 호스트 여유는 약 40GiB다.

실행 당시 `pilot/*.py`를 결과 디렉터리의 `source/pilot/`에 복사하고 manifest의
소스 해시와 모두 일치함을 확인했다(`source-snapshot.json`). 이후 보고서의
"정리 안정화"라는 모호한 열 이름만 "대기 조건 충족"으로 바로잡고 DB별 의미를
명시했다. 실험 실행 코드·설정·원시 측정값은 변경하지 않았다.
현재 보고서 생성기의 표시 수정은 실행 당시 소스 스냅샷과 구분한다.

## 순차 스캔 금지 검증 강화

사용자의 후속 지시에 따라 PostgreSQL ANN 검색의 GUC·계획 검증을 강화했다.
매 검색 전 동일 SQL의 계획을 검사해 off 설정, 지정된 ordered HNSW만 허용한다.
순차 스캔·다른 인덱스·sort 등 fallback은 검색 SQL을 실행하기 전에 실패시킨다.
원장 조회와 exact oracle의 전체 데이터 읽기는 검색 측정과 구분한다.

```sh
PILOT_SMOKE_ROOT=results/20261024T185358Z-smoke-7376a5 python3 -m unittest discover -s tests -v
python3 -u -m scripts.verify_ann_guard
```

실제 출력:

```text
Ran 19 tests in 0.522s
OK
{"root": "results/20261024T192803Z-ann-guard", "status": "passed", "probes": 4}
```

실제 PostgreSQL에서 정상 HNSW 검색, GUC를 on으로 바꾼 요청의 실행 전 차단,
off 상태에서 HNSW 인덱스를 제거한 요청의 실행 전 차단, 재시작 후 off 재적용을 확인했다.
`receipt.json`·`sql-trace.json`에 실제 SQL과 응답을 보존했다. 전용 컨테이너·볼륨을
정리했고 기존 자원 목록이 유지됨을 확인했다.

기존 파일럿의 PostgreSQL checkpoint 54개와 재구축 9개, 총 63개의 저장된
EXPLAIN ANALYZE를 새 규칙으로 재검사해 모두 통과했다. 그 표본에는 off 설정과
실제 HNSW 실행이 기록되어 있다. 이 사실을 과거 모든 개별 검색에 새 사전 검사가
실행되었다는 의미로 소급하지 않는다. 전체 파일럿의 재실행은 하지 않았다.
원시 결과·실행 당시 소스 스냅샷은 보존했다. 이후 호출 지연에는 새 계획 검사 비용이
포함되므로 기존 지연과 같은 측정 경로로 취급하지 않는다.
