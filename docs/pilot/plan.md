# 로컬 최소 파일럿

상태: 실행 결과는 각 `results/<run-id>/manifest.json`과 `REPORT.md`를 따른다.

## 목적과 범위

PostgreSQL+pgvector HNSW 및 Qdrant HNSW에서 기존 인덱스를 유지한 배치 증분
삽입·삭제의 품질 측정 경로와 필요한 시간을 검증한다. 전체 연구는 아카이브에 보존한다.
공유 ChatGPT 대화는 ego-browser 앱 시작 오류로 미확인이다. 요구사항 전체 검토 완료를
주장하지 않으며 이 파일럿은 대화에서 명시한 조건에 기반한다.

## 고정 설정

- 초기 10,000건, 128차원 float32, 군집 16개, squared L2 oracle. PostgreSQL의 L2와 순위는 동일하다.
- seed 11·29·47. 튜닝 질의 200개, 평가 질의 800개는 문서 데이터와 별도로 생성한다.
- 매 회차 초기 크기의 10% 변경, 5회 누적. 삽입만·삭제만·삭제+신규 ID 삽입을 독립 실행한다.
- 2 DB × 3 seed × 3 시나리오 = 이력 18개, checkpoint 108개, 최종 재구축 대조군 18개.
- 매 checkpoint에서 원장과 전체 DB ID·벡터를 대조하고 생존 데이터의 exact top-100을 새로 계산한다.
- 마지막 재구축은 동일 설정·동일 생존 데이터·ID 순서로 별도 측정한다. 회차 사이 DROP/REINDEX 금지.
- HNSW 후보는 (M=16, efConstruction=100), (M=32, efConstruction=200).
  검색 ef=32·64·128·256; ef가 k보다 작으면 실제 요청은 max(ef,k)이며 중복 후보는 제거한다.
  Recall@10·100 모두 0.95 이상인 후보 중 튜닝 질의의 중앙 지연 최소를 선택한다.
  평가용 seed의 튜닝 질의만 사용하며 평가 결과로 재튜닝하지 않는다.
- 배치 100·1000을 초기 및 인덱스 유지 증분 경로 각각 시험한다. PostgreSQL은 COPY,
  Qdrant는 batch upsert(wait=true). 삭제는 집합 기반 API/SQL. 쓰기 확인 비용 포함.
- 원본 float32 정밀도, 복제 1, DB 2 vCPU·4GiB, 한 번에 DB 하나만 실행.
- 초기 적재는 인덱스 후생성, 증분 삽입은 인덱스를 유지한다. 기본 유지보수를 허용하며
  checkpoint마다 최대 120초의 상태 안정화를 관찰한다. 미완료는 명시하고 품질과 함께 보고한다.

## 스모크와 정확성

1,000건·32차원·튜닝 20/평가 40질의·seed 11·변경 2회로 두 DB의 3개 시나리오를
실행한다. DB 재시작 후 데이터 생존을 확인하고, 단일 이력 안에서는 테이블/컬렉션
정체성을 기록한다. Qdrant는 작은 컬렉션의 실제 HNSW 생성을 위해 indexing_threshold=1KB,
default_segment_number=1, full_scan_threshold=10KiB(이 버전의 최소 허용값)을 설정한다. 미인덱싱 구간의 exact scan은
상태와 함께 보고하고 graph-only 결과로 오인하지 않는다.
PostgreSQL은 별도 EXPLAIN에서 HNSW Index Scan을 확인하며 측정 세션의 seqscan을 끄고
`hnsw.iterative_scan=strict_order`로 삭제된 후보를 건너뛴 뒤 충분한 결과를 탐색한다.
강제 ANN 경로 실험이며 planner 자동 선택 성능 실험과 구분한다.

추가 강화: PostgreSQL은 검색 세션에서 `enable_seqscan=off`·`enable_indexscan=on`을
설정하고, 재시작 후에도 다시 적용한다. 매 검색 전에 해당 검색 SQL 전체의
`SHOW enable_seqscan` 및 `EXPLAIN (FORMAT JSON)`을 확인한다. 설정이 off가 아니거나
계획에 지정된 HNSW 이외의 scan·sort 등이 있으면 검색 SQL 실행 전에 실패한다.
checkpoint의 EXPLAIN ANALYZE에서는 HNSW의 실제 실행도 확인한다.
PostgreSQL의 off는 절대 금지가 아니라 planner 억제이므로 설정만으로 통과시키지 않는다.
[공식 설명](https://www.postgresql.org/docs/17/runtime-config-query.html).
이 규칙은 측정 대상 ANN 검색에 적용하며 정답 계산·원장 검증의 전체 데이터 읽기는 별도다.
추가 계획 검사 비용은 이후 PostgreSQL 클라이언트 호출 지연에 포함되므로 과거 지연과
그대로 비교하지 않는다. 기존 원시 결과와 실행 당시 소스는 수정하지 않는다.

스모크 품질·원장·재시작·ANN 상태 검증을 모두 통과한 후 본 파일럿을 실행한다.
정답은 float64 직접 거리 계산을 기준으로 하고 별도 행렬식 구현·수작업 동점 사례와
교차 검증한다. ID·벡터 누락, 삭제 ID 반환, 오래된 정답 주입을 검출해야 한다.

## 운영 한도와 증거

추가 이미지·볼륨·파일 포함 8GiB 상한, 호스트 여유 25GiB 하한.
실행 전과 각 단계에서 검사하고 측정 사이에 Docker 상태·사용량을 기록한다.
프로젝트 전용 컨테이너·볼륨만 생성/정리하며 기존 자원은 보존한다.
설치 제외 4시간 상한. 실패는 원시 stderr·부분 결과와 함께 보존하고 변경 재시도 금지.
타임아웃은 실패 또는 정리 미완료로 기록한다. 미실행 셀은 성공으로 집계하지 않는다.
기본 정리 정책은 종료 시 프로젝트 컨테이너·재생성 가능한 볼륨 제거이며 원시 결과는 유지한다.

검색 전에 warmup 1회, 평가 질의는 단일 동시성으로 실행한다. p50/p95/p99는 클라이언트
호출 경로 지연이다. exact oracle·상태 검증·EXPLAIN은 검색 타이밍 밖에서 수행한다.
각 checkpoint는 alive hash·raw 검색 ID·latency·recall·쓰기 시간·대기 상태를 보존한다.
완료 기준은 개수뿐 아니라 원장·ANN·품질 목표·재시작 검증 통과다.
여기서 품질 목표 통과는 튜닝 후보 선택 시의 조건이다. 고정 설정으로 업데이트한 뒤
평가 질의의 재현율이 0.95 아래로 떨어지면 실험 결과로 기록하며 삭제·재튜닝하지 않는다.
manifest의 `passed`는 실행·증거 검증 완료이며 모든 checkpoint에서 품질이 유지되었다는 뜻이 아니다.
본 파일럿은 온라인 동시 부하, 관련성 라벨, 필터·분포 이동, 장기 열화 결론을 다루지 않는다.
이 항목과 Milvus·Weaviate·IVFFlat은 파일럿 보고서 검토 시 전체 계획에서 재개한다.
