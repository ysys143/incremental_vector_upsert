# 공개 벡터 진입 스모크 결과

## 판정

실제 MS MARCO·Wikipedia 벡터를 사용한 PostgreSQL·Qdrant 진입 스모크가 통과했다.
71/71 native 노드와 필수 검사가 성공했으며, 28개 갱신 경로의 84개 checkpoint와 최종 fresh 28개, 총 112개 품질 측정이다.
전체 5회·50회 연구·기하 조건·필터 실험의 완료를 의미하지 않는다.

## 입력과 조건

- 원본 768차원 float32, 최대 내적(IP). 벡터 정규화·새 임베딩 생성 없음.
- 데이터셋별 원본 표본 100,000개에서 초기 1,000개를 선정했다. 100개씩 2회 갱신했다.
- 튜닝 질의 200개와 평가 질의 60개를 분리했다. 평가 seed 11, calibration seed 7.
- 문서·질의의 동일 바이트 중복을 제외했다. 튜닝 문서와 평가에서 사용한 모든 문서도 분리했다.
- 삽입 최종 1,200개, 삭제 최종 800개, 나머지 경로는 1,000개다.
- DB마다 2 CPU·4GiB, DB 슬롯 1개. 같은 DB의 모든 경로는 동일한 초기 물리 snapshot에서 분기했다.
- Wikipedia 질의는 문서에서 유래한 제공 벡터이며 실제 사용자 검색 질의라고 부르지 않는다. 메타데이터는 실험용 라벨이다.

## 실제 선택한 설정

측정한 탐색 범위 안에서 calibration Recall@10/100 0.99를 만족한 후보 중 p95가 가장 낮은 설정이다. 평가 질의와 갱신 후에도 항상 0.99가 유지된다는 뜻은 아니다.

| 데이터셋·DB | m | ef_construction | ef_search | 초기 적재 batch |
|---|---:|---:|---:|---:|
| msmarco-postgres | 32 | 200 | 512 | 1000 |
| msmarco-qdrant | 32 | 200 | 256 | 100 |
| wikipedia-postgres | 32 | 200 | 512 | 1000 |
| wikipedia-qdrant | 16 | 100 | 100 | 1000 |

## 최종 Recall@100과 fresh 차이

최종 Recall@100의 범위는 98.583%~100.000%, fresh 대비 차이는 −0.900~+0.067%p였다.
각 fresh는 해당 경로와 동일한 최종 ID·벡터·버전·질의에서 한 번 구축했다.

| 경로 | 이력 Recall@100 | fresh | 차이(%p) |
|---|---:|---:|---:|
| msmarco-postgres-churn | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-delete | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-insert | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-metadata | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-reinsert | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-same_vector | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-upsert | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-churn | 99.983% | 100.000% | -0.017 |
| msmarco-qdrant-delete | 99.983% | 100.000% | -0.017 |
| msmarco-qdrant-insert | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-metadata | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-reinsert | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-same_vector | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-upsert | 99.983% | 100.000% | -0.017 |
| wikipedia-postgres-churn | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-delete | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-insert | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-metadata | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-reinsert | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-same_vector | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-upsert | 100.000% | 100.000% | +0.000 |
| wikipedia-qdrant-churn | 99.150% | 99.217% | -0.067 |
| wikipedia-qdrant-delete | 98.583% | 99.483% | -0.900 |
| wikipedia-qdrant-insert | 98.950% | 98.950% | +0.000 |
| wikipedia-qdrant-metadata | 99.017% | 99.100% | -0.083 |
| wikipedia-qdrant-reinsert | 99.067% | 99.000% | +0.067 |
| wikipedia-qdrant-same_vector | 98.800% | 99.017% | -0.217 |
| wikipedia-qdrant-upsert | 99.000% | 99.017% | -0.017 |

## 벡터 값이 바뀌지 않는 제어군

다음 경로에서는 최초와 최종의 벡터·질의 해시가 같은 것을 확인했다. 메타데이터 변경의 버전 원장은 별도 상태로 검증했다.

| 경로 | 변경 전 Recall@100 | 변경 후 | fresh | 동일 이력 내 차이(%p) |
|---|---:|---:|---:|---:|
| msmarco-postgres-metadata | 100.000% | 100.000% | 100.000% | +0.000 |
| msmarco-postgres-same_vector | 100.000% | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-metadata | 100.000% | 100.000% | 100.000% | +0.000 |
| msmarco-qdrant-same_vector | 100.000% | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-metadata | 100.000% | 100.000% | 100.000% | +0.000 |
| wikipedia-postgres-same_vector | 100.000% | 100.000% | 100.000% | +0.000 |
| wikipedia-qdrant-metadata | 99.033% | 99.017% | 99.100% | -0.017 |
| wikipedia-qdrant-same_vector | 99.033% | 98.800% | 99.017% | -0.233 |

평가 질의 60개이므로 Recall@100에서 이웃 하나의 차이는 약 0.0167%p다.
동일 벡터 쓰기나 메타데이터 변경에서 작은 차이가 관찰됐다는 사실만으로 그래프 손상을 단정하지 않는다.
본실험의 정적 기준선에는 쓰기 없는 반복 검색을 포함해 검색 자체의 변동을 구분하고, 최종 fresh 3회로 구축 변동도 측정해야 한다.

## 경로 검증과 한계

- PostgreSQL은 COPY와 staging COPY upsert를 실제 사용했다. enable_seqscan=off 및 실제 지정 HNSW 실행 계획을 검증했다.
- Qdrant는 Dot·exact=false로 검색하고 HNSW 카운터 증가 및 plain/exact fallback 부재를 확인했다.
- 모든 checkpoint의 실제 ID·벡터·메타데이터·버전을 현재 event 원장과 대조했다.
- 짧은 경로·평가 seed 1개·질의 60개·fresh 1회 결과다. 장기 열화·동등성·제품 순위를 확정할 수 없다.
- 공유 호스트와 서로 다른 클라이언트 경로이므로 지연을 독립 클라우드 성능 비교로 취급하지 않는다.
- 이 진입 스모크는 기존 keep-alive 쓰기 클라이언트로 성공했다. 이후 대용량 스모크에서 확인된 유휴 연결 문제는 별도로 수정·재검증하며 서로 다른 전송 버전의 시간을 합치지 않는다.

## 원시 증거

- `real-results/smoke-20261026-01/selected-scope-receipt.json` 및 `guardian-receipt.json`
- 같은 디렉터리의 `cells/*/*/receipt.json`, 실제 SQL 계획·telemetry·raw.npz·쓰기 intent/ACK
- `real-results/readiness-20261025/real-entry-smoke-result.json`
- `real-results/readiness-20261025/entry-control-quality.json`
- `real-results/source-20261026-02`의 공식 원본·ETag·구간 해시·native 증거
