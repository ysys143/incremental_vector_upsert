# 제품 수준 선행 벤치마크와 이번 측정의 관계

열람일: 2026-10-25. 아래는 원 저자·운영자가 공개한 측정 설명을 확인한 것이다.
벤더/작성자 보고의 독립 재현이나 같은 환경에서의 제품 순위를 의미하지 않는다.

## VDBBench 1.0: 지속 적재와 최적화 전후

Tian Min의 [2025-07-04 Milvus 공식 발표](https://milvus.io/blog/vdbbench-1-0-benchmarking-with-your-real-world-production-workloads.md)는
Cohere 10M에서 초당 500행을 적재하고, 적재 진행률 10%마다 검색을 측정하는
Pinecone·Elasticsearch 사례를 제시한다. 최적화 전후 결과와 실제 경과 시간도 구분한다.

이 자료는 실제 제품의 지속 적재 중 품질·지연·처리량 변화 근거다. 같은 최종 데이터의
한 번/분할 삽입 경로와 매 시점 fresh를 대응 비교한 결과로 전용하지 않는다.
그래프 판독 근사값을 정밀한 실측 표처럼 재인용하지 않았다. 현재 코드의 정답·보정식은
해당 revision을 별도로 확인해야 하며, 이번 실험은 각 D_t의 exact top-k를 직접 사용한다.

## Microsoft: Garnet/DiskANN과 다른 제품

[Microsoft DiskANN 공식 performance wiki](https://github.com/microsoft/DiskANN/wiki/Perf%3A-Garnet-Providers-vs-other-Vector-DBs-%28Zilliz%2C-Pinecone%2C-etc.%29)는
Wikipedia-10M·Cohere 768차원 float32, 초당 1,000벡터 추가, Azure D32v6 환경의
Garnet/DiskANN 측정을 설명한다. 다른 DB 수치는 benchmark website에서 가져왔다고 명시한다.

따라서 같은 호스트에서 모든 제품을 독립 재측정한 비교로 표현하지 않는다.
일부 그래프 수치를 읽어 이번 로컬 실험과 직접 배율 비교하지 않는다.
해당 client의 [공식 README](https://github.com/microsoft/DiskANN/blob/main/vectorset/README.md)는
별도 ingestion/query 명령과 미리 계산된 ground truth 사용을 설명한다.
그 protocol의 지표 정의와 현재 데이터 집합의 정답을 구분해야 한다.

## AIMultiple: 작은 규모의 삭제·재삽입과 필터

[작성자 원문](https://aimultiple.com/open-source-vector-databases)은 7개 오픈소스 엔진에
대해 무작위 20% 삭제·동량 신규 문서 재삽입 이후 Recall@10 0.97~1.00과 삭제 ID
재등장 없음으로 보고한다. 업데이트 구간은 대부분 초기 30,000건, Weaviate는
3,000건이었다고 공개한다. 이 결과는 그 규모의 해당 교체 조건에 한정한다.

같은 글은 작은 선택도 조건의 Recall 1.0 일부가 HNSW가 아닌 exact full-scan 경로라고
명시한다. LanceDB의 recall 파라미터를 잘못 선택했던 결과를 철회·수정한 과정도
공개한다. 그러므로 제품별 올바른 탐색 knob와 실제 실행 모드를 함께 확인해야 한다.
이번 실험은 PostgreSQL 계획·Qdrant 카운터로 ANN 경로를 검증하며, 필터의 exact
fallback 값이나 외부 글의 처리량을 이번 점수와 합치지 않는다.

## 현재 연구가 추가하는 비교

- 같은 초기 인덱스를 복원해 경로를 분기하고 신규 집합·순서를 공유한다.
- 매 고유 시점의 현재 데이터 정답 및 fresh 대조군을 둔다.
- 신규/기존 비율, 기하 조건, 순서·분할·중간 대기 정책을 구분한다.
- 기존·신규·경계 질의를 따로 평가하고 물리 ANN 경로를 확인한다.
- 로컬 합성 데이터 결과의 범위를 명시한다. 대규모 cloud·동시 부하·실제 임베딩
  결과로 확대하려면 보존한 전체 연구의 별도 실행이 필요하다.
