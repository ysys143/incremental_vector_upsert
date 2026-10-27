# 후속 실험의 이론·출처 확인

참조 대화 본문은 사용자가 직접 전달했다. 그 본문에 적힌 숫자와 날짜를 모두 독립
검증한 것으로 취급하지 않는다. 이번 실험은 아래 확인 가능한 방법론에 근거하며,
사용자 본문에 있던 효과 크기를 정답이나 예상 결과로 넣지 않는다.

| 쟁점 | 확인한 1차 자료 | 실험에 반영한 범위 |
|---|---|---|
| 삽입 순서·내재 차원 | [Elliott·Clark, 2024](https://arxiv.org/abs/2405.17813) | 동일 최종 데이터·삽입 순서의 대응 비교와 집단별 난도 진단. 논문의 특정 %p 수치는 재인용하지 않음 |
| 동적 그래프 유지보수 | [FreshDiskANN](https://arxiv.org/abs/2105.09613), [IP-DiskANN](https://arxiv.org/abs/2502.13826) | 데이터 변화와 연결 복구·consolidation의 구분. 알고리즘 결과를 DB 제품 순위로 전용하지 않음 |
| 동적 ANN 측정 | [Intel SVS 공식 동적 벤치마크](https://intel.github.io/ScalableVectorSearch/benchs/dynamic.html) | 지속 삽입·삭제와 품질·갱신 비용의 구분. SVS 라이브러리 결과를 상용 DB 결과로 표현하지 않음 |
| 제품 벤치마크 | [VectorDBBench 공식 저장소](https://github.com/zilliztech/VectorDBBench) | 제품 구현·부하·지표 정의를 검토할 출발점. 스트리밍 그래프 판독값을 이번 baseline으로 사용하지 않음 |
| PostgreSQL 검색 경로 | [공식 planner 설정](https://www.postgresql.org/docs/17/runtime-config-query.html), [pgvector](https://github.com/pgvector/pgvector) | seqscan off는 절대 금지가 아니므로 매 검색의 HNSW 계획과 실제 checkpoint 실행을 검증 |
| 초기 상태 복제 | [PostgreSQL 파일 시스템 백업](https://www.postgresql.org/docs/17/backup-file.html) | 정상 종료한 전체 클러스터를 복사·복원하고 시작 전 파일 해시로 동일 초기 인덱스를 증명 |
| Qdrant 검색 모드 | [1.17 API](https://api.qdrant.tech/v-1-17-x/api-reference/service/telemetry?explorer=true), [1.17.1 telemetry 구조](https://github.com/qdrant/qdrant/blob/v1.17.1/lib/segment/src/telemetry.rs), [상세도 정의](https://github.com/qdrant/qdrant/blob/v1.17.1/lib/common/common/src/types.rs) | details_level=4, segment UUID와 dense index 구조, HNSW/plain/exact 카운터 변화 검증 |

## 원인 해석의 경계

- 현재 D_t에서 exact top-k를 다시 계산한다. 최종 전체 데이터의 정답을 적재율로
  나눈 값을 Recall@k로 사용하지 않는다. 외부 VDBBench의 현재 코드가 항상 어떤
  보정식을 사용한다는 별도 주장은 이번 조사에서 확정하지 않았다.
- exact kNN 그래프는 데이터의 기하학적 구조다. 이를 제품 내부 HNSW의 실제
  bridge edge나 conductance로 표현하지 않는다.
- 같은 초기 인덱스·같은 신규 집합·같은 순서를 고정한 대응 경로와, 각 시점 fresh를
  함께 사용한다. fresh 반복에서도 구축 난수·병렬 실행·삽입 순서의 효과를 구분한다.
- 지속 삽입 중 지연·가시성과 안정화 후 ANN recall은 다른 지표다. 전수 검색이
  fallback으로 섞이면 ANN-only 결과로 인정하지 않는다.
- 사용자 본문의 MERIT 수치, FreshDiskANN 기반 100K+60K batch 크기별 수치,
  Lucene 이슈 날짜·수치, 제품별 그래프 판독값은 해당 원문을 특정해 검증하기 전까지
  UNVERIFIED다. 이번 실험의 성공 기준이나 결론에 사용하지 않는다.

## 실제 API 확인

`results/20261024T201142Z-telemetry-probe`는 상세도 3에서 segment 카운터가 없었다.
`results/20261024T201222Z-telemetry-level4`는 상세도 4에서 segment UUID와 HNSW
2회 증가, exact 양성 대조군의 별도 증가를 보존한다. unused counter의 생략은
고정 버전 Rust 직렬화 정의와 일치한다. 전체 telemetry 구조가 없는 응답을 이 규칙으로
성공 처리하지 않는다. 실제 snapshot·plain 양성 대조군의 최종 검증은 별도 IO 검사
receipt를 따른다.
