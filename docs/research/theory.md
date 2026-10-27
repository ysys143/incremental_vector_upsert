# 증분 업데이트 이론 검토와 파일럿 판정

## 가설

1. 동일한 최종 생존 데이터라도 업데이트 이력이 인덱스 구조와 재현율을 바꿀 수 있다.
   증분 경로와 최종 재구축을 같은 설정으로 비교한다. 구축 순서·병렬 난수에 따른 차이도
   있으므로 한 번의 차이를 인과 증명으로 취급하지 않고 seed별 결과를 보존한다.
2. 삭제 마커·그래프 복구·segment 정리는 자원과 탐색에 영향을 준다.
   유지보수는 DB 고유 동작을 허용하고 checkpoint의 정리 상태를 기록한다.
3. 쓰기 확인과 검색 가시성은 다를 수 있다. 전체 ID·벡터 readback 및 생존 집합 검증이
   끝나야 recall을 산출한다. 가시성 실패를 ANN 오차로 분류하지 않는다.
4. 작은 데이터에서 전수 검색을 ANN으로 오인하기 쉽다. PostgreSQL 실행 계획과
   Qdrant 인덱싱 카운터·검색 설정을 확인한다. Qdrant 카운터는 근삿값이므로 전체 구조의
   증명이 아니며 미인덱싱 tail과 내부 optimizer 전환의 한계를 보고한다.
5. 작은 파일럿에서 열화가 없다는 것은 대규모·장기 업데이트의 동등성 증거가 아니다.
   합성 군집이 쉬울 수 있고 단기 churn이 복구 한계에 도달하지 않을 수 있다.

## 근거 및 제품별 확인 사항

- [FreshDiskANN](https://arxiv.org/abs/2105.09613): 스트리밍 ANN의 삽입·삭제와 품질 유지.
- [IP-DiskANN](https://arxiv.org/abs/2502.13826): 삭제 후 일괄 consolidation과 in-place 복구의 구분.
- [HNSW 업데이트 연구](https://arxiv.org/abs/2407.07871): 반복 변경과 unreachable points 가설.
- [pgvector 공식 문서](https://github.com/pgvector/pgvector): HNSW, ef_search, VACUUM,
  COPY, EXPLAIN, exact search. 이 파일럿의 강제 ANN과 planner-auto를 구분한다.
- [PostgreSQL 대량 적재](https://www.postgresql.org/docs/current/populate.html):
  COPY 후 인덱스 구축·ANALYZE는 초기 적재에 적용한다. 온라인 변경에서 인덱스를 제거하지 않는다.
- [Qdrant 인덱싱](https://qdrant.tech/documentation/manage-data/indexing/): 작은 segment의
  전수 검색과 full_scan_threshold. 동일 ef 값으로 제품을 공정하게 맞출 수 없다.
- [Qdrant optimizer](https://qdrant.tech/documentation/operations/optimizer/):
  vacuum/merge/indexing과 읽기·쓰기의 공존. 정리 전후와 쓰기 확인의 의미를 구분한다.

## 해석 경계

exact Recall은 임베딩 거리의 근사 오차를 측정하며 의미 관련성이나 RAG 응답 품질을
증명하지 않는다. 측정 중 부하가 없는 안정 checkpoint로 제한하므로 실시간 경쟁도
증명하지 않는다. 효과 크기와 seed별 차이를 보고하며 세 seed의 작은 표본으로 동등성을
주장하지 않는다. 성능 수치는 공유 Mac/VM·서로 다른 클라이언트 전송을 포함한다.
