# 참조 대화의 수치 원문 확인

열람일: 2026-10-25. 본 캠페인이 실행되는 동안 추가로 확인한 문헌 검증이다.
동결된 계획·초기 references 문서의 UNVERIFIED는 준비 당시 상태이며, 아래는 그 후의
확인 결과다. 실험 조건이나 성공 기준을 결과에 맞춰 변경하지 않았다.

## 1. 동일 최종 집합의 배치 크기 비교

원문은 Das·Devadkar·Godbole의 arXiv preprint
[Recall-Bounded Staleness for Filtered Multi-Tenant ANN Graph Indexes under Concurrent Updates](https://arxiv.org/html/2610.15130), 2026-10-10, V-B(c)다.

초기 100K에 같은 60K를 삽입해 모든 조건이 160K에 도달한다. base graph·seed·
cache-empty protocol을 공유하고 같은 ground truth로 평가한다. 저자 보고값은 다음과 같다.

| 병합 크기 | 병합 횟수 | 최종 Recall@10 (%) |
|---:|---:|---:|
| 1K | 60 | 99.436 |
| 2.5K | 24 | 99.343 |
| 5K | 12 | 99.243 |
| 10K | 6 | 99.168 |
| 20K | 3 | 99.013 |

양 끝 차이는 0.423%p다. 이 표는 직접 측정한 경로 차이이며 상용 DB 비교가 아니다.
같은 장의 일부 이론 적합·누적 손실 분석은 다른 가정과 추정 fresh 기준을 사용하므로
표의 경로 간 차이와 혼동하지 않는다. 작은 batch 일부는 논문의 이론 보정 범위 밖이며
저자도 이를 직접 관찰에만 사용한다. PostgreSQL·Qdrant가 같은 batch 내부 이웃 비가시성
메커니즘을 갖는다고 가정하지 않는다. [원문 V-B](https://arxiv.org/html/2610.15130)

이번 실험의 -0.5%p 경고는 후속 조사 후보의 기본 기준이다. 그 미만의 효과를 없다고
판정하는 기준이 아니며, 모든 효과 크기를 CSV에 보존한다.

## 2. Lucene의 삭제만 수행한 실험과 churn을 구분

[Apache Lucene #16552](https://github.com/apache/lucene/issues/16552)는
graph reuse를 켠 경로와 rebuild 경로를 같은 live set에서 비교한다. 같은 초기
인덱스를 공유하고 2%씩 50회 변경한다. 원문에는 서로 다른 두 실험이 있다.

- 삭제만: 초기 100K에서 live set이 줄어든다. 50회 후 Recall@100은 rebuild 0.930,
  reuse 0.860으로 상대 격차가 -7.0%p다.
- Churn: 삭제와 신규 삽입으로 크기를 유지한다. 50회 후 rebuild 0.937,
  reuse 0.910으로 상대 격차가 -2.7%p다.

참조 대화의 91.0%·93.7%는 churn 표와 일치한다. reuse의 절대 Recall은 0.847에서
0.910으로 올랐지만 rebuild와의 격차는 커졌다. 따라서 절대값만으로 index aging을
판정할 수 없다는 사례다. 특정 버전 구현의 버그 재현이며 모든 Lucene 기반 제품이나
다른 DB의 현재 동작으로 일반화하지 않는다. [원문 재현 조건·두 결과표](https://github.com/apache/lucene/issues/16552)

## 3. MERIT의 누적 삭제 결과

[MERIT 원문 Table 2](https://arxiv.org/html/2607.29173)에는 Sift1M에서
Wolverine Recall@10이 삭제 전 0.9962, 30% 삭제 후 0.9932, 50% 삭제 후 0.9883으로
보고돼 있다. 양 끝은 0.79%p 차이다. 순수 누적 삭제 실험으로, 신규 벡터 삽입이나
같은 ID의 벡터 교체 실험과 구분한다. 알고리즘/인덱스 연구 결과이며 특정 DB 제품의
upsert 안정성을 직접 보장하지 않는다.

## 이번 결과 해석에 적용할 구분

1. 삽입 전후 R_inc(t), 같은 D_t의 fresh, 같은 최종 데이터의 경로 차이를 별도 제시한다.
2. 기존·신규·경계 질의의 변화는 평균 점수에 숨기지 않는다.
3. API 요청 batch·트랜잭션과 실제 인덱스 편입·segment merge 단위를 구분한다.
4. 다른 구현의 큰 효과나 작은 효과를 재현해야 통과한다는 기준을 두지 않는다.
5. 위 수치는 저자/이슈 작성자의 보고를 원문에서 확인한 것이며 이 환경에서 재현한 값이 아니다.
