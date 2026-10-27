# 공개 벡터 클러스터별 삽입 결과

48이력·120fresh·480검색 조건·211,200평가 질의의 raw 반환 ID와 oracle를 대조했다.
이 파일은 native 측정·분석 산출물이며 최종 정리·제출 G5는 별도 receipt로 확인한다.

## 고정 운영 설정

| 데이터 | DB | M | efConstruction | ef |
|---|---|---:|---:|---:|
| msmarco | postgres | 16 | 100 | 1000 |
| msmarco | qdrant | 48 | 300 | 256 |
| wikipedia | postgres | 16 | 100 | 512 |
| wikipedia | qdrant | 48 | 300 | 256 |

## 최종 순서 효과와 상호작용

| 데이터 | DB | seed | 대비 | 차이 (%p) | 95% 질의 구간 |
|---|---|---:|---|---:|---|
| msmarco | postgres | 11 | C5-M5 | +0.008 | [-0.030, +0.047] |
| msmarco | postgres | 11 | interaction | -0.010 | [-0.020, +0.000] |
| msmarco | postgres | 29 | C5-M5 | +0.032 | [-0.007, +0.070] |
| msmarco | postgres | 29 | interaction | +0.000 | [-0.015, +0.017] |
| msmarco | postgres | 47 | C5-M5 | +0.018 | [-0.015, +0.052] |
| msmarco | postgres | 47 | interaction | +0.000 | [-0.013, +0.013] |
| msmarco | qdrant | 11 | C5-M5 | -0.048 | [-0.127, +0.030] |
| msmarco | qdrant | 11 | interaction | -0.065 | [-0.173, +0.033] |
| msmarco | qdrant | 29 | C5-M5 | -0.028 | [-0.105, +0.045] |
| msmarco | qdrant | 29 | interaction | -0.003 | [-0.092, +0.080] |
| msmarco | qdrant | 47 | C5-M5 | -0.053 | [-0.132, +0.022] |
| msmarco | qdrant | 47 | interaction | -0.098 | [-0.187, -0.008] |
| wikipedia | postgres | 11 | C5-M5 | +0.010 | [-0.027, +0.048] |
| wikipedia | postgres | 11 | interaction | +0.000 | [-0.010, +0.010] |
| wikipedia | postgres | 29 | C5-M5 | +0.030 | [-0.017, +0.078] |
| wikipedia | postgres | 29 | interaction | +0.003 | [-0.010, +0.018] |
| wikipedia | postgres | 47 | C5-M5 | +0.013 | [-0.023, +0.052] |
| wikipedia | postgres | 47 | interaction | +0.000 | [-0.015, +0.017] |
| wikipedia | qdrant | 11 | C5-M5 | +0.007 | [-0.043, +0.055] |
| wikipedia | qdrant | 11 | interaction | -0.007 | [-0.067, +0.052] |
| wikipedia | qdrant | 29 | C5-M5 | -0.032 | [-0.080, +0.012] |
| wikipedia | qdrant | 29 | interaction | -0.052 | [-0.110, +0.007] |
| wikipedia | qdrant | 47 | C5-M5 | +0.017 | [-0.025, +0.060] |
| wikipedia | qdrant | 47 | interaction | -0.010 | [-0.058, +0.040] |

## 클러스터별 5회 삽입과 fresh

| 데이터 | DB | seed | C5 R@100 (%) | fresh (%) | 차이 (%p) |
|---|---|---:|---:|---:|---:|
| msmarco | postgres | 11 | 99.700 | 99.706 | -0.006 |
| msmarco | postgres | 29 | 99.797 | 99.735 | +0.062 |
| msmarco | postgres | 47 | 99.788 | 99.746 | +0.043 |
| msmarco | qdrant | 11 | 98.915 | 98.997 | -0.082 |
| msmarco | qdrant | 29 | 98.975 | 98.954 | +0.021 |
| msmarco | qdrant | 47 | 99.000 | 98.941 | +0.059 |
| wikipedia | postgres | 11 | 99.732 | 99.743 | -0.012 |
| wikipedia | postgres | 29 | 99.702 | 99.717 | -0.015 |
| wikipedia | postgres | 47 | 99.725 | 99.709 | +0.016 |
| wikipedia | qdrant | 11 | 99.700 | 99.703 | -0.003 |
| wikipedia | qdrant | 29 | 99.715 | 99.695 | +0.020 |
| wikipedia | qdrant | 47 | 99.742 | 99.696 | +0.046 |

## 근거와 해석 범위

- [자원 계측](resource-summary.json), [구축·쓰기 비용](operation-costs.json), [사전 조사 신호](investigation-signals.json).
- RSS는 worker OS 최대값과 DB 프로세스별 RSS 합의 주기 표본을 구분한다. DB 합은 공유 페이지를 중복 계산할 수 있다.
- [전체 endpoint](endpoints.json), [대응 대비와 24개 family 보정 구간](contrasts.json), [ef 곡선](ef-curves.json).
- [현재 데이터 시점별 비교](trajectories.csv), [집단별 표본 수와 차이](cohorts.csv), [검색 조건 전수](conditions.csv).
- 질의 bootstrap은 고정 입력과 구축에 조건부다. 세 재표본 seed는 같은 공개 pool을 공유한다.
- 단순 시간 변화와 같은 상태 fresh 차이를 분리했다. 중간 fresh는 1회이므로 구축 변동의 확정 판정이 아니다.
- 클러스터는 정규화한 복사본에서 만들었고 DB는 원본 IP로 검색했다. 실제 HNSW edge를 관측한 결과가 아니다.
- 한 번/5회 차이에는 commit·중간 확인·optimizer 대기가 포함된다. 효과가 0 근처라고 동등성을 증명하지 않는다.
- 지연은 공유 로컬 환경의 참고치다. 제품 보편적 순위·클라우드 대규모 성능으로 일반화하지 않는다.
