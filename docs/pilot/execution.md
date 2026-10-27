# 실험 실행 기록

## 2026-10-25 KST: 로컬 파일럿 준비

- 호스트: Apple M2, 8 CPU, 24GB. Docker/Colima: ARM64, 4 CPU, 8,307,175,424 bytes RAM.
- 최초 여유 공간: 약 42GiB. 기존 정지 컨테이너 16개와 기존 볼륨을 조사만 했다.
- NumPy 2.5.2가 이미 설치되어 별도 패키지 설치는 하지 않았다.
- 셸 DNS는 외부 registry/Jina 조회 실패. Docker 데몬의 이미지 pull은 성공했다.
- PostgreSQL: `pgvector/pgvector:0.8.7-pg17-trixie`, ARM64, digest
  `sha256:7a7e9f22015b67edb4bef5c59daeebcd7e74bfa570df6ce60ae01237c8648a84`.
- Qdrant: `qdrant/qdrant:v1.17.1`, ARM64, digest
  `sha256:94728574965d17c6485dd361aa3c0818b325b9016dac5ea6afec7b4b2700865f`.
- Qdrant 1.17.1은 고정한 안정 릴리스이며 최신 버전이라는 주장은 하지 않는다.
- ego-browser로 공유 대화를 재조회했으나 `Failed to start ego lite`로 실패했다.
  참조 대화 내용은 여전히 미확인이다. 설치·업데이트·우회 브라우저 접속은 하지 않았다.
- 사용자가 WDL 프로젝트 설정 없이 진행을 명시적으로 선택했다. 해당 선택을
  `.git/wdl/hook-state.json`에 공식 decision CLI로 기록했다. 프로젝트 전용 WDL 검사는
  실행하지 않으며 독립 방법론 검사와 실제 DB 실험 결과를 별도로 보고한다.

## 첫 실패: 가변 길이 검색 결과 저장

`results/20261024T185124Z-smoke-bc5be8`은 PostgreSQL 삭제 시나리오에서
`ValueError: setting an array element with a sequence`로 실패했다.
검색 결과가 k개 미만일 때 NumPy 직사각형 배열 변환에 실패한 것이다.
고정 길이 패딩과 실제 결과 길이를 함께 저장하도록 수정하고, 결과 부족이
재현율·부족률에 그대로 반영되는 회귀 검사를 추가했다. 실패 원본은 보존한다.

## 두 번째 실패: Qdrant 최소 임계값과 PostgreSQL 설정 누락

`results/20261024T185212Z-smoke-93961b`은 Qdrant 컬렉션 생성에서
`422 ... full_scan_threshold: value 0 invalid, must be 10 or larger`로 실패했다.
해당 버전의 최소 허용값 10KiB로 수정했다. 이때의 PostgreSQL 표본은
`hnsw.iterative_scan`을 빠뜨린 상태여서 삭제 후 결과 부족이 발생했다.
공식 strict_order 반복 탐색을 활성화한 뒤 별도 실행 ID로 다시 검증했다.
이전 표본을 제품 고유 열화의 증거로 사용하지 않는다.

## 스모크 증거 보강

`results/20261024T185252Z-smoke-207684`에서 양쪽 DB의 18 checkpoint와 6 대조군이
통과했다. 이후 PostgreSQL EXPLAIN을 실제 ANALYZE/BUFFERS 실행으로 보강하고,
원시 NPZ 해시 검증과 server stdout/stderr 분리 보존을 추가했다.
소스가 달라진 이전 스모크를 본 파일럿의 선행 증거로 재사용하지 않는다.
현재 소스로 새 스모크를 수행한 결과는 최신 manifest를 따른다.
