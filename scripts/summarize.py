"""Create the human-readable pilot summary from one explicit validated run."""
import json
import os
from pathlib import Path
import sys

from vectorbench.experiments.pilot.report import load, validate_run


def span(values, percent=True):
    factor = 100 if percent else 1
    return f"{min(values) * factor:.2f}–{max(values) * factor:.2f}"


def summarize(root, target=Path("docs/pilot/results.md")):
    target = Path(target).resolve()
    validate_run(root)
    m = load(root / "manifest.json")
    if m["status"] != "passed":
        raise ValueError("only a completed passed run may become the final summary")
    budgets = load(root / "budget.json")
    cp = [load(p) for p in root.glob("seed-*/*/*/checkpoint-[0-9]*.json")]
    done = list(root.glob("seed-*/*/*/done.json"))
    raw_bytes = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
    lines = ["# 로컬 증분 업데이트 최소 파일럿 결과", "",
             f"실행: `{m['id']}`. 실행 및 원시 증거 검증 **PASS**.", "",
             f"독립 이력 {len(done)}/{m['planned_histories']}, checkpoint {len(cp)}/{m['planned_checkpoints']}, 재구축 대조군 {len(done)}/{m['planned_histories']}.",
             "합성 10,000건·128차원, seed 3개, 평가 질의 800개, 변경 회차 5회.",
             "삽입은 15,000건, 삭제는 5,000건, churn은 10,000건으로 종료했다.",
             "회차 사이 PostgreSQL 테이블·HNSW OID를 유지하고 Qdrant의 동일 컬렉션에 변경을 누적했다.", "",
             "PASS는 실험 수행·원장 대조·증거 검증 완료를 뜻한다. 모든 checkpoint의 품질 유지나 제품 순위의 증명이 아니다.", "",
             "## 검색 품질", "",
             "값은 seed 3개의 최솟값–최댓값이다. 재현율은 %, 재구축 대비 차이는 퍼센트포인트다.", "",
             "| DB | 시나리오 | 초기 R@100 (%) | 최종 R@100 (%) | 최종 − 재구축 (%p) |",
             "|---|---|---:|---:|---:|"]
    for db in ("postgres", "qdrant"):
        for scenario in ("insert", "delete", "churn"):
            initial, final, deltas = [], [], []
            for seed in m["config"]["seeds"]:
                p = root / f"seed-{seed}" / db / scenario
                initial.append(load(p / "checkpoint-0.json")["recall100"])
                final.append(load(p / f"checkpoint-{m['config']['rounds']}.json")["recall100"])
                deltas.append(load(p / "done.json")["delta_recall100"])
            lines.append(f"| {db} | {scenario} | {span(initial)} | {span(final)} | {span(deltas)} |")
    under = sum(r["recall100"] < .95 or r["recall10"] < .95 for r in cp)
    lines += ["", f"평가 checkpoint 중 초기 튜닝 목표 0.95 미달: {under}/{len(cp)}개. 평가 결과로 재튜닝하지 않았다.", "",
              "삽입 후 초기보다 낮아진 재현율에는 데이터 수 증가 효과가 섞인다. 같은 최종 데이터의 재구축 차이를 함께 해석해야 한다.",
              "삭제·churn의 차이에는 dead tuple, optimizer, 구축 순서·병렬 난수 차이가 섞인다. 이를 인덱스의 영구 열화로 단정하지 않는다.", "",
              "## 자원과 시간", "",
              f"- 본 파일럿 전체 소요: {m['elapsed_s'] / 60:.1f}분. 튜닝·재시작·정답 계산·대조군·정리 포함, 사전 설치·스모크 제외.",
              f"- 본 실행 결과 디렉터리: 약 {raw_bytes / 2**20:.1f}MiB (이 요약 작성 전 기준).",
              f"- 새 DB 이미지 논리 크기 합계: {budgets[-1]['image_bytes'] / 2**20:.1f}MiB. Docker VM 물리 할당량과는 다르다.",
              f"- 실행 중 호스트 공간 감소 관측 최댓값: {max(b['conservative_host_growth_bytes'] for b in budgets) / 2**20:.1f}MiB. 이미지 다운로드 이후 기준이며 다른 호스트 작업 영향도 포함한다.",
              f"- 실행 중 최소 호스트 여유: {min(b['free_bytes'] for b in budgets) / 2**30:.2f}GiB.",
              "- DB별 2 vCPU·4GiB, Colima 전체 4 vCPU·약 7.7GiB. DB는 순차 실행.",
              "- 전용 컨테이너·볼륨은 정리했고 이미지는 재실행용 캐시로 보존했다. 기존 컨테이너·볼륨은 보존 여부를 inventory로 대조한다.", "",
              "## 근거와 한계", "",
              "- 단위·결함 주입 검사 13개 통과. 실제 스모크가 선행 조건이며 소스 해시가 일치하는 증거만 사용했다.",
              "- PostgreSQL은 실제 EXPLAIN ANALYZE에서 HNSW를 확인했다. Qdrant는 인덱싱 카운터·HNSW 설정으로 확인했으며 개별 질의 내부 trace는 아니다.",
              "- PostgreSQL은 commit 가시성을 확인했으며 VACUUM 완료를 기다리거나 강제 실행하지 않았다. Qdrant는 optimizer 안정화를 최대 120초 관찰했다. 유지보수 정책을 통제한 알고리즘 단독 비교가 아니다.",
              "- 제한된 두 구축 후보·세 유효 검색 ef 후보에서 선택했다. 절대적 최적 튜닝이나 통계적 동등성의 주장은 하지 않는다.",
              "- 지연은 서로 다른 클라이언트 전송 경로와 공유 Mac/VM 영향을 포함하므로 DB 순위로 사용하지 않는다.",
              "- 공유 ChatGPT 대화는 ego-browser 시작 실패로 미확인이다. WDL 프로젝트 전용 검사는 사용자 선택에 따라 NOT RUN.",
              "- 실제 임베딩·장기 churn·동시 부하·필터·Milvus·Weaviate·IVFFlat·클라우드 실험은 보류한 전체 계획의 후속 항목이다.", "",
              f"상세 seed별 결과: [REPORT]({Path(os.path.relpath((root / 'REPORT.md').resolve(), target.parent)).as_posix()}). 원시 데이터와 설정은 같은 디렉터리에 보존한다.", "",
              "```sh", f"python3 -m vectorbench.experiments.pilot.report {root}", f"python3 -m scripts.summarize {root}", "```"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(lines) + "\n")
    print(target)


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
