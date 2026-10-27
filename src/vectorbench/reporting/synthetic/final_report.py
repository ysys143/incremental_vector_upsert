"""Write a readable Markdown report from verified derived and native evidence."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import time

from vectorbench.experiments.synthetic_insertion.contracts import load, read_ack, save, sha


def spread(value, percent=True):
    scale = 100 if percent else 1
    return f"{value['mean'] * scale:.3f} [{value['min'] * scale:.3f}, {value['max'] * scale:.3f}]"


def run(root, target):
    root = root.resolve()
    target = target.resolve()
    receipt, ack = read_ack(root, "summary:all")
    audit = load(root / "analysis/native-audit.json")
    if audit["status"] != "PASS" or not audit["asset_check_passed"] or audit["summary_run_id"] != ack["run_id"]:
        raise AssertionError("native audit/check missing")
    q = load(root / "analysis/quality-summary/aggregates.json")
    q_receipt = load(root / "analysis/quality-summary/receipt.json")
    if q_receipt["aggregate_sha256"] != sha(root / "analysis/quality-summary/aggregates.json"):
        raise AssertionError("derived quality artifact changed")
    geometry_receipt = load(root / "analysis/geometry-cohorts/receipt.json")
    for name, digest in geometry_receipt["files"].items():
        if sha(root / "analysis/geometry-cohorts" / name) != digest:
            raise AssertionError("derived geometry artifact changed")
    manifest = load(root / "manifest.json")
    if manifest["config"]["smoke"] or manifest["config"]["seeds"] != [11, 29, 47]:
        raise AssertionError("this narrative report is for the approved three-seed full local study")
    expected_geometry_rows = len(manifest["config"]["seeds"]) * (1 + 2 * 4 * len(manifest["config"]["ratios"]))
    if geometry_receipt["status"] != "PASS" or geometry_receipt["rows"] != expected_geometry_rows:
        raise AssertionError("old/new geometry diagnostics incomplete")
    ledger = load(root / "ledger.json")
    counts = receipt["result"]["counts"]
    clocks = load(root / "stage-clocks.json")
    end = time.time()
    free = shutil.disk_usage(root).free
    own_names = {r["name"] for path in (root / "cells").rglob("resources.json")
                 for r in load(path)["resources"]}
    observed = {}
    for kind, command in (("containers", ["docker", "ps", "-a", "--format", "{{.Names}}"]),
                          ("volumes", ["docker", "volume", "ls", "--format", "{{.Name}}"])):
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=True)
        observed[kind] = sorted(own_names.intersection(result.stdout.splitlines()))
    if any(observed.values()):
        raise AssertionError("declared campaign Docker resources remain: " + str(observed))
    retained = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
    evidence = {"status": "PASS", "finished": end, "campaign_retained_bytes": retained,
                "host_free_bytes": free, "declared_resources_remaining": observed,
                "declared_resource_names": len(own_names), "native_runs": audit["native_runs"],
                "quality_measurements": counts["quality_measurements"],
                "summary_run_id": ack["run_id"], "code_sha256": sha(Path(__file__))}
    phase_starts = sorted((v["started"], k) for k, v in clocks.items())
    last_end = max(a["started"] + load(root / a["out"] / "receipt.json")["elapsed_s"]
                   for v in ledger.values() for a in v["attempts"])
    evidence["phase_elapsed_minutes"] = {
        name: ((phase_starts[i + 1][0] if i + 1 < len(phase_starts) else last_end) - start) / 60
        for i, (start, name) in enumerate(phase_starts)}
    save(root / "analysis/final-audit.json", evidence)
    relative = Path(os.path.relpath(root, target.parent)).as_posix()

    def doc_link(path):
        return Path(os.path.relpath(Path(path).resolve(), target.parent)).as_posix()
    summary_path = Path(ledger["summary:all"]["attempts"][-1]["out"])
    endpoints = q["endpoint_groups"]
    warnings = q["checkpoint_warning_conditions"]
    overall = [r for r in endpoints if r["cohort"] == "overall"]
    worst = min(overall, key=lambda r: r["gap100"]["mean"])
    lines = ["# 로컬 삽입 품질 실험 결과", "",
        f"실행 `{root.name}`의 native 셀 {audit['native_runs']}/{len(manifest['cells'])}개와 최종 asset check가 통과했다.",
        f"경로 {counts['paths']}개, 증분 시점 {counts['incremental_checkpoints']}개, fresh 측정 {counts['fresh_checkpoints']}개, 총 {counts['quality_measurements']}개 품질 측정을 검증했다.",
        "이번 로컬 삽입 연구의 완료이며, 보관된 전체 다중 DB·클라우드·필터·삭제/upsert 연구의 완료는 아니다.", "",
        "## 주요 관측", "",
        f"- 전체 질의 기준 가장 낮은 평균 최종 history gap@100은 `{worst['db']}/{worst['geometry']}/r{worst['ratio']}/w{worst['waves']}/{worst['order']}/{worst['policy']}`의 {spread(worst['gap100'])}%p다.",
        f"- 중간·최종 시점 전체에서 사전 -0.5%p 기준을 만족하고 fresh 최소값보다 낮은 결과가 2개 이상 seed에 나타난 조건은 {len(warnings)}개다. 0개여도 동등성이 입증된 것은 아니다.",
        "- 아래 평균과 대괄호 범위는 데이터 seed 11·29·47의 평균 [최솟값, 최댓값]이다. 신뢰구간이 아니다.", "",
        "## 1. 삽입 전후 품질과 같은 데이터의 재구축", "",
        "최대 삽입량(10K+2K×5), 무작위 순서, 매 wave 인덱싱·측정 대기 조건이다.",
        "삽입 전 값은 미래 신규·경계 질의도 D0만 검색한 값이다. fresh는 각 최종 상태의 3회 재구축 평균을 seed별로 구한 뒤 집계했다.", "",
        "| DB | 신규 geometry | 삽입 전 R@100 (%) | 삽입 후 R@100 (%) | fresh R@100 (%) | history gap (%p) |",
        "|---|---|---:|---:|---:|---:|"]
    for row in overall:
        if row["ratio"] == 100 and row["waves"] == 5 and row["order"] == "random" and row["policy"] == "settled":
            lines.append(f"| {row['db']} | {row['geometry']} | {spread(row['initial100'])} | {spread(row['recall100'])} | {spread(row['fresh100'])} | {spread(row['gap100'])} |")
    lines += ["", "현재 데이터량 증가와 난도 변화는 fresh에도 영향을 준다. 삽입 전후 하락 전체를 갱신 이력의 손상으로 해석하지 않는다.",
              "10% 삽입, Recall@10, 모든 집단·시점의 값은 아래 CSV 및 aggregates.json에 함께 남겼다.", "",
              "## 2. 같은 최종 데이터: 한 번과 다섯 번", "",
              "같은 초기 인덱스와 최종 ID·벡터 집합을 확인한 대응 비교다. 양수는 한 번 경로의 R@100이 더 높다는 뜻이다.",
              "API 요청/커밋의 분할과 중간 대기를 포함한 경로 비교다. 제품 내부 ANN 구축 단위를 직접 제어한 결과는 아니다.", "",
              "| DB | geometry | 신규/초기 (%) | 순서 | 한 번−다섯 번 R@100 (%p) |",
              "|---|---|---:|---|---:|"]
    for row in q["one_minus_five_groups"]:
        if row["cohort"] == "overall":
            lines.append(f"| {row['db']} | {row['geometry']} | {row['ratio']} | {row['order']} | {spread(row['one_minus_five_r100'])} |")
    lines += ["", "## 3. 순서 및 중간 관찰·대기", "",
              "동일 초기 인덱스·최종 데이터 쌍이다. ordered−random, continuous−settled의 R@100 차이다.",
              "초기 그래프를 복제했어도 이후 삽입 시 backend PRNG를 동일 seed로 제어하지 못한다. 작은 차이를 순서·분할만의 인과 효과로 확정하지 않는다.", "",
              "| 비교 | DB | geometry | wave | 차이 (%p) |", "|---|---|---|---:|---:|"]
    for name, label in (("ordered_minus_random_groups", "순서"), ("continuous_minus_settled_groups", "관찰·대기")):
        for row in q[name]:
            if row["cohort"] == "overall":
                lines.append(f"| {label} | {row['db']} | {row['geometry']} | {row['waves']} | {spread(row['delta100'])} |")
    lines += ["", "## 4. 집단별 확인과 기하 진단", "",
              "기존·신규·경계 질의를 각 200개씩 동일 가중 집계했다. 실제 서비스의 질의 비율이 아니다.",
              "아래는 전체 조건 중 집단별 가장 낮은 평균 최종 history gap이다. 조건이 서로 다르므로 집단 간 직접 순위가 아니다.", "",
              "| DB | 질의 집단 | R@10 history gap 최솟값 (%p) | R@100 history gap 최솟값 (%p) |",
              "|---|---|---:|---:|"]
    for db in ("postgres", "qdrant"):
        for cohort in ("old", "new", "boundary"):
            rows = [r for r in endpoints if r["db"] == db and r["cohort"] == cohort]
            lines.append(f"| {db} | {cohort} | {min(r['gap10']['mean'] for r in rows)*100:.3f} | {min(r['gap100']['mean'] for r in rows)*100:.3f} |")
    lines += ["", "고정 입력의 exact 20-NN 그래프에서 연결 성분·교차 이웃·conductance와 MMD를 측정했다.",
              "추가 진단은 기존·신규 문서 집단별 NN1/NN20 거리와 LID 분포, 신규→D0 거리 / D0 내부 거리를 보존했다.",
              "이 값은 데이터의 이웃 구조이며 제품 내부 HNSW 그래프 관측값이 아니다. 거리·밀도·위상을 독립 조작한 완전 요인 실험도 아니다.", "",
              "## 5. 튜닝·검색 경로·자원", "",
              "- 별도 seed=7의 D0 및 네 최대 삽입 상태에서 공개 후보 중 Recall@10·100≥0.95를 충족하는 구성을 선택했다. 평가 중에는 동결했다.",
              "- PostgreSQL 17.11 / pgvector 0.8.7: M=32, efConstruction=200, efSearch=100. 초기 COPY 배치 1,000, 증분 wave 전체를 COPY/커밋 한 번으로 전송했다.",
              "- Qdrant 1.17.1: M=32, efConstruction=200, ef=128. 초기 전송 배치 1,000, 증분 wave마다 batch upsert 한 번이다.",
              "- PostgreSQL 모든 검색에서 seqscan off 및 지정 HNSW 계획을 검증하고 시점별 실제 실행도 확인했다. Qdrant는 HNSW 카운터 증가, plain/exact 증가 0, 안정된 segment를 확인했다.",
              "- M2 24GB, Colima 4 CPU·약 7.7GiB VM에서 DB별 2 CPU·4GiB, 한 번에 한 DB만 실행했다. 품질은 현재 상태 exact 정답에 대한 측정이다. 지연·처리량은 공유 호스트와 서로 다른 클라이언트 경로의 영향을 받으므로 제품 성능 순위로 해석하지 않는다.",
              f"- 입력·원시 결과·파생 분석의 캠페인 보존 크기는 약 {retained/1024**2:.1f}MiB, 보고 시 호스트 여유는 {free/1024**3:.2f}GiB였다. 기존 이미지·VM·과거 실험은 이 보존 크기에 포함하지 않았다.",
              f"- 선언된 캠페인 컨테이너·볼륨 잔존 수는 {sum(map(len, observed.values()))}개다. 기존 자원과 소유가 불명확한 자원을 전역 prune하지 않았다.",
              "- 단계별 실측 시간(분): " + ", ".join(f"{name} {minutes:.1f}" for name, minutes in evidence["phase_elapsed_minutes"].items()) + ". 파생 기하 분석 시간은 별도 receipt에 기록했다.",
              "- 추가 유료 API·클라우드 인스턴스를 사용하지 않았다. WDL 프로젝트 설정은 기존 without-setup 선택을 유지했다.", "",
              "## 6. 완료 증거와 재현", "",
              f"- native summary run: `{ack['run_id']}`; `summary/complete_evidence=PASS`.",
              f"- [전체 측정 보고서]({relative}/{summary_path}/REPORT.md)",
              f"- [시점·집단별 CSV]({relative}/{summary_path}/curves.csv), [최종 CSV]({relative}/{summary_path}/endpoints.csv), [대응 경로 CSV]({relative}/{summary_path}/paired-paths.csv)",
              f"- [seed별 집계·모든 시점 경고]({relative}/analysis/quality-summary/aggregates.json)",
              f"- [집단별 거리·LID 진단]({relative}/analysis/geometry-cohorts/cohorts.csv)",
              f"- [native run·asset check 감사]({relative}/analysis/native-audit.json), [정리·자원 감사]({relative}/analysis/final-audit.json)",
              f"- [동결된 실행 계획]({doc_link('docs/synthetic-insertion/plan.md')}), [스모크·실패 조치 기록]({doc_link('docs/synthetic-insertion/execution.md')})",
              f"- [제품 벤치마크 원문·제약]({doc_link('docs/research/product-benchmarks.md')}), [연구 수치 원문 확인]({doc_link('docs/research/literature-audit.md')})", "",
              "```sh", f".venv/bin/python -m vectorbench.experiments.synthetic_insertion.launch {root.relative_to(Path.cwd())}", "```",
              "완료 셀은 native run·ack·파일 해시를 검증하고 건너뛴다. 실패한 쓰기를 자동 재전송하지 않는다.", "",
              f"실제 임베딩·더 큰 규모·장기 삭제/동일 ID upsert·필터/선택도/카디널리티·추가 DB·클라우드 비교는 [기존 전체 계획]({doc_link('docs/archive/full-plan-2026-10-25.md')})의 미실행 범위로 남아 있다.",
              "이 결과만으로 그 범위의 품질이나 비용을 확정하지 않는다.", ""]
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".new")
    temporary.write_text("\n".join(lines))
    temporary.replace(target)
    save(root / "analysis/report-receipt.json", {"status": "PASS", "report": str(target),
         "report_sha256": sha(target), "code_sha256": sha(Path(__file__)),
         "inputs": {str(root / "analysis" / n): sha(root / "analysis" / n) for n in
                    ("native-audit.json", "quality-summary/receipt.json", "geometry-cohorts/receipt.json", "final-audit.json")}})
    print(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--report", type=Path, default=Path("docs/synthetic-insertion/results.md"))
    args = parser.parse_args()
    run(args.root, args.report)
