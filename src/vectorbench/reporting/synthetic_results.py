"""Verify all raw evidence, then compare current-data and paired-path recall."""
import csv
import gzip
import json
from pathlib import Path
import statistics

import numpy as np

from vectorbench.backends.pilot import assert_postgres_ann
from vectorbench.experiments.pilot.model import digest, score, unpacked_results
from vectorbench.experiments.synthetic_insertion.contracts import canonical, load, read_ack, save, sha
from vectorbench.evidence.synthetic_telemetry import verify_delta


def validate_point(root, point, db, fixture, metadata):
    folder = root / point["relative_path"]
    receipt = load(folder / "receipt.json")
    if receipt["raw_sha256"] != sha(folder / "raw.npz"):
        raise AssertionError("measurement raw file changed")
    expected = metadata["states"][receipt["data_hash"]]
    with np.load(root / expected["path"]) as oracle, np.load(folder / "raw.npz") as raw:
        np.testing.assert_array_equal(raw["ids"], oracle["ids"])
        np.testing.assert_array_equal(raw["truth"], oracle["truth"])
        ids = raw["ids"]
        if digest(ids, fixture["pool"][ids]) != receipt["data_hash"]:
            raise AssertionError("quality result scored against another data state")
        rows = unpacked_results(raw)
        quality = score(ids, raw["truth"], rows)
        for name, value in quality.items():
            if not np.isclose(receipt[name], value, atol=1e-12, rtol=0):
                raise AssertionError("quality summary differs from raw response")
        if len(raw["latency_ms"]) != len(fixture["queries"]):
            raise AssertionError("query/latency observations missing")
        for group, name in enumerate(("old", "new", "boundary")):
            take = np.flatnonzero(raw["cohort"] == group)
            quality = score(ids, raw["truth"][take], [rows[i] for i in take])
            for metric, value in quality.items():
                if not np.isclose(receipt["groups"][name][metric], value, atol=1e-12, rtol=0):
                    raise AssertionError("cohort summary differs from raw response")
    if db == "postgres":
        state = load(folder / "state.json")
        assert_postgres_ann(load(folder / "path.json"), state["enable_seqscan"], "items_hnsw", analyzed=True)
        with gzip.open(folder / "query-plans.json.gz", "rt") as stream:
            guards = json.load(stream)
        if len(guards) != receipt["queries"]:
            raise AssertionError("per-query plan guards missing")
        for guard in guards:
            assert_postgres_ann(guard["plan"], guard["setting"], "items_hnsw")
    else:
        verify_delta(load(folder / "telemetry-before.json"), load(folder / "telemetry-after.json"), receipt["queries"])
    return receipt


def cohort(point, name):
    return point if name == "overall" else point["groups"][name]


def summarize(root, out, manifest):
    fixtures, metas = {}, {}
    fresh = {}
    histories = []
    counts = {"paths": 0, "incremental_checkpoints": 0, "fresh_checkpoints": 0, "quality_measurements": 0}
    for key, spec in manifest["cells"].items():
        if spec["kind"] not in {"fresh", "incremental"}:
            continue
        result, _ = read_ack(root, key)
        result = result["result"]
        fk = spec["fixture"]
        if fk not in fixtures:
            with np.load(root / "inputs" / fk / "fixture.npz") as stream:
                fixtures[fk] = {k: stream[k] for k in stream.files}
            metas[fk] = load(root / "inputs" / fk / "metadata.json")
        points = [validate_point(root, p, spec["db"], fixtures[fk], metas[fk]) for p in result["points"]]
        if spec["kind"] == "fresh":
            if len(points) != 1 or points[0]["data_hash"] != spec["state"]:
                raise AssertionError("fresh control missing or wrong data")
            fresh.setdefault((spec["db"], fk, spec["state"]), []).append(points[0])
            counts["fresh_checkpoints"] += 1
        else:
            expected_steps = [0, spec["waves"]] if spec["policy"] == "continuous" else list(range(spec["waves"] + 1))
            if len(points) != len(expected_steps):
                raise AssertionError("history checkpoints missing")
            if len(result["writes"]) != spec["waves"] or any(len(w["requests"]) != 1 for w in result["writes"]):
                raise AssertionError("history wave/request boundary mismatch")
            for step, point in zip(expected_steps, points):
                if point["data_hash"] != spec["states"][step]:
                    raise AssertionError("incremental checkpoint state mismatch")
            histories.append((spec, points, expected_steps, result))
            counts["paths"] += 1
            counts["incremental_checkpoints"] += len(points)
    counts["quality_measurements"] = counts["incremental_checkpoints"] + counts["fresh_checkpoints"]
    if counts != manifest["expected"]:
        raise AssertionError("incomplete study: " + canonical(counts))
    curves, endpoints = [], []
    for spec, points, steps, result in histories:
        for step, point in zip(steps, points):
            controls = [point] if step == 0 else fresh[(spec["db"], spec["fixture"], point["data_hash"])]
            for group in ("overall", "old", "new", "boundary"):
                measured = cohort(point, group)
                row = {name: spec[name] for name in ("id", "db", "seed", "geometry", "ratio", "waves", "order", "policy", "stage")}
                row.update(step=step, count=point["count"], cohort=group,
                    fresh_repeats=len(controls), data_hash=point["data_hash"],
                    initial_snapshot_sha=result["initial_snapshot_sha"])
                for k in (10, 100):
                    values = [cohort(p, group)[f"recall{k}"] for p in controls]
                    row.update({f"recall{k}": measured[f"recall{k}"], f"fresh{k}": statistics.mean(values),
                        f"fresh{k}_min": min(values), f"fresh{k}_max": max(values),
                        f"gap{k}": measured[f"recall{k}"] - statistics.mean(values)})
                curves.append(row)
                if step == spec["waves"]:
                    endpoints.append(row)
    for name, rows in (("curves.csv", curves), ("endpoints.csv", endpoints)):
        with open(out / name, "w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    paired = []
    lookup = {(r["db"], r["seed"], r["geometry"], r["ratio"], r["order"], r["policy"], r["waves"], r["cohort"]): r for r in endpoints}
    for key, row in lookup.items():
        db, seed, geo, ratio, order, policy, waves, group = key
        if waves != 1:
            continue
        other = lookup[(db, seed, geo, ratio, order, policy, 5, group)]
        if row["data_hash"] != other["data_hash"] or row["initial_snapshot_sha"] != other["initial_snapshot_sha"]:
            raise AssertionError("paired paths do not share initial index and final data")
        paired.append({"db": db, "seed": seed, "geometry": geo, "ratio": ratio, "order": order,
                       "cohort": group, "one_minus_five_r10": row["recall10"] - other["recall10"],
                       "one_minus_five_r100": row["recall100"] - other["recall100"]})
    with open(out / "paired-paths.csv", "w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(paired[0]))
        writer.writeheader()
        writer.writerows(paired)
    lines = ["# 삽입 품질 변화 후속 실험", "", f"실행 ID: `{root.name}`", "",
             "품질: exact oracle 기준 SOLID. 지연·구축 시간: 공유 Mac/VM의 INDICATIVE.", "",
             f"경로 {counts['paths']}, 증분 checkpoint {counts['incremental_checkpoints']}, fresh {counts['fresh_checkpoints']}, 총 {counts['quality_measurements']}개 검증 완료.",
             "이 수는 독립 표본 수가 아니다. 데이터 seed별 대응 차이와 fresh 내부 구축 반복을 구분한다.", "",
             "## 최종 전체 Recall@100과 fresh 대비 차이", "",
             "아래는 seed별 범위(%)이며 차이는 %p다. 세부 집단·시점별 값은 CSV를 따른다.", "",
             "| DB | geometry | 신규/초기 % | wave | 순서 | 정책 | 최종 R@100 | history gap |",
             "|---|---|---:|---:|---|---|---:|---:|"]
    groups = {}
    for row in endpoints:
        if row["cohort"] == "overall":
            group_key = tuple(row[n] for n in ("db", "geometry", "ratio", "waves", "order", "policy"))
            groups.setdefault(group_key, []).append(row)
    for key, rows in sorted(groups.items()):
        values, gaps = [r["recall100"] * 100 for r in rows], [r["gap100"] * 100 for r in rows]
        lines.append("| " + " | ".join(map(str, key)) + f" | {min(values):.3f}–{max(values):.3f} | {min(gaps):+.3f}–{max(gaps):+.3f} |")
    warnings = []
    by_condition = {}
    for row in endpoints:
        for k in (10, 100):
            key = tuple(row[n] for n in ("db", "geometry", "ratio", "waves", "order", "policy", "cohort")) + (f"recall{k}",)
            if row[f"gap{k}"] <= -.005 and row[f"recall{k}"] < row[f"fresh{k}_min"]:
                by_condition.setdefault(key, []).append(row["seed"])
    warnings = [{"condition": key, "seeds": seeds} for key, seeds in by_condition.items() if len(seeds) >= 2]
    save(out / "warnings.json", warnings)
    lines += ["", "## 해석과 한계", "",
              f"R@10·100의 -0.5%p 경고 기준에 해당하는 조건 수: {len(warnings)}. 조건과 seed는 warnings.json에 기록했다.",
              "경고는 구조적 손상이나 제품 순위의 증명이 아니다. fresh 반복의 변동과 집단별 차이를 함께 해석한다.",
              "", "기존·신규·경계 질의 집단을 동일 가중으로 집계했다. 실제 질의 트래픽 비율이 아니다.",
              "현재 데이터의 난도와 갱신 이력을 구분하기 위해 모든 고유 중간/최종 데이터에 fresh 대조군을 두었다.",
              "exact kNN 그래프 진단은 제품 내부 ANN 그래프 관측이 아니다. 세 데이터 seed로 동등성을 주장하지 않는다.",
              "API 요청/커밋 크기와 내부 그래프 편입·merge 단위는 구분한다. 중간 측정·대기의 영향은 continuous 조건과 함께 읽는다.",
              "모든 ANN 검색에서 PostgreSQL 계획 또는 Qdrant segment 카운터 검증을 통과했다. 원장·oracle 전체 읽기는 별도다.",
              "WDL 프로젝트 설정은 사용자의 without-setup 선택을 유지했다. 1차 파일럿과 원시 증거는 덮어쓰지 않았다.", "",
              "## 재현", "", "```sh", f".venv/bin/python -m vectorbench.experiments.synthetic_insertion.launch {root}", "```",
              "완료 셀은 native Dagster 성공 run과 artifact ack를 검증해 건너뛴다. 실패 셀은 명시적 retry disposition 없이는 실행하지 않는다."]
    (out / "REPORT.md").write_text("\n".join(lines) + "\n")
    save(out / "counts.json", counts)
    return {"counts": counts, "warnings": len(warnings), "report": str((out / "REPORT.md").relative_to(root))}
