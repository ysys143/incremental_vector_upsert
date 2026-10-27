"""Scientific task consumers; implemented entry smoke is distinct from the main study."""
from pathlib import Path
import json
import subprocess
import numpy as np

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.datasets.data import Pool
from vectorbench.workloads.events import Live
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.backends.server import Server
from vectorbench.experiments.real_vectors.smoke_data import OPERATIONS, config, prepare
from vectorbench.datasets.storage import load_events, load_state
from vectorbench.workloads.workload import fresh, history, oracle_for, read_point


def parents(root, out):
    result = {}
    for ref in load(out / "admission.json")["dependencies"]:
        path = root / "cells" / ref["node"].replace(":", "__") / ref["run_id"] / "receipt.json"
        if sha(path) != ref["sha256"]:
            raise AssertionError("native parent binding changed")
        result[ref["node"]] = load(path)["result"]
    return result


def pool_from(prepared):
    ref = prepared["source"]
    pool = Pool.open(ref["dataset"], ref["base_path"], ref["query_path"])
    if pool.base_sha != ref["base_sha256"] or pool.query_sha != ref["query_sha256"]:
        raise AssertionError("prepared data source changed")
    return pool


def snapshot(db, pool, prepared, tuning, out, c=None):
    c = c or config()
    live = load_state(prepared["initial"], len(pool.vectors))
    with np.load(prepared["query_rows"]) as q:
        query_rows = q["evaluation"]
    cohorts = ["entry-all"] * len(query_rows)
    control = prepared["paths"]["insert"]["controls"][0]
    truth = oracle_for(control, pool, live, query_rows)
    server = Server(db, out / "server", config=c)
    error = None
    try:
        adapter = server.start()
        adapter.batch = tuning["initial_batch"]
        ids, rows, attrs = metadata_arrays(live)
        setup = adapter.setup(ids, pool.vectors[rows], tuning["build"], attributes=attrs)
        adapter.set_ef(tuning["ef"])
        before = read_point(server, adapter, live, pool, pool.queries[query_rows], truth, cohorts,
                            out / "before-restart", c, control)
        adapter = server.restart()
        adapter.set_ef(tuning["ef"])
        after = read_point(server, adapter, live, pool, pool.queries[query_rows], truth, cohorts,
                           out / "after-restart", c, control)
        target = out / "initial.tar.gz"
        exported = server.export(target)
        archive = {k: exported[k] for k in ("archive_sha", "files_hash", "segment_signature", "graph_hash")}
        archive["archive"] = str(target)
        return {"archive": archive, "export": exported, "setup": setup, "before_restart": before,
                "after_restart": after, "initial_ledger_sha256": live.digest()}
    except BaseException as exc:
        error = exc
        raise
    finally:
        errors = server.cleanup()
        if errors:
            save(out / "cleanup-failure.json", {"errors": errors, "first_error": repr(error)})
            if error is None:
                raise RuntimeError("snapshot cleanup failed: " + repr(errors))


def report(dependencies, out, large_requests=False):
    histories = {k: v for k, v in dependencies.items() if k.startswith("history:")}
    controls = {k: v for k, v in dependencies.items() if k.startswith("fresh:")}
    expected = {f"{dataset}-{db}-{operation}" for dataset in ("msmarco", "wikipedia")
                for db in ("postgres", "qdrant") for operation in OPERATIONS}
    if {k.split(":", 1)[1] for k in histories} != expected or {k.split(":", 1)[1] for k in controls} != expected:
        raise AssertionError("entry smoke is missing a required DB/dataset/operation")
    rows, points = [], 0
    rounds = 1 if large_requests else 2
    for key in sorted(expected):
        inc, rebuilt = histories["history:"+key], controls["fresh:"+key]
        if inc["steps"] != list(range(rounds+1)) or len(inc["writes"]) != rounds or len(inc["points"]) != rounds+1:
            raise AssertionError("entry smoke checkpoint/write count mismatch")
        if large_requests:
            operation = key.split("-", 2)[2]
            write = inc["writes"][0]
            expected_write = 10000 if operation == "insert" else 0 if operation == "delete" else 5000
            expected_delete = 5000 if operation in {"delete", "churn", "reinsert"} else 0
            if write["written"] != expected_write or write["deleted"] != expected_delete or write["status"] != "ACKNOWLEDGED":
                raise AssertionError("large request did not acknowledge its declared row count")
            for request in write["requests"]:
                if request["kind"] == "COPY" and request["rows"] != expected_write:
                    raise AssertionError("large COPY was silently split")
        if inc["final_ledger_sha256"] != rebuilt["state"]:
            raise AssertionError("fresh control has a different current live set/version")
        a, b = inc["points"][-1], rebuilt["point"]
        if any(a[field] != b[field] for field in ("vector_state_sha256", "query_sha256", "ledger_sha256")):
            raise AssertionError("fresh/incremental comparison differs in state or query")
        for point in [*inc["points"], b]:
            if point["status"] != "PASS" or point["ann"]["status"] != "PASS" or point["metric"] != "ip":
                raise AssertionError("entry smoke requires actual IP ANN for every point")
            points += 1
        rows.append({"path": key, "recall10": a["recall10"], "recall100": a["recall100"],
                     "fresh_recall10": b["recall10"], "fresh_recall100": b["recall100"],
                     "delta_recall100": a["recall100"] - b["recall100"]})
    expected_points = 28*(rounds+2)
    if points != expected_points:
        raise AssertionError("smoke checkpoint count differs from declared scope")
    save(out / "comparisons.json", rows)
    title = "요청 크기 스모크" if large_requests else "진입 스모크"
    text = ["# 공개 벡터 DB " + title, "", f"{title} 28개 경로·{expected_points}개 품질 측정이 검증됐다.",
            "초기 snapshot의 재시작 전후 검사 및 튜닝 측정은 이 수에 포함하지 않는다.",
            "전체 5회·50회 연구, 기하 조건·필터 확장은 미완이며 이 스모크로 대체하지 않는다.", "",
            "| 경로 | Recall@100 | fresh | 차이(pp) |", "|---|---:|---:|---:|"]
    text += [f"| {r['path']} | {100*r['recall100']:.3f}% | {100*r['fresh_recall100']:.3f}% | {100*r['delta_recall100']:.3f} |" for r in rows]
    (out / "report.md").write_text("\n".join(text) + "\n")
    return {"scope": "REQUEST_SMOKE_ONLY" if large_requests else "ENTRY_SMOKE_ONLY", "paths": 28, "incremental_points": 28*(rounds+1), "fresh_points": 28,
            "main_study": "NOT RUN", "report": str(out / "report.md")}


def execute(root, node, run_id, out, manifest, spec):
    if manifest['mode'] == 'real-clustered-insertion':
        from vectorbench.experiments.real_vectors.clustered.run import execute as clustered_execute
        return clustered_execute(root,node,run_id,out,manifest,spec)
    if manifest['mode'] == 'real-twelve-hour':
        from vectorbench.experiments.real_vectors.twelve_hour.twelve_run import execute as twelve_execute
        return twelve_execute(root,node,run_id,out,manifest,spec)
    if manifest["mode"].startswith("real-baseline-"):
        from vectorbench.experiments.real_vectors.baseline.baseline_actions import execute as baseline_execute
        return baseline_execute(root,node,run_id,out,manifest,spec)
    if manifest["mode"] not in {"real-db-smoke", "real-request-smoke"}:
        raise NotImplementedError("main-study and filtered actions have not been qualified")
    deps = parents(root, out)
    large_requests = manifest["mode"] == "real-request-smoke"
    action = spec["action"]
    if action == "adopt":
        ref = spec["adopt"]
        if sha(root / "reuse-proof.json") != ref["proof_sha256"]:
            raise AssertionError("unchanged-consumer adoption proof changed")
        previous = load(Path(ref["root"]) / "manifest.json")
        if previous["config"] != manifest["config"] or previous["nodes"][node] != ref["scientific_spec"]:
            raise AssertionError("adoption scientific configuration changed")
        result = subprocess.run([manifest["orchestration_python"], "-m", "vectorbench.experiments.real_vectors.adoption",
            ref["root"], node, ref["receipt_sha256"]], capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise AssertionError("native adoption failed: " + result.stderr[-2000:])
        adopted = json.loads(result.stdout)
        save(out / "native-adoption.json", adopted["native_adoption"])
        return adopted
    if action == "barrier":
        return {"scope": "REQUEST_SMOKE_ONLY" if large_requests else "ENTRY_SMOKE_ONLY", "native_parents": sorted(deps)}
    if action == "report":
        return report(deps, out, large_requests)
    if action == "prepare":
        result = subprocess.run([manifest["orchestration_python"], "-m", "vectorbench.evidence.native_read",
            manifest["source_root"], spec["dataset"]], capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise AssertionError("real native source producer not qualified: " + result.stderr[-2000:])
        source = json.loads(result.stdout)
        if source != manifest["source_refs"][spec["dataset"]]:
            raise AssertionError("native source differs from the frozen smoke prerequisite")
        pool = Pool.open(source["dataset"], source["base_path"], source["query_path"])
        prepared = prepare(pool, out, large_requests)
        prepared["source"] = source
        save(out / "source.json", source)
        return prepared
    prepared = deps["preparation:" + spec["dataset"]]
    pool = pool_from(prepared)
    with np.load(prepared["query_rows"]) as q:
        query_rows = q["calibration" if action == "tune" else "evaluation"]
    c = config(large_requests)
    if action == "tune":
        from vectorbench.evaluation.calibration import tune
        live = load_state(prepared["calibration"], len(pool.vectors))
        return tune(spec["db"], pool, live, query_rows, prepared["calibration_control"], out, c)
    tuning = deps["tuning:" + spec["dataset"] + "-" + spec["db"]]
    if action == "snapshot":
        return snapshot(spec["db"], pool, prepared, tuning, out, c)
    path = prepared["paths"][spec["operation"]]
    cohorts = ["entry-all"] * len(query_rows)
    if action == "history":
        frozen = deps["initial:" + spec["dataset"] + "-" + spec["db"]]
        start = load_state(prepared["initial"], len(pool.vectors))
        return history(spec["db"], pool, start.records(), load_events(path["events"]), query_rows,
                       cohorts, path["controls"], tuning, frozen["archive"], out, config=c)
    if action == "fresh":
        live = load_state(path["final"], len(pool.vectors))
        return fresh(spec["db"], pool, live, query_rows, cohorts, path["controls"][-1], tuning, out, c)
    raise ValueError("unknown scientific action: " + action)
