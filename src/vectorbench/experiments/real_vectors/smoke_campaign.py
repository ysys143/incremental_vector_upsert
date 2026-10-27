"""Freeze the entire real-data entry DAG; fail before creation if source is not ready."""
import argparse
from pathlib import Path
import sys
import subprocess

from vectorbench.evidence.contracts import GIB, check_space, save, sha
from vectorbench.experiments.real_vectors.smoke_spec import OPERATIONS, config
from vectorbench.evidence.registry import Registry
from vectorbench.evidence.source_manifest import freeze_sources


def nodes():
    result = {}
    def add(role, identity, resource, action, phase, deps=(), **fields):
        key = role + ":" + identity
        result[key] = {"id": identity, "role": role, "resource": resource, "action": action,
                       "phase": phase, "deps": list(deps), "reserve_bytes": 2*GIB if resource == "db" else 64*1024**2, **fields}
        return key
    datasets, dbs = ("msmarco", "wikipedia"), ("postgres", "qdrant")
    prep = {d: add("preparation", d, "cpu", "prepare", "PREPARE", dataset=d) for d in datasets}
    pgate = add("prepared", "all", "cpu", "barrier", "PREPARE", prep.values(), singleton=True)
    tunes = {(d, db): add("tuning", d+"-"+db, "db", "tune", "CALIBRATE", [prep[d], pgate], dataset=d, db=db) for d in datasets for db in dbs}
    tgate = add("calibrated", "all", "cpu", "barrier", "CALIBRATE", tunes.values(), singleton=True)
    initial = {(d, db): add("initial", d+"-"+db, "db", "snapshot", "SNAPSHOT", [prep[d], tunes[d, db], tgate], dataset=d, db=db) for d in datasets for db in dbs}
    igate = add("snapshotted", "all", "cpu", "barrier", "SNAPSHOT", initial.values(), singleton=True)
    histories = [add("history", d+"-"+db+"-"+op, "db", "history", "HISTORY",
        [prep[d], tunes[d, db], initial[d, db], igate], dataset=d, db=db, operation=op)
        for d in datasets for db in dbs for op in OPERATIONS]
    hgate = add("histories_done", "all", "cpu", "barrier", "HISTORY", histories, singleton=True)
    rebuilt = [add("fresh", d+"-"+db+"-"+op, "db", "fresh", "FRESH",
        [prep[d], tunes[d, db], hgate], dataset=d, db=db, operation=op)
        for d in datasets for db in dbs for op in OPERATIONS]
    add("report", "all", "cpu", "report", "REPORT", histories+rebuilt, singleton=True)
    return result


def prepare(root, source_root, large_requests=False, entry_smoke=None):
    from vectorbench.evidence.native_read import read
    prerequisite = None
    if large_requests:
        if entry_smoke is None:
            raise ValueError("large requests require completed native entry smoke")
        import dagster as dg
        from vectorbench.orchestration.engine import native_receipt
        if load_entry_mode(entry_smoke) != "real-db-smoke":
            raise AssertionError("large request prerequisite is not an entry smoke")
        with dg.DagsterInstance.from_config(str(entry_smoke.resolve()/"dagster-home")) as instance:
            receipt = native_receipt(entry_smoke.resolve(), "report:all", instance)
            checks = [e.asset_check_evaluation for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
            if len(checks) != 1 or not checks[0].passed or receipt["result"].get("scope") != "ENTRY_SMOKE_ONLY":
                raise AssertionError("entry smoke native check not passed")
            if any(receipt["result"].get(k) != v for k, v in {"paths":28,"incremental_points":84,"fresh_points":28}.items()):
                raise AssertionError("entry smoke does not contain every required path/checkpoint")
            from vectorbench.evidence.contracts import load
            if Path(load(entry_smoke / "manifest.json")["source_root"]).resolve() != source_root.resolve():
                raise AssertionError("large requests require entry smoke on the same real source")
            prerequisite = {"root": str(entry_smoke.resolve()), "native_run": receipt["run_id"],
                            "result": receipt["result"]}
    check_space(root.parent, reserve=2*GIB)
    running = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], check=True,
                             capture_output=True, text=True, timeout=15).stdout
    if running.strip():
        raise AssertionError("smoke preparation requires no active DB/container measurement")
    # This checks real native SUCCESS and blocking checks, not just files bearing PASS.
    source_refs = {d: read(source_root, d) for d in ("msmarco", "wikipedia")}
    baseline = Registry(source_root / "admission.sqlite").baseline_free()
    if baseline is None:
        raise AssertionError("source phase has no immutable original space budget")
    check_space(root.parent, baseline_free=baseline, reserve=2*GIB)
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    sources = freeze_sources(root)
    save(root / "manifest.json", {"mode": "real-request-smoke" if large_requests else "real-db-smoke", "config": config(large_requests).validate(),
         "measurement_python": "/opt/homebrew/bin/python3", "orchestration_python": sys.executable,
         "source_root": str(source_root.resolve()), "source_refs": source_refs, "sources": sources,
         "space_baseline_free": baseline,
         "nodes": nodes(), "expected": {"paths": 28, "history_points": 56 if large_requests else 84, "fresh_points": 28},
         "entry_smoke": prerequisite,
         "goal_scope": "REQUEST_SMOKE_ONLY; full five/fifty-round matrix remains required" if large_requests else "ENTRY_SMOKE_ONLY; full five/fifty-round matrix remains required"})
    return root


def load_entry_mode(root):
    from vectorbench.evidence.contracts import load
    return load(root / "manifest.json")["mode"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--large-requests", action="store_true")
    parser.add_argument("--entry-smoke-root", type=Path)
    args = parser.parse_args()
    print(prepare(args.out, args.source_root, args.large_requests, args.entry_smoke_root))
