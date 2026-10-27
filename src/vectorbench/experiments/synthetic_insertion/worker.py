"""One substantive experiment cell, invoked by a Dagster asset in the original runtime."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
import traceback

import numpy as np

from vectorbench.experiments.pilot.model import digest, validate_snapshot
from vectorbench.experiments.pilot.run import docker
from vectorbench.experiments.synthetic_insertion.contracts import (DATABASES, GEOMETRIES, canonical, effective_sources, load, save, sha,
                        source_revision, verify_files)
from vectorbench.experiments.synthetic_insertion.data import nearest, waves_for
from vectorbench.evaluation.synthetic_measure import measure_point, read_measurement, telemetry
from vectorbench.orchestration.synthetic_resources import OwnedServer, space_guard
from vectorbench.evidence.synthetic_telemetry import segments, verify_delta, UnverifiedTelemetry


def acknowledged(root, key):
    ledger = load(root / "ledger.json")
    entry = ledger[key]
    if entry["status"] != "PASS":
        raise AssertionError("dependency not acknowledged: " + key)
    ack = load(root / entry["ack"])
    receipt_path = root / ack["receipt"]
    if sha(receipt_path) != ack["receipt_sha256"]:
        raise AssertionError("acknowledged receipt changed: " + key)
    receipt = load(receipt_path)
    if receipt["campaign"] != root.name or receipt["key"] != key or ack["run_id"] != entry["run_id"]:
        raise AssertionError("receipt scope/run identity mismatch")
    verify_files(root, receipt)
    return receipt


def fixture(root, key):
    folder = root / "inputs" / key
    with np.load(folder / "fixture.npz") as stream:
        data = {name: stream[name] for name in stream.files}
    return data, load(folder / "metadata.json")


def chosen(root, db):
    return acknowledged(root, "tune:" + db)["result"]["chosen"]


def owner_run(db, out, action, archive=None):
    server = OwnedServer(db, out, **(archive or {}))
    error = None
    try:
        adapter = server.start()
        return action(server, adapter)
    except BaseException as exc:
        error = exc
        raise
    finally:
        errors = server.cleanup()
        if errors and error is None:
            raise RuntimeError("owned DB cleanup failed: " + repr(errors))


def verify_fixture(root, spec, out):
    for path, expected in spec["files"].items():
        if sha(root / path) != expected:
            raise AssertionError("prepared input changed: " + path)
    f, m = fixture(root, spec["fixture"])
    if m["query_hash"] != hashlib.sha256(f["queries"].tobytes()).hexdigest():
        raise AssertionError("query data changed")
    return {"fixture": spec["fixture"], "files": spec["files"], "states": len(m["states"])}


def tune(root, spec, out, manifest):
    c = manifest["config"]
    states = []
    iid, im = fixture(root, f"s{c['calibration_seed']}-iid")
    initial_queries = iid["queries"][:min(200, c["queries_per_cohort"])]
    states.append(("initial", iid["pool"][:c["n"]], np.arange(c["n"]), initial_queries,
                   np.zeros(len(initial_queries), dtype=np.int64)))
    for geo in GEOMETRIES:
        f, m = fixture(root, f"s{c['calibration_seed']}-{geo}")
        count = min(200, len(f["queries"]))
        select = np.linspace(0, len(f["queries"]) - 1, count, dtype=np.int64)
        states.append((geo, f["pool"], np.arange(2*c["n"]), f["queries"][select], f["cohort"][select]))
    candidates = []

    def action(server, adapter):
        probes = []
        for batch in c["initial_batch_candidates"]:
            adapter.batch = batch
            setup = adapter.setup(np.arange(c["n"]), iid["pool"][:c["n"]], c["builds"][0])
            validate_snapshot(np.arange(c["n"]), iid["pool"][:c["n"]], *adapter.snapshot())
            probes.append({"batch": batch, "setup": setup})
            save(out / "initial-batch-probes.json", probes)
        initial_batch = min(probes, key=lambda p: p["setup"]["load_s"])["batch"]
        for ef_values in (c["search_ef"], [c["conditional_search_ef"]]):
            for build in c["builds"]:
                state_results = {ef: [] for ef in ef_values}
                for label, vectors, ids, queries, cohorts in states:
                    space_guard(root)
                    adapter.batch = initial_batch
                    setup = adapter.setup(ids, vectors, build)
                    truth = nearest(ids, vectors, queries)
                    for ef in ef_values:
                        adapter.set_ef(ef)
                        folder = out / f"m{build[0]}-ef{ef}-{label}"
                        point = read_measurement(adapter, ids, vectors, queries, truth, cohorts, folder, root)
                        state_results[ef].append({"state": label, "setup": setup,
                                                 "recall10": point["recall10"], "recall100": point["recall100"],
                                                 "p50_ms": point["p50_ms"]})
                for ef, rows in state_results.items():
                    candidates.append({"build": build, "ef": ef, "states": rows,
                        "passed": all(r["recall10"] >= .95 and r["recall100"] >= .95 for r in rows),
                        "mean_p50_ms": float(np.mean([r["p50_ms"] for r in rows]))})
                    save(out / "candidates.json", candidates)
            passing = [p for p in candidates if p["passed"]]
            if passing:
                best = min(passing, key=lambda p: p["mean_p50_ms"])
                result = {"build": best["build"], "ef": best["ef"], "initial_batch": initial_batch,
                          "calibration_seed": c["calibration_seed"], "quality_target": .95,
                          "calibration_states": best["states"]}
                save(out / "chosen.json", result)
                return {"chosen": result, "candidates": len(candidates)}
        raise AssertionError("QUALITY_TARGET_UNMET: no candidate meets both targets in all calibration states")
    return owner_run(spec["db"], out, action)


def initial_snapshot(root, spec, out, manifest):
    choice = chosen(root, spec["db"])
    f, m = fixture(root, spec["fixture"])
    n = manifest["config"]["n"]

    def action(server, adapter):
        adapter.batch = choice["initial_batch"]
        setup = adapter.setup(np.arange(n), f["pool"][:n], choice["build"])
        adapter.set_ef(choice["ef"])
        validate_snapshot(np.arange(n), f["pool"][:n], *adapter.snapshot())
        adapter.path(f["queries"][0])
        snapshot = server.export(out / "initial.tgz")
        snapshot["archive"] = str((out / "initial.tgz").relative_to(root))
        snapshot["base_data_hash"] = m["base_state"]
        save(out / "snapshot.json", snapshot)
        return {"snapshot": snapshot, "setup": setup, "chosen": choice}
    return owner_run(spec["db"], out, action)


def incremental(root, spec, out, manifest):
    c = manifest["config"]
    f, m = fixture(root, spec["fixture"])
    base = acknowledged(root, "base:" + spec["base"])["result"]
    snapshot = base["snapshot"]
    choice = base["chosen"]
    archive = {"archive": root / snapshot["archive"], "archive_sha": snapshot["archive_sha"],
               "files_hash": snapshot["files_hash"], "segment_signature": snapshot.get("segment_signature"),
               "graph_hash": snapshot.get("graph_hash")}
    waves = waves_for(spec, c, f)
    if [digest(w, f["pool"][w]) for w in waves] != spec["wave_hashes"]:
        raise AssertionError("wave order/input changed after catalogue freeze")
    points, writes = [], []

    def action(server, adapter):
        adapter.set_ef(choice["ef"])
        n = c["n"]
        validate_snapshot(np.arange(n), f["pool"][:n], *adapter.snapshot())
        initial_state = adapter.state()
        if adapter.name == "postgres":
            if any(initial_state[k] != snapshot["initial_state"][k] for k in ("table_oid", "index_oid")):
                raise AssertionError("initial index OID differs from the frozen initial index")
        points.append(measure_point(adapter, f, m, spec["states"][0], root, out / "point-0"))
        for step, wave in enumerate(waves, 1):
            space_guard(root)
            intent = {"step": step, "count": len(wave), "hash": digest(wave, f["pool"][wave]),
                      "status": "INTENT", "expected_requests": 1, "requests": []}
            save(out / f"write-{step}.json", intent)
            if adapter.name == "postgres":
                original = adapter.sql

                def traced(sql):
                    if sql.startswith("COPY "):
                        intent["requests"].append({"kind": "COPY", "bytes": len(sql.encode()), "rows": len(sql.splitlines()) - 2})
                    return original(sql)
                adapter.sql = traced
            else:
                original = adapter.request

                def traced(method, path, body=None):
                    if method == "PUT" and "/points?" in path:
                        encoded = json.dumps(body, separators=(",", ":")).encode()
                        if len(encoded) > 30 * 1024**2:
                            raise AssertionError("wave payload exceeds the declared 30MiB preflight limit")
                        intent["requests"].append({"kind": "batch-upsert", "bytes": len(encoded),
                            "rows": len(body["batch"]["ids"]), "wait": "wait=true" in path})
                    return original(method, path, body)
                adapter.request = traced
            adapter.batch = len(wave)
            start = time.perf_counter()
            try:
                adapter.insert(wave, f["pool"][wave])
            finally:
                if adapter.name == "postgres":
                    adapter.sql = original
                else:
                    adapter.request = original
            intent.update(status="ACKNOWLEDGED", ack_s=time.perf_counter() - start)
            save(out / f"write-{step}.json", intent)
            if len(intent["requests"]) != 1 or intent["requests"][0]["rows"] != len(wave):
                raise AssertionError("hidden chunking or incorrect wave request size")
            writes.append(intent)
            if spec["policy"] == "settled" or step == len(waves):
                point = measure_point(adapter, f, m, spec["states"][step], root, out / f"point-{step}")
                points.append(point)
                if adapter.name == "postgres":
                    state = adapter.state()
                    if any(state[k] != initial_state[k] for k in ("table_oid", "index_oid")):
                        raise AssertionError("index rebuilt during incremental history")
                save(out / "points.json", points)
            # Continuous mode deliberately performs no metadata/readback/query/wait here.
        save(out / "resources-final.json", server.resources())
        return {"points": points, "writes": writes, "final_data_hash": points[-1]["data_hash"],
                "initial_snapshot_sha": snapshot["archive_sha"], "spec": spec}
    return owner_run(spec["db"], out, action, archive)


def fresh(root, spec, out, manifest):
    f, m = fixture(root, spec["fixture"])
    choice = chosen(root, spec["db"])
    state = m["states"][spec["state"]]
    with np.load(root / state["path"]) as data:
        ids = data["ids"]

    def action(server, adapter):
        adapter.batch = choice["initial_batch"]
        setup = adapter.setup(ids, f["pool"][ids], choice["build"])
        adapter.set_ef(choice["ef"])
        point = measure_point(adapter, f, m, spec["state"], root, out / "point")
        save(out / "resources-final.json", server.resources())
        return {"points": [point], "setup": setup, "spec": spec}
    return owner_run(spec["db"], out, action)


def execute(root, key, out, run_id):
    manifest = load(root / "manifest.json")
    sources = effective_sources(root, manifest)
    revision = source_revision(sources)
    if load(out / "source-admission.json") != {"revision": revision, "run_id": run_id}:
        raise AssertionError("worker source/run admission differs")
    for name, expected in sources.items():
        if sha(name) != expected:
            raise AssertionError("worker sources changed after admission: " + name)
    spec = manifest["cells"][key]
    for dep in spec["deps"]:
        acknowledged(root, dep)
    space_guard(root)
    started = time.time()
    if spec["kind"] == "fixture":
        result = verify_fixture(root, spec, out)
    elif spec["kind"] == "tune":
        result = tune(root, spec, out, manifest)
    elif spec["kind"] == "base":
        result = initial_snapshot(root, spec, out, manifest)
    elif spec["kind"] == "incremental":
        result = incremental(root, spec, out, manifest)
    elif spec["kind"] == "fresh":
        result = fresh(root, spec, out, manifest)
    elif spec["kind"] == "summary":
        from vectorbench.reporting.synthetic_results import summarize
        result = summarize(root, out, manifest)
    else:
        raise ValueError(spec["kind"])
    files = {str(p.relative_to(root)): sha(p) for p in out.rglob("*") if p.is_file()
             and p.name not in {"resources.json", "active-worker.json", "stdout.log", "stderr.log", "supervision.json"}}
    if spec["kind"] == "fixture":
        files.update(spec["files"])
    receipt = {"status": "PASS", "campaign": root.name, "key": key, "run_id": run_id,
               "source_revision": revision,
               "elapsed_s": time.time() - started, "result": result, "artifacts": files}
    save(out / "receipt.json", receipt)
    print(canonical({"key": key, "status": "PASS", "elapsed_s": receipt["elapsed_s"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("key")
    parser.add_argument("out", type=Path)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    try:
        execute(args.root, args.key, args.out, args.run_id)
    except BaseException as exc:
        save(args.out / "failure.json", {"status": "FAIL", "first_error": repr(exc), "traceback": traceback.format_exc()})
        raise
