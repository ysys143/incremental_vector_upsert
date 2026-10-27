"""Quality measurements with verification outside the timed search call."""
import gzip
import hashlib
import json
from pathlib import Path
import time

import numpy as np

from vectorbench.backends.pilot import assert_postgres_ann
from vectorbench.experiments.pilot.model import digest, packed_results, score, validate_snapshot, validate_truth
from vectorbench.experiments.synthetic_insertion.contracts import load, save, sha
from vectorbench.orchestration.synthetic_resources import space_guard
from vectorbench.evidence.synthetic_telemetry import UnverifiedTelemetry, segments, verify_delta


def telemetry(adapter):
    return adapter.request("GET", "/telemetry?details_level=4&anonymize=false")


def query(adapter, vector, guards=None):
    if adapter.name == "postgres":
        statement = adapter.search_sql(vector)
        raw = adapter.sql("SHOW enable_seqscan;\nEXPLAIN (FORMAT JSON) " + statement)
        setting, plan = raw.split("\n", 1)
        plan = json.loads(plan)
        assert_postgres_ann(plan, setting, f"{adapter.table}_hnsw")
        if guards is not None:
            guards.append({"setting": setting, "sql_sha256": hashlib.sha256(statement.encode()).hexdigest(), "plan": plan})
        start = time.perf_counter()
        rows = json.loads(adapter.sql(statement))
    else:
        start = time.perf_counter()
        rows = adapter.query(vector)
    return rows, (time.perf_counter() - start) * 1000


def read_measurement(adapter, ids, vectors, queries, truth, cohorts, out, campaign_root):
    out.mkdir(parents=True, exist_ok=False)
    space_guard(campaign_root)
    wait = adapter.wait()
    save(out / "wait.json", wait)
    if not wait["settled"]:
        raise UnverifiedTelemetry("ANN ready deadline exceeded")
    actual = adapter.snapshot()
    validate_snapshot(ids, vectors, *actual)
    state = adapter.state()
    save(out / "state.json", state)
    path = adapter.path(queries[0])
    save(out / "path.json", path)
    validate_truth(ids, vectors, queries, truth)
    for vector in queries:
        query(adapter, vector)
    before = None
    if adapter.name == "qdrant":
        before = telemetry(adapter)
        save(out / "telemetry-before.json", before)
        segments(before)
    rows, ms, guards = [], [], []
    for i, vector in enumerate(queries):
        if i % 100 == 0:
            space_guard(campaign_root)
        returned, latency = query(adapter, vector, guards)
        rows.append(returned)
        ms.append(latency)
    if adapter.name == "qdrant":
        after = telemetry(adapter)
        save(out / "telemetry-after.json", after)
        ann = verify_delta(before, after, len(queries))
    else:
        ann = {"status": "PASS", "verified_queries": len(guards), "seqscan": "off",
               "actual_plan_after": adapter.path(queries[0])}
        with gzip.open(out / "query-plans.json.gz", "wt") as stream:
            json.dump(guards, stream)
    quality = score(ids, truth, rows)
    groups = {}
    for group, label in enumerate(("old", "new", "boundary")):
        chosen = np.flatnonzero(cohorts == group)
        if len(chosen):
            groups[label] = score(ids, truth[chosen], [rows[i] for i in chosen])
    np.savez_compressed(out / "raw.npz", truth=truth, ids=ids, latency_ms=ms,
                        cohort=cohorts, **packed_results(rows))
    receipt = {"status": "PASS", "count": len(ids), "queries": len(queries),
               "data_hash": digest(ids, vectors), "query_hash": hashlib.sha256(queries.tobytes()).hexdigest(),
               "raw_sha256": sha(out / "raw.npz"), "groups": groups, **quality,
               "p50_ms": float(np.percentile(ms, 50)), "p95_ms": float(np.percentile(ms, 95)),
               "p99_ms": float(np.percentile(ms, 99)), "ann": ann,
               "timing": "search call only; plan/telemetry/oracle excluded; shared-host INDICATIVE"}
    save(out / "receipt.json", receipt)
    return receipt


def measure_point(adapter, fixture, metadata, state_hash, root, out):
    state_info = metadata["states"][state_hash]
    if sha(root / state_info["path"]) != state_info["sha256"]:
        raise AssertionError("oracle artifact changed")
    with np.load(root / state_info["path"]) as data:
        ids, truth = data["ids"], data["truth"]
    if digest(ids, fixture["pool"][ids]) != state_hash:
        raise AssertionError("oracle state differs from actual vector set")
    attempts = []
    for attempt in (1, 2):
        folder = out / f"read-attempt-{attempt}"
        try:
            receipt = read_measurement(adapter, ids, fixture["pool"][ids], fixture["queries"], truth,
                                       fixture["cohort"], folder, root)
            receipt["read_attempts"] = attempts
            receipt["relative_path"] = str(folder.relative_to(root))
            return receipt
        except UnverifiedTelemetry as exc:
            failure = {"status": "UNVERIFIED", "error": str(exc), "attempt": attempt}
            save(folder / "unverified.json", failure)
            attempts.append(failure)
            # The approved read-only retry is only for a changing segment set/index,
            # never for a plain fallback, missing counters, or writes.
            if attempt == 2 or not any(x in str(exc) for x in ("segment set changed", "segment/index changed")):
                raise
    raise AssertionError("unreachable")
