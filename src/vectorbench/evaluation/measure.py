"""Metric-aware evidence with timing separated from plan/oracle/readback checks."""
import gzip
import hashlib
import json
from pathlib import Path
import time

import numpy as np

from vectorbench.experiments.pilot.model import score, packed_results
from vectorbench.evidence.synthetic_telemetry import segments, verify_delta
from vectorbench.backends.adapters import assert_ann
from vectorbench.evidence.contracts import check_space, save, sha
from vectorbench.evaluation.metrics import direct_exact, state_hash


def metadata_arrays(live):
    records = live.records()
    return (np.array([w.id for w in records], dtype=np.int64),
            np.array([w.row for w in records], dtype=np.int64),
            np.array([[w.category, w.ordinal, w.version] for w in records], dtype=np.int64))


def verify_snapshot(ids, vectors, actual_ids, actual_vectors):
    """Original float32 values must round-trip exactly; no loose embedding tolerance."""
    np.testing.assert_array_equal(np.asarray(actual_ids, dtype=np.int64), ids)
    np.testing.assert_array_equal(np.asarray(actual_vectors, dtype=np.float32), vectors)


def verify_current(adapter, live, pool, out):
    """Settled state verification also runs on waves without a search checkpoint."""
    started = time.time()
    ids, rows, attrs = metadata_arrays(live)
    wait = adapter.wait()
    if not wait['settled']:
        raise AssertionError('indexing readiness deadline exceeded')
    verify_snapshot(ids, pool[rows], *adapter.snapshot())
    expected = [{'id':int(i),'category':int(a[0]),'ordinal':int(a[1]),'version':int(a[2])} for i,a in zip(ids,attrs)]
    if adapter.metadata() != expected:
        raise AssertionError('current metadata/version differs from event ledger')
    result = {'status':'PASS','started_at':started,'finished_at':time.time(),
              'ledger_sha256':live.digest(),'wait':wait,'state':adapter.state()}
    save(out / 'state-verification.json', result)
    return result


def query(adapter, vector, guards):
    if adapter.name == "postgres":
        statement = adapter.search_sql(vector)
        raw = adapter.sql("SHOW enable_seqscan;\nEXPLAIN (FORMAT JSON) " + statement)
        setting, plan = raw.split("\n", 1)
        assert_ann(json.loads(plan), setting, adapter.table + "_hnsw", adapter.metric)
        guards.append({"setting": setting, "sql_sha256": hashlib.sha256(statement.encode()).hexdigest(), "plan": json.loads(plan)})
        started = time.perf_counter()
        result = json.loads(adapter.sql(statement))
    else:
        started = time.perf_counter()
        result = adapter.query(vector)
    return result, (time.perf_counter() - started) * 1000


def point(adapter, live, pool, queries, truth, cohorts, out, config=None, oracle_ref=None):
    """One settled unfiltered checkpoint. Filtered telemetry has its own qualification gate."""
    if adapter.predicate is not None:
        raise NotImplementedError("filtered measurements require independently qualified filtered ANN telemetry")
    out = Path(out)
    started_wall = time.time()
    check_space(out.parent, config)
    out.mkdir(parents=True, exist_ok=False)
    ids, rows, attrs = metadata_arrays(live)
    vectors = pool[rows]
    if adapter.metric != "ip":
        raise AssertionError("public real-study datasets require IP")
    wait = adapter.wait()
    save(out / "wait.json", wait)
    if not wait["settled"]:
        raise AssertionError("indexing readiness deadline exceeded")
    verify_snapshot(ids, vectors, *adapter.snapshot())
    expected_metadata = [{"id": int(i), "category": int(a[0]), "ordinal": int(a[1]), "version": int(a[2])} for i, a in zip(ids, attrs)]
    if adapter.metadata() != expected_metadata:
        raise AssertionError("current metadata/version differs from the event ledger")
    state = adapter.state()
    save(out / "state.json", state)
    if adapter.name == "qdrant" and state["config"]["params"]["vectors"]["distance"] != "Dot":
        raise AssertionError("Qdrant metric is not Dot")
    path = adapter.path(queries[0])
    save(out / "path.json", path)
    np.testing.assert_array_equal(truth[:8], direct_exact(ids, vectors, queries[:8], k=truth.shape[1], metric=adapter.metric))
    for vector in queries:
        query(adapter, vector, [])
    before = None
    if adapter.name == "qdrant":
        before = adapter.request("GET", "/telemetry?details_level=4&anonymize=false")
        save(out / "telemetry-before.json", before)
        segments(before)
    returned, latency, guards = [], [], []
    for index, vector in enumerate(queries):
        if index % 100 == 0:
            check_space(out, config)
        result, ms = query(adapter, vector, guards)
        returned.append(result); latency.append(ms)
    if adapter.name == "qdrant":
        after = adapter.request("GET", "/telemetry?details_level=4&anonymize=false")
        save(out / "telemetry-after.json", after)
        ann = verify_delta(before, after, len(queries))
    else:
        analyzed = adapter.path(queries[0])
        ann = {"status": "PASS", "seqscan": "off", "verified_queries": len(guards), "actual_plan_after": analyzed}
        with gzip.open(out / "query-plans.json.gz", "wt") as stream:
            json.dump(guards, stream)
    quality = score(ids, truth, returned)
    groups = {}
    for name in sorted(set(cohorts)):
        selected = np.flatnonzero(np.asarray(cohorts) == name)
        groups[str(name)] = score(ids, truth[selected], [returned[i] for i in selected])
    packed = packed_results(returned)
    # Study IDs fit int32. Preserve every returned ID and short-response length losslessly.
    if any(i > np.iinfo(np.int32).max for result in returned for i in result):
        raise AssertionError("compact response encoding would truncate an ID")
    np.savez_compressed(out / "raw.npz", latency_ms=latency,
                        returned=packed["returned"].astype(np.int32), returned_lengths=packed["returned_lengths"].astype(np.int16))
    receipt = {"status": "PASS", "metric": adapter.metric, "count": len(ids), "queries": len(queries),
               "started_at": started_wall, "finished_at": time.time(),
               "vector_state_sha256": state_hash(ids, vectors), "ledger_sha256": live.digest(),
               "query_sha256": hashlib.sha256(np.asarray(queries, dtype="<f4").tobytes()).hexdigest(),
               "oracle": oracle_ref, "raw_sha256": sha(out / "raw.npz"), "groups": groups,
               **quality, "p50_ms": float(np.percentile(latency, 50)),
               "p95_ms": float(np.percentile(latency, 95)), "p99_ms": float(np.percentile(latency, 99)),
               "ann": ann, "timing": "search only; checks/warmup excluded; shared host INDICATIVE"}
    save(out / "receipt.json", receipt)
    return receipt
