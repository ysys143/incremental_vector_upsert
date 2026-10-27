"""Complete isolated history/fresh paths; source provenance is checked before entry."""
from pathlib import Path
import hashlib

import numpy as np

from vectorbench.evidence.contracts import Config, check_space, load, save, sha
from vectorbench.datasets.data import Pool
from vectorbench.workloads.events import Live
from vectorbench.evaluation.measure import metadata_arrays, point, verify_current
from vectorbench.orchestration.isolation import measured
from vectorbench.evaluation.metrics import state_hash
from vectorbench.backends.server import Server
from vectorbench.workloads.writes import wave
from vectorbench.evidence.synthetic_telemetry import UnverifiedTelemetry


def read_point(server, adapter, live, pool, queries, truth, cohorts, out, config, control):
    failures = []
    for attempt in (1, 2):
        folder = out / f"read-{attempt}"
        try:
            result = measured(server, out / f"guard-{attempt}", lambda: point(adapter, live, pool.vectors,
                queries, truth, cohorts, folder, config, control))
            return {**result, "selected_read": str(folder), "read_failures": failures}
        except UnverifiedTelemetry as exc:
            failure = {"status": "UNVERIFIED", "error": str(exc), "attempt": attempt}
            save(folder / "unverified.json", failure)
            failures.append(failure)
            if attempt == 2 or not any(s in str(exc) for s in ("segment set changed", "segment/index changed")):
                raise
    raise AssertionError("unreachable")


def oracle_for(reference, pool, live, query_rows):
    path = Path(reference["path"])
    if sha(path) != reference["sha256"]:
        raise AssertionError("checkpoint oracle bytes changed")
    receipt_path = path.with_suffix(path.suffix + ".receipt.json")
    if reference.get("receipt_sha256") is not None and sha(receipt_path) != reference["receipt_sha256"]:
        raise AssertionError("checkpoint oracle receipt changed")
    receipt = load(receipt_path)
    ids, rows, _ = metadata_arrays(live)
    query_sha = hashlib.sha256(np.asarray(pool.queries[query_rows], dtype="<f4").tobytes()).hexdigest()
    if (receipt["source_sha256"] != pool.base_sha or receipt["source_queries_sha256"] != pool.query_sha
            or receipt["query_sha256"] != query_sha or receipt["metric"] != "ip"
            or receipt["vector_state_sha256"] != state_hash(ids, pool.vectors[rows])):
        raise AssertionError("oracle belongs to another data state/source/metric")
    with np.load(path) as data:
        np.testing.assert_array_equal(data["ids"], ids)
        np.testing.assert_array_equal(data["rows"], rows)
        np.testing.assert_array_equal(data["query_rows"], query_rows)
        return data["truth"].astype(np.int64)


def history(db, pool, initial, events, query_rows, cohorts, controls, tuning, archive, out,
            policy="settled", config=None, quality_steps=None, subset_indices=None, extra_probe=None, server_options=None):
    if policy not in {"settled", "continuous"} or archive is None:
        raise ValueError("history requires an explicit policy and the same frozen initial index")
    c = config or Config()
    check_space(out, c)
    live = Live(initial, len(pool.vectors))
    server = Server(db, out, metric="ip", config=c, **archive, **(server_options or {}))
    points, writes = [], []
    schedule = set(range(len(events)+1) if quality_steps is None else quality_steps)
    if not {0,len(events)} <= schedule or not schedule <= set(range(len(events)+1)):
        raise ValueError('invalid checkpoint schedule')
    subset = np.arange(len(query_rows)) if subset_indices is None else np.asarray(subset_indices,dtype=int)
    if len(set(subset)) != len(subset) or np.any(subset<0) or np.any(subset>=len(query_rows)):
        raise ValueError('invalid fixed query subset')
    extras, verifications = {}, []
    def evaluate(step):
        indexes = np.arange(len(query_rows)) if step == len(events) else subset
        truth = oracle_for(controls[step], pool, live, query_rows)[indexes]
        group = cohorts[str(step)] if isinstance(cohorts, dict) else cohorts
        control = {**controls[step], 'query_subset_indices':indexes.tolist()}
        return read_point(server,adapter,live,pool,np.asarray(pool.queries[np.asarray(query_rows)[indexes]]),truth,
                          np.asarray(group)[indexes].tolist(),out/f'point-{step}',c,control)
    error = None
    try:
        adapter = server.start()
        adapter.set_ef(tuning["ef"])
        first = adapter.state()
        points.append(evaluate(0))
        if extra_probe is not None:
            extras['0'] = extra_probe(server,adapter,live,0,out/'extra-0')
        point_steps = [0]
        for step, event in enumerate(events, 1):
            check_space(out, c)
            # Reconcile the event in a separate in-memory live-set before any DB write.
            expected = Live(live.records(), len(pool.vectors)).apply(event)
            writes.append(wave(adapter, event, pool.vectors, out, step, c))
            live = expected
            if (policy == "settled" or step == len(events)) and step in schedule:
                points.append(evaluate(step))
                point_steps.append(step)
                if extra_probe is not None:
                    extras[str(step)] = extra_probe(server,adapter,live,step,out/f'extra-{step}')
            elif policy == 'settled':
                verifications.append(measured(server,out/f'verify-guard-{step}',lambda:
                    verify_current(adapter,live,pool.vectors,out/f'verify-{step}')))
            if policy == 'settled' or step == len(events):
                state = adapter.state()
                if db == "postgres" and any(first[k] != state[k] for k in ("table_oid", "index_oid")):
                    raise AssertionError("index rebuilt during incremental history")
            # Continuous policy has no intermediate readback/status/query/indexing wait.
        result = {"points": points, "steps": point_steps, "writes": writes, "final_ledger_sha256": live.digest(),
                  "initial_snapshot_sha256": archive["archive_sha"], "policy": policy,
                  "unmeasured_wave_verifications":verifications,"extra_probes":extras}
        save(out / "history.json", result)
        return result
    except BaseException as exc:
        error = exc
        raise
    finally:
        errors = server.cleanup()
        if errors:
            save(out / "cleanup-failure.json", {"errors": errors, "first_error": repr(error)})
            if error is None:
                raise RuntimeError("owned DB cleanup failed: " + repr(errors))


def fresh(db, pool, live, query_rows, cohorts, control, tuning, out, config=None, extra_probe=None, server_options=None):
    c = config or Config()
    check_space(out, c)
    server = Server(db, out, metric="ip", config=c, **(server_options or {}))
    error = None
    try:
        adapter = server.start()
        adapter.batch = tuning["initial_batch"]
        ids, rows, attributes = metadata_arrays(live)
        setup = adapter.setup(ids, pool.vectors[rows], tuning["build"], attributes=attributes)
        adapter.set_ef(tuning["ef"])
        truth = oracle_for(control, pool, live, query_rows)
        result = read_point(server, adapter, live, pool, np.asarray(pool.queries[query_rows]), truth, cohorts,
                            out / "point", c, control)
        extras = extra_probe(server,adapter,live,0,out/'extra') if extra_probe is not None else {}
        return {"point": result, "setup": setup, "state": live.digest(),"extra_probes":extras}
    except BaseException as exc:
        error = exc
        raise
    finally:
        errors = server.cleanup()
        if errors:
            save(out / "cleanup-failure.json", {"errors": errors, "first_error": repr(error)})
            if error is None:
                raise RuntimeError("owned fresh DB cleanup failed: " + repr(errors))
