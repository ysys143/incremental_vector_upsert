"""Immutable input preparation and G1 validation for the clustered insertion study."""
from pathlib import Path
import hashlib

import numpy as np

from vectorbench.datasets.storage import load_events, load_state, save_events
from vectorbench.evidence.checkpoints import Checkpoints
from vectorbench.evidence.contracts import Config, load, save, sha
from vectorbench.evaluation.metrics import direct_exact, state_hash
from vectorbench.experiments.real_vectors.baseline.baseline_data import split
from vectorbench.workloads.events import Event, Live, Write, initial
from .clustering import cluster


def _digest(array):
    return hashlib.sha256(np.asarray(array).tobytes()).hexdigest()


def _sample_diagnostics(pool, old, new, fitted, seed):
    """Sample topology diagnostics; these are not DB graph edges."""
    rng = np.random.default_rng(np.random.SeedSequence([int(seed), 889]))
    oi = rng.choice(len(old), min(512, len(old)), replace=False)
    ni = rng.choice(len(new), min(256, len(new)), replace=False)
    old_x = np.asarray(pool.vectors[old[oi]], dtype=np.float64)
    new_x = np.asarray(pool.vectors[new[ni]], dtype=np.float64)
    candidates = np.concatenate((old_x, new_x))
    labels = np.asarray(fitted["labels"])[ni]
    cross_ip, same_cluster, normalized_distance = [], [], []
    norms = np.linalg.norm(candidates, axis=1)
    unit = candidates / norms[:, None]
    for j, query in enumerate(new_x):
        ip = candidates @ query
        ip[len(old_x) + j] = -np.inf
        ranks = np.lexsort((np.arange(len(ip)), -ip))[:min(20, len(ip)-1)]
        cross_ip.append(float(np.mean(ranks < len(old_x))))
        new_neighbors = ranks[ranks >= len(old_x)] - len(old_x)
        same_cluster.append(float(np.mean(labels[new_neighbors] == labels[j])) if len(new_neighbors) else None)
        cosine = unit @ unit[len(old_x)+j]
        cosine[len(old_x)+j] = -np.inf
        normalized_distance.append(float(1.0 - np.max(cosine)))
    norms_old = np.linalg.norm(old_x, axis=1)
    norms_new = np.linalg.norm(new_x, axis=1)
    return {"anchor_old_count": len(old_x), "anchor_new_count": len(new_x),
            "old_neighbor_fraction_in_new_top20_ip_mean": float(np.mean(cross_ip)),
            "same_cluster_fraction_among_new_top20_ip_mean": float(np.mean([v for v in same_cluster if v is not None])) if any(v is not None for v in same_cluster) else None,
            "normalized_nearest_cosine_distance_mean": float(np.mean(normalized_distance)),
            "old_norm_min_median_max": [float(norms_old.min()), float(np.median(norms_old)), float(norms_old.max())],
            "new_norm_min_median_max": [float(norms_new.min()), float(np.median(norms_new)), float(norms_new.max())],
            "sample_scope": "fixed 512 old / 256 new anchors; sampled original IP and normalized cosine, not HNSW edges"}


def _cohorts(truth, n, labels):
    top = np.asarray(truth[:, :10], dtype=np.int64)
    fraction = np.mean(top >= n, axis=1)
    origin = np.full(len(top), 2, dtype=np.uint8)
    origin[fraction <= .2] = 0
    origin[fraction >= .8] = 1
    dominant = np.full(len(top), -1, dtype=np.int8)
    mixed = np.zeros(len(top), dtype=np.uint8)
    for i, row in enumerate(top):
        new = row[row >= n]
        if len(new):
            counts = np.bincount(labels[new - n], minlength=5)
            dominant[i] = int(np.argmax(counts))  # tie: smaller canonical ID
            mixed[i] = int(np.max(counts) / len(new) < .6)
    counts = {"old": int(np.sum(origin == 0)), "new": int(np.sum(origin == 1)),
              "boundary": int(np.sum(origin == 2)), "no_new_neighbor": int(np.sum(dominant < 0)),
              "mixed": int(np.sum(mixed)),
              **{f"dominant_cluster_{i}": int(np.sum(dominant == i)) for i in range(5)}}
    return {"origin": origin, "dominant_cluster": dominant, "mixed": mixed,
            "new_fraction": fraction.astype(np.float32)}, counts


def _state_checks(pool, point, query_rows):
    state = load_state(point["state"]["path"], len(pool.vectors))
    if state.digest() != point["state"]["ledger_sha256"] or sha(point["state"]["path"]) != point["state"]["sha256"]:
        raise AssertionError("state ledger or bytes changed")
    control = point["control"]
    receipt_path = Path(control["path"]).with_suffix(".npz.receipt.json")
    receipt = load(receipt_path)
    if sha(control["path"]) != control["sha256"] or sha(receipt_path) != control["receipt_sha256"]:
        raise AssertionError("oracle bytes or receipt changed")
    if receipt["ledger_sha256"] != state.digest() or receipt["source_sha256"] != pool.base_sha or receipt["source_queries_sha256"] != pool.query_sha:
        raise AssertionError("oracle is bound to another state or source")
    with np.load(control["path"]) as arrays:
        truth, ids, rows, qr = (arrays[k].copy() for k in ("truth", "ids", "rows", "query_rows"))
    records = state.records()
    expected_ids = np.asarray([w.id for w in records])
    expected_rows = np.asarray([w.row for w in records])
    if not np.array_equal(ids, expected_ids) or not np.array_equal(rows, expected_rows) or not np.array_equal(qr, query_rows):
        raise AssertionError("oracle ID/source-row/query mapping mismatch")
    if receipt["vector_state_sha256"] != state_hash(ids, pool.vectors[rows]):
        raise AssertionError("oracle is bound to different current vector bytes")
    query_bytes = np.asarray(pool.queries[query_rows], dtype="<f4").tobytes()
    if receipt["query_sha256"] != hashlib.sha256(query_bytes).hexdigest():
        raise AssertionError("oracle is bound to different query bytes or order")
    if len(truth) != len(query_rows) or len(ids) != receipt["count"] or truth.shape[1] != min(100, len(ids)):
        raise AssertionError("oracle result dimensions mismatch")
    if any(len(set(map(int, row))) != len(row) or not set(map(int, row)).issubset(state.current) for row in truth):
        raise AssertionError("duplicate or missing current result ID")
    spot = np.unique(np.linspace(0, len(query_rows)-1, min(8, len(query_rows)), dtype=int))
    scalar = direct_exact(ids, pool.vectors[rows], pool.queries[np.asarray(query_rows)[spot]], metric="ip")
    np.testing.assert_array_equal(truth[spot], scalar)
    return truth


def prepare(pool, out, c=None):
    """Prepare both exact-query families for seed 7 and three evaluation seeds.

    ``pool`` must come from validated Pool.open; this function never writes source files.
    The output directory must not exist, preventing evidence overwrite on resume.
    """
    c = c or Config()
    c.validate()
    if tuple(c.seeds) != (11, 29, 47) or c.calibration_seed != 7 or c.query_rows != 600 or c.calibration_queries != 200:
        raise ValueError("clustered study frozen seeds and query sizes changed")
    if pool.dataset not in {"msmarco", "wikipedia"} or pool.vectors.dtype != np.float32 or pool.queries.dtype != np.float32:
        raise ValueError("expected original public float32 pool")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    selected = split(pool, c)
    qcal = np.asarray(selected["calibration_queries"], dtype=np.int64)
    qeval = np.asarray(selected["evaluation_queries"], dtype=np.int64)
    if len(qcal) != 200 or len(qeval) != 600 or len(set(qcal).intersection(qeval)):
        raise AssertionError("query holdout mismatch")
    query_path = out / "query-rows.npz"
    np.savez_compressed(query_path, calibration=qcal, evaluation=qeval)
    cache = {"calibration": Checkpoints(pool, qcal, out / "calibration-checkpoints", c),
             "evaluation": Checkpoints(pool, qeval, out / "evaluation-checkpoints", c)}
    seeds, row_sets = {}, {}
    for seed in (7, *c.seeds):
        if seed == 7:
            document_rows = np.asarray(selected["calibration_documents"], dtype=np.int64)
            primary = "calibration"
        else:
            document_rows = np.random.default_rng(seed).permutation(selected["evaluation_documents"])[:2*c.n]
            primary = "evaluation"
        if len(document_rows) != 2*c.n or len(np.unique(document_rows)) != 2*c.n:
            raise AssertionError("D0/A document split incomplete")
        old, incoming = document_rows[:c.n], document_rows[c.n:]
        row_sets[str(seed)] = document_rows
        original_bytes = _digest(pool.vectors[document_rows])
        source_rows = np.asarray(pool.original_rows[incoming], dtype=np.int64)
        fit = cluster(np.asarray(pool.vectors[incoming], dtype=np.float32), source_rows, seed)
        seed_dir = out / f"seed-{seed}"
        seed_dir.mkdir()
        cluster_path = seed_dir / "clusters.npz"
        np.savez_compressed(cluster_path, source_rows=source_rows, labels=fit["labels"],
                            cluster_order=fit["cluster_order"], random_order=fit["random_order"],
                            clustered_order=fit["clustered_order"], centers=fit["centers"])
        start = initial(old)
        writes = [Write(c.n+i, int(row), 0, int(fit["labels"][i]), int(pool.original_rows[row]))
                  for i, row in enumerate(incoming)]
        live_initial = Live(start, len(pool.vectors))
        start_points = {name: checkpoint.get(live_initial) for name, checkpoint in cache.items()}
        paths = {}
        sizes = fit["wave_sizes"]
        for name, order in (("M1", fit["random_order"]), ("M5", fit["random_order"]),
                            ("C1", fit["clustered_order"]), ("C5", fit["clustered_order"])):
            chunks = [order] if name.endswith("1") else np.split(order, np.cumsum(sizes)[:-1])
            events = [Event(writes=tuple(writes[int(i)] for i in chunk), operation="insert") for chunk in chunks]
            path_dir = seed_dir / name
            path_dir.mkdir()
            events_path = path_dir / "events.npz"
            event_receipt = save_events(events_path, events)
            current = Live(start, len(pool.vectors))
            controls, states = [], {}
            by_query = {qname: [] for qname in cache}
            for step in range(len(events)+1):
                if step:
                    current.apply(events[step-1])
                points = {qname: checkpoint.get(current) for qname, checkpoint in cache.items()}
                controls.append(points[primary]["control"])
                states[str(step)] = points[primary]["state"]
                for qname in cache:
                    by_query[qname].append(points[qname]["control"])
            if len(current.current) != 2*c.n:
                raise AssertionError("final current count mismatch")
            paths[name] = {"events": str(events_path), "events_sha256": event_receipt["sha256"],
                           "controls": controls, "controls_by_query_set": by_query,
                           "states": states, "observed_steps": list(range(len(events)+1)),
                           "counts": {"waves": len(events), "writes": c.n, "deletes": 0,
                                      "final_count": 2*c.n, "wave_sizes": [len(e.writes) for e in events]},
                           "final_ledger_sha256": current.digest(),
                           "scope": "PREPARED_INPUT_ONLY; DB NOT RUN"}
        final_control = paths["M1"]["controls"][-1]
        with np.load(final_control["path"]) as arrays:
            final_truth = arrays["truth"].copy()
        cohorts, cohort_counts = _cohorts(final_truth, c.n, fit["labels"])
        cohort_path = seed_dir / "cohorts.npz"
        np.savez_compressed(cohort_path, **cohorts)
        for path in paths.values():
            path["cohort_codes"] = str(cohort_path)
            path["cohort_sha256"] = sha(cohort_path)
        if original_bytes != _digest(pool.vectors[document_rows]):
            raise AssertionError("original source vector bytes changed during clustering")
        cohort_labels = np.asarray(["old", "new", "boundary"], dtype=object)[cohorts["origin"]].tolist()
        seeds[str(seed)] = {"query_rows": primary, "initial": start_points[primary]["state"]["path"],
                            "initial_control": start_points[primary]["control"],
                            "initial_controls_by_query_set": {name: point["control"] for name, point in start_points.items()},
                            "paths": paths, "cluster_labels": str(cluster_path), "cluster_sha256": sha(cluster_path),
                            "wave_sizes": sizes, "cohort_codes": str(cohort_path), "cohort_labels": cohort_labels,
                            "cohort_counts": cohort_counts, "input_vector_sha256": original_bytes,
                            "diagnostics": {**fit["diagnostics"], **_sample_diagnostics(pool, old, incoming, fit, seed)}}
    overlaps = {f"{a}-{b}": {"document_count": len(set(map(int, row_sets[a])) & set(map(int, row_sets[b]))),
                             "document_fraction": len(set(map(int, row_sets[a])) & set(map(int, row_sets[b]))) / (2*c.n)}
                for i, a in enumerate(row_sets) for b in list(row_sets)[i+1:]}
    if any(overlaps[k]["document_count"] for k in overlaps if k.startswith("7-")):
        raise AssertionError("calibration documents overlap evaluation documents")
    result = {"dataset": pool.dataset, "pool_root": str(Path(getattr(pool.vectors, "filename", "")).parent) if getattr(pool.vectors, "filename", None) else None,
              "source": {"base_sha256": pool.base_sha, "query_sha256": pool.query_sha},
              "query_rows": str(query_path), "query_rows_sha256": sha(query_path),
              "query_sets": {"calibration": 200, "evaluation": 600},
              "seeds": seeds, "holdout": selected["diagnostics"], "sample_overlap": overlaps,
              "input_bindings": {"base_sha256": pool.base_sha, "query_sha256": pool.query_sha,
                                 "query_rows_sha256": sha(query_path),
                                 "original_row_map_sha256": _digest(pool.original_rows),
                                 "original_query_row_map_sha256": _digest(pool.original_query_rows)},
              "unique_ledgers": len({key for cp in cache.values() for key in cp.states}),
              "unique_oracles": sum(len(cp.oracles) for cp in cache.values()),
              "scope": "PREPARED_INPUT_ONLY; DB NOT RUN"}
    validate_prepared(result, pool, c)
    save(out / "prepared-inputs.json", result)
    return result


def validate_prepared(prepared, pool, c=None):
    """G1 binding and replay check; rejects tampered IDs, bytes, query order or oracle refs."""
    c = c or Config()
    if pool.base_sha != prepared["input_bindings"]["base_sha256"] or pool.query_sha != prepared["input_bindings"]["query_sha256"]:
        raise AssertionError("source identity changed")
    for array, expected in ((pool.vectors, pool.base_sha), (pool.queries, pool.query_sha)):
        filename = getattr(array, "filename", None)
        if filename is not None and sha(filename) != expected:
            raise AssertionError("original public vector file bytes changed")
    if _digest(pool.original_rows) != prepared["input_bindings"]["original_row_map_sha256"] or _digest(pool.original_query_rows) != prepared["input_bindings"]["original_query_row_map_sha256"]:
        raise AssertionError("original source row ledger changed")
    query_path = Path(prepared["query_rows"])
    if sha(query_path) != prepared["query_rows_sha256"]:
        raise AssertionError("query order bytes changed")
    with np.load(query_path) as arrays:
        queries = {key: arrays[key].copy() for key in ("calibration", "evaluation")}
    if len(set(map(int, queries["calibration"])) & set(map(int, queries["evaluation"]))):
        raise AssertionError("calibration/evaluation queries overlap")
    for seed, data in prepared["seeds"].items():
        if sha(data["cluster_labels"]) != data["cluster_sha256"]:
            raise AssertionError("cluster bytes changed")
        with np.load(data["cluster_labels"]) as arrays:
            labels = arrays["labels"].copy(); random_order = arrays["random_order"].copy()
            clustered_order = arrays["clustered_order"].copy(); source_rows = arrays["source_rows"].copy()
            cluster_order = arrays["cluster_order"].copy()
        if sorted(np.bincount(labels, minlength=5).tolist()) != sorted(data["wave_sizes"]):
            raise AssertionError("cluster sizes changed")
        if len(np.unique(labels)) != 5 or sorted(random_order.tolist()) != list(range(c.n)) or sorted(clustered_order.tolist()) != list(range(c.n)):
            raise AssertionError("cluster/order incomplete")
        if sorted(cluster_order.tolist()) != list(range(5)) or not np.all(np.diff([np.min(source_rows[labels == i]) for i in range(5)]) > 0):
            raise AssertionError("canonical cluster IDs or cluster permutation changed")
        expected_clustered = np.concatenate([np.flatnonzero(labels == cid) for cid in cluster_order])
        if not np.array_equal(np.sort(clustered_order), np.sort(expected_clustered)):
            raise AssertionError("clustered order does not cover all clusters")
        cursor = 0
        for cid, size in zip(cluster_order, data["wave_sizes"]):
            if not np.all(labels[clustered_order[cursor:cursor+size]] == cid):
                raise AssertionError("clustered insertion wave mixes cluster IDs")
            cursor += size
        start = load_state(data["initial"], len(pool.vectors))
        incoming = [None] * c.n
        path_orders, final_digests = {}, []
        for name, path in data["paths"].items():
            if sha(path["events"]) != path["events_sha256"]:
                raise AssertionError("event bytes changed")
            events = load_events(path["events"])
            if len(events) != (1 if name.endswith("1") else 5):
                raise AssertionError("event count changed")
            order = [w.id-c.n for event in events for w in event.writes]
            path_orders[name] = order
            if len(order) != c.n or sorted(order) != list(range(c.n)):
                raise AssertionError("missing/duplicate incoming ID")
            if any(w.category != int(labels[w.id-c.n]) or w.ordinal != int(pool.original_rows[w.row])
                   for event in events for w in event.writes):
                raise AssertionError("incoming ID/cluster/source-row metadata mapping changed")
            if name.endswith("5") and [len(event.writes) for event in events] != data["wave_sizes"]:
                raise AssertionError("wave size changed")
            live = Live(start.records(), len(pool.vectors))
            if path["states"]["0"]["ledger_sha256"] != start.digest():
                raise AssertionError("initial state differs across paths")
            for step in range(len(events)+1):
                if step:
                    live.apply(events[step-1])
                state_ref = path["states"][str(step)]
                if sha(state_ref["path"]) != state_ref["sha256"] or load_state(state_ref["path"], len(pool.vectors)).digest() != live.digest():
                    raise AssertionError("replay and state map disagree")
                for qname, qrows in queries.items():
                    control = path["controls_by_query_set"][qname][step]
                    _state_checks(pool, {"state": state_ref, "control": control}, qrows)
                if path["controls"][step] != path["controls_by_query_set"][data["query_rows"]][step]:
                    raise AssertionError("primary oracle mapping changed")
            if path["final_ledger_sha256"] != live.digest():
                raise AssertionError("final ledger changed")
            final_digests.append(live.digest())
            for w in events[0].writes if name == "M1" else ():
                incoming[w.id-c.n] = w.row
        if len(set(final_digests)) != 1 or path_orders["M1"] != path_orders["M5"] or path_orders["C1"] != path_orders["C5"]:
            raise AssertionError("initial/final or path insertion order differs")
        if path_orders["M1"] != random_order.tolist() or path_orders["C1"] != clustered_order.tolist():
            raise AssertionError("frozen cluster insertion order differs")
        if any(row is None for row in incoming) or not np.array_equal(pool.original_rows[incoming], source_rows):
            raise AssertionError("incoming source row mapping changed")
        original = np.concatenate(([w.row for w in start.records()], incoming))
        if _digest(pool.vectors[original]) != data["input_vector_sha256"]:
            raise AssertionError("selected original float32 vector bytes changed")
        if sha(data["cohort_codes"]) != data["paths"]["M1"]["cohort_sha256"]:
            raise AssertionError("fixed final cohorts changed")
        with np.load(data["cohort_codes"]) as codes:
            expected_labels = np.asarray(["old", "new", "boundary"], dtype=object)[codes["origin"]].tolist()
            saved_codes = {key: codes[key].copy() for key in ("origin", "dominant_cluster", "mixed", "new_fraction")}
        if data["cohort_labels"] != expected_labels:
            raise AssertionError("fixed final cohort labels changed")
        with np.load(data["paths"]["M1"]["controls"][-1]["path"]) as control:
            recomputed, cohort_counts = _cohorts(control["truth"], c.n, labels)
        if any(not np.array_equal(saved_codes[key], recomputed[key]) for key in saved_codes) or cohort_counts != data["cohort_counts"]:
            raise AssertionError("cohorts do not match frozen final exact neighbors")
    return True
