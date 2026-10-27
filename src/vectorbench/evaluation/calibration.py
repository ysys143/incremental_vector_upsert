"""Disclosed real-input smoke calibration; no inheritance of synthetic winners."""
from pathlib import Path
import numpy as np

from vectorbench.experiments.pilot.model import validate_snapshot
from vectorbench.evidence.contracts import save
from vectorbench.orchestration.isolation import measured
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.backends.server import Server
from vectorbench.workloads.workload import oracle_for, read_point


def tune(db, pool, live, query_rows, control, out, config):
    server = Server(db, out / "server", config=config)
    error = None
    try:
        adapter = server.start()
        ids, rows, attrs = metadata_arrays(live)
        vectors, queries = pool.vectors[rows], np.asarray(pool.queries[query_rows])
        truth = oracle_for(control, pool, live, query_rows)
        probes, candidates = [], []
        for batch in (100, 1000):
            adapter.batch = batch
            setup = measured(server, out / f"load-{batch}",
                lambda: adapter.setup(ids, vectors, (16, 100), attributes=attrs))
            validate_snapshot(ids, vectors, *adapter.snapshot())
            probes.append({"batch": batch, "setup": setup})
            save(out / "load-probes.json", probes)
        batch = min(probes, key=lambda p: p["setup"]["load_s"])["batch"]
        ef_values = (100, 128, 256, 512, 1000) if config.n >= 10000 else (100, 128, 256, 512)
        for build in ((16, 100), (32, 200)):
            adapter.batch = batch
            setup = measured(server, out / f"build-{build[0]}",
                lambda: adapter.setup(ids, vectors, build, attributes=attrs))
            for ef in ef_values:
                adapter.set_ef(ef)
                result = read_point(server, adapter, live, pool, queries, truth,
                    ["calibration"] * len(queries), out / f"m{build[0]}-ef{ef}", config, control)
                candidates.append({"build": list(build), "ef": ef, "initial_batch": batch,
                                   "setup": setup, "point": result})
                save(out / "candidates.json", candidates)
        frontiers = {}
        for target in (.95, .99):
            eligible = [v for v in candidates if min(v["point"]["recall10"], v["point"]["recall100"]) >= target
                        and v["point"]["shortfall10"] == 0 and v["point"]["shortfall100"] == 0]
            if not eligible:
                raise AssertionError(f"no actual ANN candidate met smoke Recall@10/100 {target}")
            frontier = [a for a in eligible if not any(
                b["point"]["p95_ms"] <= a["point"]["p95_ms"]
                and b["setup"]["build_s"] <= a["setup"]["build_s"]
                and (b["point"]["p95_ms"] < a["point"]["p95_ms"] or b["setup"]["build_s"] < a["setup"]["build_s"])
                for b in eligible)]
            frontiers[str(target)] = [{"build": v["build"], "ef": v["ef"]} for v in frontier]
        best = min((v for v in candidates if min(v["point"]["recall10"], v["point"]["recall100"]) >= .99
                    and v["point"]["shortfall100"] == 0), key=lambda v: v["point"]["p95_ms"])
        result = {"build": best["build"], "ef": best["ef"], "initial_batch": batch,
                  "grid": {"build": [[16, 100], [32, 200]], "ef": list(ef_values), "batch": [100, 1000]},
                  "pareto": frontiers, "selected_target": .99, "candidates": len(candidates),
                  "scope": f"real-input smoke calibration at N={config.n}; main histories need separate qualification",
                  "selection": "lowest measured p95 among quality-qualified candidates; shared-host indicative"}
        save(out / "tuning.json", result)
        return result
    except BaseException as exc:
        error = exc
        raise
    finally:
        errors = server.cleanup()
        if errors:
            save(out / "cleanup-failure.json", {"errors": errors, "first_error": repr(error)})
            if error is None:
                raise RuntimeError("calibration DB cleanup failed: " + repr(errors))
