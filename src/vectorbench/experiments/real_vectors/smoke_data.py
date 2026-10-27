"""Real-source entry fixtures: keep all dimensions and precompute every live oracle."""
from pathlib import Path
import numpy as np

from vectorbench.evidence.contracts import Config, load, save, sha
from vectorbench.datasets.data import oracle
from vectorbench.workloads.events import Event, Live, Write, initial, replacement_history
from vectorbench.datasets.storage import save_events, save_state
from vectorbench.experiments.real_vectors.smoke_spec import OPERATIONS, config
from vectorbench.datasets.selection import entry_split
from vectorbench.evidence.checkpoints import Checkpoints


def prepare(pool, out, large_requests=False):
    c = config(large_requests)
    rounds = 1 if large_requests else 2
    insert_chunk = c.n if large_requests else c.n//10
    update_chunk = c.n//2 if large_requests else c.n//10
    split = entry_split(pool, c.n, rounds*insert_chunk, c.calibration_queries, c.query_rows)
    rows, replacements = split["initial"], split["replacement"]
    labels = np.arange(len(pool.vectors), dtype=np.int64) % 10
    start = initial(rows, labels)
    query_rows = split["evaluation_queries"]
    calibrate_queries = split["calibration_queries"]
    calibration_rows = split["calibration_documents"]
    save(out / "duplicate-diagnostics.json", split["diagnostics"])
    calibration = Live(initial(calibration_rows, labels), len(pool.vectors))
    save_state(out / "calibration.npz", calibration)
    oracle(pool, calibration, calibrate_queries, out / "calibration-oracle.npz", c)
    save_state(out / "initial.npz", Live(start, len(pool.vectors)))
    np.savez_compressed(out / "query-rows.npz", evaluation=query_rows, calibration=calibrate_queries)
    cache = Checkpoints(pool, query_rows, out / "checkpoints", c)
    paths = {}
    for operation in OPERATIONS:
        if operation == "insert":
            events = [Event(writes=tuple(Write(c.n + i, int(replacements[i]), 0,
                            int(labels[replacements[i]]), int(replacements[i]))
                            for i in range(step*insert_chunk, (step+1)*insert_chunk))) for step in range(rounds)]
        else:
            events = replacement_history(start, replacements, rounds=rounds, chunk=update_chunk,
                                         operation=operation, seed=11, categories=labels)
        folder = out / operation
        live = Live(start, len(pool.vectors))
        for event in events:
            live.apply(event)
        compiled = {"events": events, "observed_steps": list(range(rounds+1)), "final": live,
                    "counts": {"waves": rounds, "writes": sum(len(e.writes) for e in events),
                               "deletes": sum(len(e.deletes) for e in events), "final_count": len(live.current)}}
        prepared = cache.path(start, compiled, folder)
        paths[operation] = {"events": prepared["events"], "controls": prepared["controls"],
                            "final": prepared["states"][str(rounds)]["path"], "final_ledger_sha256": live.digest()}
    result = {"dataset": pool.dataset, "initial": str(out / "initial.npz"),
              "calibration": str(out / "calibration.npz"),
              "calibration_control": {"path": str(out / "calibration-oracle.npz"), "sha256": sha(out / "calibration-oracle.npz"),
                                      "receipt_sha256": sha(out / "calibration-oracle.npz.receipt.json")},
              "query_rows": str(out / "query-rows.npz"), "paths": paths,
              "metadata_kind": "controlled row-modulo-10 labels; not natural document metadata",
              "duplicates": split["diagnostics"],
              "unique_oracles": len(cache.oracles), "unique_ledgers": len(cache.states),
              "scope": "REQUEST_SMOKE_ONLY; not main five/fifty-round study" if large_requests else "ENTRY_SMOKE_ONLY; not main five/fifty-round study"}
    save(out / "inputs.json", result)
    return result
