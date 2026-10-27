"""Prepare immutable inputs and a finite work catalogue before any measured DB work."""
import argparse
import datetime
import hashlib
from pathlib import Path
import sys
import traceback
import uuid

import numpy as np

from vectorbench.experiments.pilot.model import digest, validate_truth
from vectorbench.experiments.pilot.run import IMAGES
from vectorbench.evidence.source_manifest import SYNTHETIC_PLAN, freeze_sources
from vectorbench.experiments.synthetic_insertion.contracts import (DATABASES, GEOMETRIES, base_key, canonical, case_specs, config,
                        expected_counts, fixture_key, save, sha)
from vectorbench.experiments.synthetic_insertion.data import (base_data, graph_diagnostics, make_fixture, nearest, states_for,
                   validate_geometry, waves_for)


def source_receipt(root):
    return freeze_sources(root, SYNTHETIC_PLAN)


def prepare(root, smoke=False, prerequisite=None):
    root.mkdir(parents=True, exist_ok=False)
    c = config(smoke)
    save(root / "preparation.json", {"status": "RUNNING", "config": c})
    manifest = {"id": root.name, "config": c, "images": IMAGES,
                "measurement_python": sys.executable, "python": sys.version,
                "numpy": np.__version__, "sources": source_receipt(root), "cells": {},
                "prerequisite_smoke": str(prerequisite.resolve()) if prerequisite else None,
                "expected": expected_counts(c), "project_checks": "without-setup by explicit user choice"}
    cases = case_specs(c)
    pending_fresh = {}
    for seed in [c["calibration_seed"], *c["seeds"]]:
        common = base_data(c["n"], c["dim"], seed)
        initial_diagnostic = graph_diagnostics(common[0], np.arange(c["n"]), c["n"], seed)
        for geometry in GEOMETRIES:
            fk = fixture_key(seed, geometry)
            folder = root / "inputs" / fk
            folder.mkdir(parents=True)
            f = make_fixture(c, seed, geometry, common)
            np.savez_compressed(folder / "fixture.npz", **f)
            ids0 = np.arange(c["n"], dtype=np.int64)
            relevant = [s for s in cases if s["seed"] == seed and s["geometry"] == geometry]
            states = {}

            def add_state(ids):
                state_hash = digest(ids, f["pool"][ids])
                if state_hash not in states:
                    truth = nearest(ids, f["pool"][ids], f["queries"])
                    validate_truth(ids, f["pool"][ids], f["queries"], truth)
                    path = folder / "states" / (state_hash + ".npz")
                    path.parent.mkdir(exist_ok=True)
                    np.savez_compressed(path, ids=ids, truth=truth)
                    states[state_hash] = {"path": str(path.relative_to(root)), "sha256": sha(path),
                                          "count": len(ids), "data_hash": state_hash}
                return state_hash

            base_hash = add_state(ids0)
            final_hash = add_state(np.arange(2 * c["n"], dtype=np.int64))
            for spec in relevant:
                states_list = states_for(spec, c, f)
                waves = waves_for(spec, c, f)
                spec["states"] = [add_state(ids) for ids in states_list]
                spec["wave_hashes"] = [digest(w, f["pool"][w]) for w in waves]
                spec["wave_counts"] = [len(w) for w in waves]
                spec["fixture"] = fk
                spec["base"] = base_key(spec["db"], seed)
                spec["deps"] = ["fixture:" + fk, "base:" + spec["base"]]
                for h in spec["states"][1:]:
                    slot = (spec["db"], fk, h)
                    stage = spec["stage"]
                    if slot not in pending_fresh or stage < pending_fresh[slot]["stage"]:
                        pending_fresh[slot] = {"db": spec["db"], "fixture": fk, "state": h,
                                              "stage": stage, "seed": seed, "geometry": geometry}
                    if h == spec["states"][-1]:
                        pending_fresh[slot]["terminal"] = True
                manifest["cells"]["incremental:" + spec["id"]] = spec
            final_diagnostics = {}
            for ratio in ([100] if seed == c["calibration_seed"] else c["ratios"]):
                selected = np.sort(np.concatenate([ids0, f["random_order"][:c["n"] * ratio // 100]]))
                final_diagnostics[str(ratio)] = graph_diagnostics(f["pool"], selected, c["n"], seed)
            save(folder / "geometry-gate.json", {"initial": initial_diagnostic, "final": final_diagnostics})
            validate_geometry(geometry, initial_diagnostic, final_diagnostics["100"])
            metadata = {"id": fk, "seed": seed, "geometry": geometry, "base_state": base_hash,
                        "max_state": final_hash, "states": states,
                        "query_hash": hashlib.sha256(f["queries"].tobytes()).hexdigest(),
                        "initial_diagnostic": initial_diagnostic, "final_diagnostics": final_diagnostics,
                        "cohort_names": ["old", "new", "boundary"], "config": c}
            save(folder / "metadata.json", metadata)
            files = {str(p.relative_to(root)): sha(p) for p in folder.rglob("*") if p.is_file()}
            manifest["cells"]["fixture:" + fk] = {"id": fk, "kind": "fixture", "stage": "SETUP",
                "fixture": fk, "files": files, "deps": []}
            print(canonical({"fixture": fk, "states": len(states), "status": "prepared"}), flush=True)
            save(root / "preparation.json", {"status": "RUNNING", "last_fixture": fk, "config": c})
    for db in DATABASES:
        manifest["cells"]["tune:" + db] = {"id": db, "kind": "tune", "stage": "SETUP", "db": db,
            "deps": ["fixture:" + fixture_key(c["calibration_seed"], g) for g in GEOMETRIES]}
        for seed in c["seeds"]:
            key = base_key(db, seed)
            manifest["cells"]["base:" + key] = {"id": key, "kind": "base", "stage": "SETUP",
                "db": db, "seed": seed, "fixture": fixture_key(seed, "iid"),
                "deps": ["tune:" + db, "fixture:" + fixture_key(seed, "iid")]}
    fresh_count = 0
    for (db, fk, h), entry in sorted(pending_fresh.items()):
        repetitions = 3 if entry.get("terminal") else 1
        for rep in range(repetitions):
            key = f"{db}-{fk}-{h[:20]}-rep{rep}"
            spec = {**entry, "id": key, "kind": "fresh", "repeat": rep,
                    "deps": ["tune:" + db, "fixture:" + fk]}
            manifest["cells"]["fresh:" + key] = spec
            fresh_count += 1
    manifest["expected"]["fresh_checkpoints"] = fresh_count
    manifest["expected"]["quality_measurements"] = fresh_count + manifest["expected"]["incremental_checkpoints"]
    if not smoke and manifest["expected"] != {"paths": 132, "incremental_checkpoints": 504,
                                               "fresh_checkpoints": 384, "quality_measurements": 888}:
        raise AssertionError("catalogue differs from the approved experiment count: " + canonical(manifest["expected"]))
    for spec in manifest["cells"].values():
        if not all(dep in manifest["cells"] for dep in spec["deps"]):
            raise AssertionError("missing catalogue dependency")
    manifest["cells"]["summary:all"] = {"id": "all", "kind": "summary", "stage": "REPORT",
        "deps": list(manifest["cells"])}
    save(root / "manifest.json", manifest)
    save(root / "ledger.json", {key: {"status": "NOT_STARTED", "attempts": []} for key in manifest["cells"]})
    save(root / "preparation.json", {"status": "PASS", "expected": manifest["expected"], "config": c})
    print(canonical({"root": str(root), "status": "PREPARED", "expected": manifest["expected"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--smoke-root", type=Path)
    args = parser.parse_args()
    name = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    if not args.smoke and args.smoke_root is None:
        raise SystemExit("full campaign requires --smoke-root with completed native smoke evidence")
    target = args.out or Path("runs") / ("synthetic-" + name)
    existed = target.exists()
    try:
        prepare(target, args.smoke, args.smoke_root)
    except BaseException as exc:
        if not existed and target.exists():
            save(target / "preparation.json", {"status": "FAIL", "first_error": repr(exc), "traceback": traceback.format_exc()})
        raise
