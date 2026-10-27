"""Complete old/new density diagnostics from immutable inputs after DB measurements finish."""
import argparse
import csv
from pathlib import Path
import time

import numpy as np

from vectorbench.experiments.pilot.model import digest, direct_exact
from vectorbench.experiments.synthetic_insertion.contracts import GEOMETRIES, load, read_ack, save, sha
from vectorbench.experiments.synthetic_insertion.data import nearest


def statistics(radii):
    lid = -1 / np.mean(np.log(np.maximum(radii[:, :-1], 1e-15) / radii[:, -1:]), axis=1)
    return {"nn1_p10": float(np.percentile(radii[:, 0], 10)),
            "nn1_p50": float(np.percentile(radii[:, 0], 50)),
            "nn1_p90": float(np.percentile(radii[:, 0], 90)),
            "nn20_p10": float(np.percentile(radii[:, -1], 10)),
            "nn20_p50": float(np.percentile(radii[:, -1], 50)),
            "nn20_p90": float(np.percentile(radii[:, -1], 90)),
            "lid_p10": float(np.percentile(lid, 10)), "lid_p50": float(np.percentile(lid, 50)),
            "lid_p90": float(np.percentile(lid, 90))}


def run(root):
    # This is a new derived artifact, never an alteration of measured cells or their source snapshots.
    read_ack(root, "summary:all")
    manifest = load(root / "manifest.json")
    c = manifest["config"]
    folder = root / "analysis" / "geometry-cohorts"
    folder.mkdir(parents=True, exist_ok=False)
    started = time.time()
    inputs, rows, baseline = {}, [], {}
    for seed in c["seeds"]:
        for geometry in GEOMETRIES:
            fk = f"s{seed}-{geometry}"
            fixture_path = root / "inputs" / fk / "fixture.npz"
            spec = manifest["cells"]["fixture:" + fk]
            if sha(fixture_path) != spec["files"][str(fixture_path.relative_to(root))]:
                raise AssertionError("fixture bytes changed")
            inputs[str(fixture_path.relative_to(root))] = sha(fixture_path)
            with np.load(fixture_path) as data:
                pool, order = data["pool"], data["random_order"]
            metadata = load(root / "inputs" / fk / "metadata.json")
            ids0 = np.arange(c["n"])
            if seed not in baseline:
                ns, radii = nearest(ids0, pool[:c["n"]], pool[:c["n"]], 20, exclude=ids0, distances=True)
                np.testing.assert_array_equal(ns[:4], direct_exact(ids0, pool[:c["n"]], pool[:4], 21)[:, 1:])
                baseline[seed] = statistics(radii)
                rows.append({"seed": seed, "geometry": "baseline", "ratio": 0, "cohort": "old",
                             "count": c["n"], **baseline[seed], "new_to_old_nn1_median": None,
                             "new_to_old_over_old_internal_nn1": None})
                np.savez_compressed(folder / f"s{seed}-baseline.npz", ids=ids0, neighbors=ns, radii=radii)
            for ratio in c["ratios"]:
                ids = np.sort(np.concatenate([ids0, order[:c["n"] * ratio // 100]]))
                state_hash = digest(ids, pool[ids])
                if state_hash not in metadata["states"]:
                    raise AssertionError("derived state is not a measured input state")
                ns, radii = nearest(ids, pool[ids], pool[ids], 20, exclude=ids, distances=True)
                np.testing.assert_array_equal(ns[:4], direct_exact(ids, pool[ids], pool[ids[:4]], 21)[:, 1:])
                np.testing.assert_allclose(np.percentile(radii[:, -1], [10, 50, 90]),
                    metadata["final_diagnostics"][str(ratio)]["r20_quantiles"], rtol=1e-10, atol=1e-10)
                novel_ids = ids[ids >= c["n"]]
                _, novel_to_old = nearest(ids0, pool[:c["n"]], pool[novel_ids], 1, distances=True)
                distance = float(np.median(novel_to_old[:, 0]))
                for name, selected in (("old", ids < c["n"]), ("new", ids >= c["n"])):
                    rows.append({"seed": seed, "geometry": geometry, "ratio": ratio, "cohort": name,
                                 "count": int(selected.sum()), **statistics(radii[selected]),
                                 "new_to_old_nn1_median": distance,
                                 "new_to_old_over_old_internal_nn1": distance / baseline[seed]["nn1_p50"]})
                np.savez_compressed(folder / f"s{seed}-{geometry}-r{ratio}.npz", ids=ids, neighbors=ns,
                                    radii=radii, new_to_old_nn1=novel_to_old[:, 0])
                print(f"geometry cohort diagnostics: seed={seed} geometry={geometry} ratio={ratio}", flush=True)
    with open(folder / "cohorts.csv", "w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    save(folder / "receipt.json", {"status": "PASS", "elapsed_s": time.time() - started,
        "rows": len(rows), "inputs": inputs, "numpy": np.__version__,
        "code_sha256": sha(Path(__file__)), "nearest_code_sha256": sha("src/vectorbench/experiments/synthetic_insertion/data.py"),
        "cohort_scope": "old/new document vectors in each final joint exact 20-NN graph; not query labels or a DB's internal ANN graph",
        "lid_definition": "(k-1)/sum(log(r_k/r_i)), k=20, i=1..k-1",
        "distance_ratio_definition": "median new-to-D0 NN1 divided by median self-excluded internal D0 NN1",
        "files": {p.name: sha(p) for p in folder.iterdir() if p.is_file()}})
    print(folder / "receipt.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    run(args.root)
