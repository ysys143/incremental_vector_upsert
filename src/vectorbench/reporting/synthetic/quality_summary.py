"""Derived endpoint and paired comparisons; run only after the native summary passes."""
import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
import statistics

from vectorbench.experiments.synthetic_insertion.contracts import load, read_ack, save, sha


CONDITION = ("db", "geometry", "ratio", "waves", "order", "policy", "cohort")


def read_rows(path):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    for row in rows:
        for key in ("seed", "ratio", "waves", "step", "count", "fresh_repeats"):
            if key in row:
                row[key] = int(row[key])
        for key in tuple(row):
            if key.startswith(("recall", "fresh10", "fresh100", "gap", "one_minus_five")):
                row[key] = float(row[key])
    return rows


def distribution(values):
    return {"mean": statistics.mean(values), "min": min(values), "max": max(values)}


def aggregate(rows, keys, values):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[key] for key in keys)].append(row)
    result = []
    for key, group in sorted(groups.items()):
        seeds = [r["seed"] for r in group]
        if len(set(seeds)) != len(seeds):
            raise AssertionError("aggregation mixed multiple observations for the same data seed")
        result.append({**dict(zip(keys, key)), "seeds": sorted(seeds),
                       **{name: distribution([r[name] for r in group]) for name in values}})
    return result


def run(root):
    receipt, ack = read_ack(root, "summary:all")
    manifest = load(root / "manifest.json")
    ledger = load(root / "ledger.json")
    if Counter(v["status"] for v in ledger.values()) != {"PASS": len(manifest["cells"])}:
        raise AssertionError("campaign still has unfinished cells")
    if receipt["result"]["counts"] != manifest["expected"]:
        raise AssertionError("native summary counts differ from the plan")
    summary = root / ledger["summary:all"]["attempts"][-1]["out"]
    sources = [summary / n for n in ("curves.csv", "endpoints.csv", "paired-paths.csv")]
    curves, endpoints, pairs = [read_rows(p) for p in sources]
    if len(curves) != 4 * manifest["expected"]["incremental_checkpoints"] or len(endpoints) != 4 * manifest["expected"]["paths"]:
        raise AssertionError("cohort rows missing")
    initial = {(r["id"], r["cohort"]): r for r in curves if r["step"] == 0}
    lookup = {tuple(r[k] for k in ("db", "seed", "geometry", "ratio", "waves", "order", "policy", "cohort")): r for r in endpoints}
    for row in endpoints:
        base = initial[(row["id"], row["cohort"])]
        for k in (10, 100):
            row[f"initial{k}"] = base[f"recall{k}"]
            row[f"before_after{k}"] = row[f"recall{k}"] - base[f"recall{k}"]
            row[f"fresh_range{k}"] = row[f"fresh{k}_max"] - row[f"fresh{k}_min"]

    comparisons = {"order": [], "policy": []}
    for row in endpoints:
        kind = "order" if row["order"] == "ordered" else "policy" if row["policy"] == "continuous" else None
        if kind is None:
            continue
        partner = dict(row, order="random", policy="settled")
        other = lookup[tuple(partner[k] for k in ("db", "seed", "geometry", "ratio", "waves", "order", "policy", "cohort"))]
        if row["data_hash"] != other["data_hash"] or row["initial_snapshot_sha"] != other["initial_snapshot_sha"]:
            raise AssertionError("comparison mixed data or initial graphs")
        comparisons[kind].append({**{k: row[k] for k in ("db", "seed", "geometry", "ratio", "waves", "cohort")},
                                  **{f"delta{k}": row[f"recall{k}"] - other[f"recall{k}"] for k in (10, 100)}})

    warnings = defaultdict(list)
    for row in curves:
        if row["step"] == 0:
            continue
        for k in (10, 100):
            if row[f"gap{k}"] <= -.005 and row[f"recall{k}"] < row[f"fresh{k}_min"]:
                warnings[tuple(row[n] for n in (*CONDITION, "step")) + (k,)].append(row["seed"])
    warning_rows = [{**dict(zip((*CONDITION, "step", "k"), key)), "seeds": sorted(set(seeds))}
                    for key, seeds in sorted(warnings.items()) if len(set(seeds)) >= 2]

    endpoint_values = [f"{name}{k}" for k in (10, 100)
                       for name in ("initial", "recall", "fresh", "gap", "before_after", "fresh_range")]
    results = {
        "units": "raw recall fractions; multiply differences by 100 for percentage points",
        "replication": "three paired data seeds; query rows and reused controls are not independent replicates",
        "endpoint_groups": aggregate(endpoints, CONDITION, endpoint_values),
        "one_minus_five_groups": aggregate(pairs, ("db", "geometry", "ratio", "order", "cohort"),
                                             ("one_minus_five_r10", "one_minus_five_r100")),
        "ordered_minus_random_groups": aggregate(comparisons["order"], ("db", "geometry", "ratio", "waves", "cohort"), ("delta10", "delta100")),
        "continuous_minus_settled_groups": aggregate(comparisons["policy"], ("db", "geometry", "ratio", "waves", "cohort"), ("delta10", "delta100")),
        "checkpoint_warning_conditions": warning_rows,
        "counts": {"curves": len(curves), "endpoints": len(endpoints), "paired": len(pairs),
                   "order_comparisons": len(comparisons["order"]), "policy_comparisons": len(comparisons["policy"])},
    }
    folder = root / "analysis" / "quality-summary"
    folder.mkdir(parents=True, exist_ok=False)
    save(folder / "aggregates.json", results)
    save(folder / "receipt.json", {"status": "PASS", "code_sha256": sha(Path(__file__)),
         "summary_run_id": ack["run_id"], "summary_receipt_sha256": ack["receipt_sha256"],
         "sources": {str(p.relative_to(root)): sha(p) for p in sources},
         "aggregate_sha256": sha(folder / "aggregates.json")})
    print(folder / "receipt.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    run(parser.parse_args().root)
