"""Read a completed real source phase through its actual native instance."""
import argparse
import json
from pathlib import Path

import dagster as dg

from vectorbench.evidence.contracts import load, sha
from vectorbench.orchestration.engine import native_receipt
from vectorbench.evidence.registry import Registry


def read(root, dataset):
    root = root.resolve()
    if load(root / "manifest.json")["mode"] != "real-source-preparation":
        raise AssertionError("orchestration/synthetic runs cannot qualify a real source prerequisite")
    node = "source:" + dataset
    with dg.DagsterInstance.from_config(str(root / "dagster-home")) as instance:
        receipt = native_receipt(root, node, instance)
        if receipt["result"]["data_kind"] != "real-precomputed" or receipt["result"]["dataset"] != dataset or receipt["result"]["metric"] != "ip":
            raise AssertionError("native producer is not the requested real IP dataset")
        evaluations = [e.asset_check_evaluation for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
        if len(evaluations) != 1 or not evaluations[0].passed:
            raise AssertionError("real source producer has no passed native evidence check")
    attempt = max((a for a in Registry(root / "admission.sqlite").snapshot()["attempts"] if a["node"] == node), key=lambda a:a["started"])
    folder = (root / attempt["receipt"]).parent
    return {"dataset": dataset, "data_kind": "real-precomputed", "metric": "ip",
            "native_root": str(root), "run_id": receipt["run_id"], "receipt_sha256": sha(root / attempt["receipt"]),
            "base_path": str(folder / "base.fbin"), "query_path": str(folder / "queries.fbin"),
            "base_sha256": receipt["result"]["base"]["sha256"], "query_sha256": receipt["result"]["queries"]["sha256"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("dataset", choices=("msmarco", "wikipedia"))
    args = parser.parse_args()
    print(json.dumps(read(args.root, args.dataset)))
