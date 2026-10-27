"""Adopt only independently successful input/Postgres nodes after a Qdrant transport fix."""
import argparse
import json
from pathlib import Path
import dagster as dg
from vectorbench.evidence.contracts import load, sha
from vectorbench.orchestration.engine import native_receipt


def read(root, node, expected_receipt_sha):
    root = root.resolve()
    manifest = load(root / "manifest.json")
    spec = manifest["nodes"][node]
    if manifest["mode"] != "real-request-smoke" or not (spec["action"] == "prepare" or spec.get("db") == "postgres"):
        raise AssertionError("adoption is restricted to input and unchanged Postgres consumers")
    with dg.DagsterInstance.from_config(str(root / "dagster-home")) as instance:
        receipt = native_receipt(root, node, instance)
        checks = [e.asset_check_evaluation for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
        if len(checks) != 1 or not checks[0].passed:
            raise AssertionError("adopted native node has no passed blocking check")
    path = root / "cells" / node.replace(":", "__") / receipt["run_id"] / "receipt.json"
    if sha(path) != expected_receipt_sha:
        raise AssertionError("adopted native receipt changed")
    return {**receipt["result"], "native_adoption": {"root": str(root), "node": node,
            "run_id": receipt["run_id"], "receipt_sha256": sha(path)}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("node")
    parser.add_argument("receipt_sha256")
    args = parser.parse_args()
    print(json.dumps(read(args.root, args.node, args.receipt_sha256)))
