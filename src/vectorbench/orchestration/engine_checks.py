"""Small real native-consumer checks, explicitly not a real-vector DB smoke."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
import traceback

import dagster as dg

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.orchestration.engine import build, native_receipt
from vectorbench.evidence.registry import Registry

ACTIVE_ROOT = None


def main():
    global ACTIVE_ROOT
    root = (Path("runs") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-engine-checks")).resolve()
    root.mkdir(parents=True)
    ACTIVE_ROOT = root
    home = root / "dagster-home"
    home.mkdir()
    (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    # Diagnostics deliberately use tiny synthetic process fixtures, never public-vector evidence.
    nodes = {}
    for identity in ("a", "b"):
        nodes["source:" + identity] = {"id": identity, "role": "source", "resource": "cpu",
            "action": "fixture", "phase": "CHECK", "deps": []}
    nodes["barrier:all"] = {"id": "all", "role": "barrier", "resource": "cpu", "action": "fixture",
                           "phase": "CHECK", "singleton": True, "deps": ["source:a", "source:b"]}
    nodes["trial:a"] = {"id": "a", "role": "trial", "resource": "db", "action": "fixture",
                       "phase": "CHECK", "deps": ["barrier:all"]}
    manifest = {"mode": "orchestration-fixture", "nodes": nodes, "sources": {},
                "measurement_python": "/opt/homebrew/bin/python3", "deadlines": {"CHECK": time.time() + 180}}
    save(root / "manifest.json", manifest)
    defs = build(root)
    dg.Definitions.validate_loadable(defs)
    checks = ["native definitions with partition mappings, automation and blocking checks validated"]
    with dg.DagsterInstance.from_config(str(home)) as instance, (root / "native.log").open("w") as logs:
        stdout, stderr = sys.stdout, sys.stderr
        try:
            sys.stdout = sys.stderr = logs
            run_ids = []
            for node in nodes:
                role, partition = node.split(":", 1)
                kwargs = {} if nodes[node].get("singleton") else {"partition_key": partition}
                result = dg.materialize([*defs.assets, *defs.asset_checks],
                    selection=dg.AssetSelection.assets(role), instance=instance, raise_on_error=False, **kwargs)
                if not result.success:
                    raise AssertionError("native diagnostic consumer failed: " + node + "; see " + str(root / "native.log"))
                record = instance.get_run_by_id(result.run_id)
                if record.status != dg.DagsterRunStatus.SUCCESS:
                    raise AssertionError("native completion is not SUCCESS")
                evaluations = [e.asset_check_evaluation for e in instance.all_logs(result.run_id) if e.asset_check_evaluation]
                if len(evaluations) != 1 or not evaluations[0].passed:
                    raise AssertionError("actual blocking asset check did not pass")
                run_ids.append(result.run_id)
            for node in nodes:
                native_receipt(root, node, instance)
            checks += ["four real native runs and their blocking checks passed", "partitioned dependencies require native success and unchanged scoped receipts"]
            # A duplicate request reaches the native consumer and must never repeat the worker.
            before = Registry(root / "admission.sqlite").snapshot()
            duplicate = dg.materialize([*defs.assets, *defs.asset_checks],
                selection=dg.AssetSelection.assets("source"), partition_key="a", instance=instance, raise_on_error=False)
            if duplicate.success or Registry(root / "admission.sqlite").snapshot() != before:
                raise AssertionError("duplicate native request repeated an acknowledged worker")
            checks.append("duplicate actual native materialization rejected without rewriting worker history")
            parent = native_receipt(root, "source:a", instance)
            original = root / next(p for p in parent["artifacts"] if p.endswith("fixture-output.json"))
            original.write_text('{"intentionally":"changed"}\n')
            try:
                native_receipt(root, "source:a", instance)
            except AssertionError:
                checks.append("changed acknowledged dependency artifact rejected")
            else:
                raise AssertionError("changed dependency accepted")
        finally:
            sys.stdout, sys.stderr = stdout, stderr
    save(root / "receipt.json", {"status": "PASS", "scope": "ORCHESTRATION_ONLY; no real dataset or DB smoke",
         "checks": checks, "native_success_run_ids": run_ids, "intentional_duplicate_failure_run_id": duplicate.run_id,
         "code_sha256": sha(Path(__file__)), "mutated_fixture": str(original.relative_to(root))})
    print(root / "receipt.json")


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        if ACTIVE_ROOT is not None:
            save(ACTIVE_ROOT / "failure.json", {"status": "FAIL", "scope": "ORCHESTRATION_ONLY",
                 "traceback": traceback.format_exc()})
        raise
