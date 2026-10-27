"""Rebind only 34 completed clustered native nodes to the original clock and bytes."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path

from vectorbench.evidence.contracts import load, sha
from vectorbench.evidence.registry import Registry

# These files may change to implement successor control; the scientific producer
# and measurement files must retain their original byte hashes.
CONTROL_CHANGES = {
    "src/vectorbench/experiments/real_vectors/clustered/adopt.py",
    "src/vectorbench/experiments/real_vectors/clustered/diagnostics.py",
    "src/vectorbench/experiments/real_vectors/clustered/report.py",
    "src/vectorbench/orchestration/guardian.py",
    "src/vectorbench/experiments/real_vectors/clustered/gates.py",
    "src/vectorbench/experiments/real_vectors/clustered/run.py",
    "src/vectorbench/experiments/real_vectors/clustered/contract.py",
    "src/vectorbench/orchestration/task.py",
    "src/vectorbench/orchestration/control.py",
    "src/vectorbench/orchestration/isolation.py",
    "src/vectorbench/evidence/contracts.py",
    "src/vectorbench/evidence/registry.py",
    "src/vectorbench/backends/server.py",
    "scripts/audit_clustered_completion.py",
    "scripts/finalize_clustered.py",
    "scripts/resume_clustered.py",
}


def _reference_file(reference):
    path = Path(reference["path"]).resolve()
    if sha(path) != reference["sha256"]:
        raise AssertionError("forecast exception or failed-cost evidence bytes changed")
    return path, load(path)


def verify_exception(current, predecessor_root):
    """The user's forecast exception is specific to one failed predecessor gate."""
    if "forecast_exception" not in current:
        raise AssertionError("explicit forecast exception reference missing")
    path, exception = _reference_file(current["forecast_exception"])
    predecessor_root = Path(predecessor_root).resolve()
    predecessor_manifest = predecessor_root / "manifest.json"
    required = {"action", "original_deadline", "original_t0", "predecessor_root",
                "predecessor_manifest_sha256", "failed_cost_gate", "user_text", "recorded_at"}
    if not required.issubset(exception):
        raise AssertionError("forecast exception fields incomplete")
    old = load(predecessor_manifest)
    old_auth = load(old["authorization"]["path"])
    if (exception["action"] != "PROCEED_DESPITE_FORECAST_BLOCK"
            or exception["user_text"] != "그냥 진행해."
            or Path(exception["predecessor_root"]).resolve() != predecessor_root
            or exception["predecessor_manifest_sha256"] != sha(predecessor_manifest)
            or exception["original_t0"] != old_auth["t0"]
            or exception["original_deadline"] != old_auth["deadline"]
            or not old_auth["t0"] <= exception["recorded_at"] <= old_auth["deadline"]):
        raise AssertionError("forecast exception does not preserve original authorization")
    failed_path, failed = _reference_file(exception["failed_cost_gate"])
    expected = predecessor_root / "cells" / "cost_gate__all"
    if expected not in failed_path.parents or failed_path.name != "cost-gate.json" or failed.get("status") != "BLOCK":
        raise AssertionError("exception does not target the failed BLOCK forecast")
    registry = Registry(predecessor_root / "admission.sqlite").snapshot()
    failed_attempts = [a for a in registry["attempts"] if a["node"] == "cost_gate:all" and a["outcome"] == "failed"]
    if len(failed_attempts) != 1 or failed_path.parent.name != failed_attempts[0]["run_id"]:
        raise AssertionError("exception points to another or nonfailed cost gate")
    if current["execution_window_end"] != old_auth["deadline"]:
        raise AssertionError("successor extended the fixed execution deadline")
    return {"path": str(path), "sha256": sha(path), "failed_cost_gate": str(failed_path),
            "failed_cost_gate_sha256": sha(failed_path), "original_t0": old_auth["t0"],
            "original_deadline": old_auth["deadline"], "user_text": exception["user_text"]}


def verify_static(current_root, node):
    """Validate scope, source snapshots, old ownership and exact successful node map."""
    current_root = Path(current_root).resolve()
    current = load(current_root / "manifest.json")
    if current.get("mode") != "real-clustered-insertion" or node not in current["nodes"]:
        raise AssertionError("wrong clustered campaign or node")
    spec = current["nodes"][node]
    if "native_adoption" not in spec:
        raise AssertionError("node is not designated for adoption")
    ref = spec["native_adoption"]
    predecessor_root = Path(ref["root"]).resolve()
    if predecessor_root == current_root:
        raise AssertionError("self-adoption is forbidden")
    if "predecessor" in current and Path(current["predecessor"]).resolve() != predecessor_root:
        raise AssertionError("adoption points to another predecessor")
    previous = load(predecessor_root / "manifest.json")
    if (previous.get("mode") != current["mode"] or previous.get("config") != current.get("config")
            or previous.get("matrix") != current.get("matrix")
            or previous.get("execution_window_end") != current.get("execution_window_end")
            or previous.get("source_refs") != current.get("source_refs")
            or previous.get("source_manifest") != current.get("source_manifest")):
        raise AssertionError("adoption changed scientific configuration, inputs or clock")
    old_spec = previous["nodes"].get(node)
    if old_spec is None or {k: v for k, v in spec.items() if k != "native_adoption"} != old_spec:
        raise AssertionError("adoption changed node action, deps, resource or scientific fields")
    if node == "cost_gate:all" or old_spec["action"] in {"cost_gate", "report"}:
        raise AssertionError("failed cost gate and report cannot be adopted")
    old_sources, new_sources = previous["sources"], current["sources"]
    if (set(old_sources) ^ set(new_sources)) - CONTROL_CHANGES:
        raise AssertionError("unapproved source file added or removed")
    for name in set(old_sources) | set(new_sources):
        if name in old_sources and sha(predecessor_root / "source" / name) != old_sources[name]:
            raise AssertionError("predecessor source snapshot changed: " + name)
        if name in new_sources:
            if sha(current_root / "source" / name) != new_sources[name] or sha(name) != new_sources[name]:
                raise AssertionError("current source snapshot/live file changed: " + name)
        if name not in CONTROL_CHANGES and old_sources.get(name) != new_sources.get(name):
            raise AssertionError("scientific source changed: " + name)
    controller = load(predecessor_root / "controller.json")
    guardian = load(predecessor_root / "guardian-receipt.json")
    old_auth = load(previous["authorization"]["path"])
    if (not controller.get("finished") or controller.get("status") != "STOPPED_INCOMPLETE"
            or controller.get("finished_at", float("inf")) > old_auth["deadline"]
            or guardian.get("status") != "RECONCILED"
            or any(item.get("cleanup", {}).get("errors") for item in guardian.get("cleanup", []))):
        raise AssertionError("predecessor has not finished native cleanup")
    registry = Registry(predecessor_root / "admission.sqlite").snapshot()
    attempts = registry["attempts"]
    successful = {a["node"]: a for a in attempts if a["outcome"] == "worker-completed"}
    if (registry["leases"] or len(attempts) != 35 or len(successful) != 34
            or {a["node"] for a in attempts if a["outcome"] == "failed"} != {"cost_gate:all"}
            or any(a["ended"] is None for a in attempts)
            or node not in successful or successful[node]["run_id"] != ref["run_id"]):
        raise AssertionError("node is not one of 34 completed predecessor attempts")
    exception = verify_exception(current, predecessor_root)
    return current, previous, predecessor_root, old_spec, successful[node], exception


def original_timing(receipt, check_finished, attempt):
    """Keep the original lease-to-finish cost basis and the separate wall interval."""
    return {"started_at": receipt["started"], "lease_acquired_at": receipt["lease_acquired"],
            "lease_acquired": receipt["lease_acquired"], "finished": receipt["finished"],
            "worker_finished_at": receipt["finished"], "native_check_finished_at": check_finished,
            "elapsed_s": receipt["finished"] - receipt["lease_acquired"],
            "wall_elapsed_s": receipt["finished"] - receipt["started"],
            "registry_finished_at": attempt["ended"],
            "worker_peak_rss_bytes": receipt.get("worker_peak_rss_bytes")}


def qualified_evidence_checks(evaluations, node, spec, run_id):
    """A run may materialize/check multiple nodes with the same partition."""
    role = node.split(":", 1)[0]
    partition = None if spec.get("singleton") else spec["id"]
    checks = list(evaluations)
    if not checks or any(not check.passed for check in checks):
        raise AssertionError("native run contains missing or failed blocking checks")
    target = [check for check in checks
              if tuple(check.asset_key.path) == (role,)
              and check.check_name == "evidence" and check.partition == partition]
    if len(target) != 1:
        raise AssertionError("target asset evidence check is missing, duplicate or wrong partition")
    materialization = target[0].target_materialization_data
    if materialization is None or materialization.run_id != run_id:
        raise AssertionError("target evidence check refers to a different materialization run")
    return target[0]


def verify(current_root, node):
    """Deep native producer/check/artifact verification, without redoing any DB work."""
    import dagster as dg
    from vectorbench.orchestration.engine import native_receipt

    current_root = Path(current_root).resolve()
    current, previous, predecessor_root, old_spec, attempt, exception = verify_static(current_root, node)
    ref = current["nodes"][node]["native_adoption"]
    with dg.DagsterInstance.from_config(str(predecessor_root / "dagster-home")) as instance:
        receipt = native_receipt(predecessor_root, node, instance)
        events = [e for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
        checks = [e.asset_check_evaluation for e in events]
        target = qualified_evidence_checks(checks, node, old_spec, receipt["run_id"])
        check_finished = max(e.timestamp for e in events if e.asset_check_evaluation == target)
    original_path = predecessor_root / "cells" / node.replace(":", "__") / receipt["run_id"] / "receipt.json"
    if (receipt["run_id"] != ref["run_id"] or sha(original_path) != ref["receipt_sha256"]
            or receipt["resource"] != old_spec["resource"] or receipt["finished"] > exception["original_deadline"]):
        raise AssertionError("adopted original native receipt differs or is late")
    cell = original_path.parent
    owner = load(cell / "owner-cleanup.json")
    if owner.get("status") != "CLEANED" or owner.get("cleanup", {}).get("errors"):
        raise AssertionError("predecessor owned cleanup failed")
    result = deepcopy(receipt["result"])
    timing = original_timing(receipt, check_finished, attempt)
    result["original_native_timing"] = timing
    bindings = {os.path.relpath(predecessor_root / name, current_root): digest
                for name, digest in receipt["artifacts"].items()}
    bindings[os.path.relpath(original_path, current_root)] = sha(original_path)
    return {"status": "PASS", "result": result, "artifacts": bindings,
            "reference": {"root": str(predecessor_root), "node": node, "run_id": receipt["run_id"],
                          "receipt_sha256": sha(original_path), "receipt_path": str(original_path),
                          "original_native_timing": timing},
            "forecast_exception": exception,
            "scope": "same successful native node and original artifacts; failed cost gate excluded"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("node")
    args = parser.parse_args()
    print(json.dumps(verify(args.root, args.node)))
