"""Partitioned, dependency-aware Dagster assets; no sequential materialization launcher."""
import hashlib
import os
from pathlib import Path
import subprocess
import time

import dagster as dg

from vectorbench.orchestration.synthetic_supervision import supervised
from vectorbench.orchestration.synthetic_cleanup import release_journal
from vectorbench.evidence.contracts import Config, canonical, load, save, sha
from vectorbench.evidence.registry import Registry


def specification_hash(spec):
    return hashlib.sha256(canonical(spec).encode()).hexdigest()


def validate_receipt(root, node, run_id, deep=True):
    manifest = load(root / "manifest.json")
    out = root / "cells" / node.replace(":", "__") / run_id
    receipt = load(out / "receipt.json")
    if receipt["campaign"] != root.name or receipt["node"] != node or receipt["run_id"] != run_id or receipt["status"] != "PASS":
        raise AssertionError("worker receipt scope differs from its native consumer")
    if receipt["spec_hash"] != specification_hash(manifest["nodes"][node]):
        raise AssertionError("cell scientific input specification changed")
    if receipt["mode"] != manifest["mode"]:
        raise AssertionError("fixture/scientific evidence cannot be mixed")
    for name, expected in (receipt["artifacts"].items() if deep else []):
        if sha(root / name) != expected:
            raise AssertionError("changed or missing worker evidence: " + name)
    return receipt, out


def native_receipt(root, node, instance, current_run=None, deep=True):
    attempts = [v for v in Registry(root / "admission.sqlite").snapshot()["attempts"] if v["node"] == node]
    if not attempts:
        raise AssertionError("dependency has no admitted native attempt: " + node)
    prior = max(attempts, key=lambda x: x["started"])
    if prior["outcome"] != "worker-completed":
        raise AssertionError("dependency worker is not complete: " + node)
    record = instance.get_run_by_id(prior["run_id"])
    if record is None or (record.status != dg.DagsterRunStatus.SUCCESS and record.run_id != current_run):
        raise AssertionError("dependency has no native success: " + node)
    if record.tags.get("realstudy/campaign") != root.name:
        raise AssertionError("dependency belongs to another campaign")
    receipt, out = validate_receipt(root, node, prior["run_id"], deep=deep)
    role, partition = node.split(":", 1)
    matches = []
    for entry in instance.all_logs(prior["run_id"]):
        materialization = entry.asset_materialization
        if materialization is not None and materialization.asset_key == dg.AssetKey(role):
            if materialization.partition in {partition, None}:
                matches.append(materialization)
    if len(matches) != 1 or matches[0].metadata["receipt_sha256"].value != sha(out / "receipt.json"):
        raise AssertionError("worker receipt differs from its native materialization")
    return receipt


def build(root):
    root = Path(root).resolve()
    manifest = load(root / "manifest.json")
    nodes = manifest["nodes"]
    roles = sorted({s["role"] for s in nodes.values()})
    singleton = {role for role in roles if all(s.get("singleton") for s in nodes.values() if s["role"] == role)}
    partitions = {role: dg.StaticPartitionsDefinition([s["id"] for s in nodes.values() if s["role"] == role])
                  for role in roles if role not in singleton}

    def dependency_defs(role):
        grouped = {}
        for spec in nodes.values():
            if spec["role"] != role:
                continue
            for dependency in spec["deps"]:
                parent_role, parent_id = dependency.split(":", 1)
                grouped.setdefault(parent_role, {}).setdefault(parent_id, []).append(spec["id"])
        return [dg.AssetDep(parent, partition_mapping=dg.StaticPartitionMapping(mapping))
                if parent not in singleton and role not in singleton else dg.AssetDep(parent)
                for parent, mapping in grouped.items()]

    def execute(context, node):
        spec = nodes[node]
        for name, expected in manifest["sources"].items():
            if sha(name) != expected:
                raise dg.Failure("frozen workload sources changed: " + name)
        context.instance.add_run_tags(context.run_id, {"realstudy/campaign": root.name})
        dependencies = []
        for dependency in spec["deps"]:
            parent = native_receipt(root, dependency, context.instance, current_run=context.run_id, deep=False)
            path = root / "cells" / dependency.replace(":", "__") / parent["run_id"] / "receipt.json"
            dependencies.append({"node": dependency, "run_id": parent["run_id"], "sha256": sha(path)})
        registry = Registry(root / "admission.sqlite")
        if "space_baseline_free" in manifest:
            registry.inherit_baseline(manifest["space_baseline_free"])
        registry.claim(node, context.run_id, specification_hash(spec))
        out = root / "cells" / node.replace(":", "__") / context.run_id
        out.mkdir(parents=True, exist_ok=False)
        deadline = manifest.get("deadlines", {}).get(spec["phase"], manifest.get("execution_window_end"))
        if deadline is None:
            deadline = registry.deadline(spec["phase"])
        save(out / "admission.json", {"node": node, "run_id": context.run_id,
             "spec_hash": specification_hash(spec), "mode": manifest["mode"], "deadline": deadline,
             "dependencies": dependencies})
        remaining = deadline - time.time() - 30
        if remaining <= 0:
            registry.finish(node, context.run_id, "stopped")
            raise dg.Failure("original stage deadline/cleanup reserve exhausted")
        env_names = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")
        previous = {k: os.environ.get(k) for k in env_names}
        save(out / "watch.json", {"finished": False, "node": node, "run_id": context.run_id})
        watch_log = (out / "watchdog.log").open("a")
        watcher = subprocess.Popen([manifest["measurement_python"], "-m", "vectorbench.orchestration.watchdog",
            str(root), str(out), "--parent", str(os.getpid()), "--node", node,
            "--run-id", context.run_id, "--deadline", str(deadline)],
            stdout=watch_log, stderr=watch_log, start_new_session=True)
        try:
            for name in env_names:
                os.environ[name] = "1"
            result = supervised([manifest["measurement_python"], "-u", "-m", "vectorbench.orchestration.task",
                str(root), node, context.run_id], out, min(remaining, manifest.get("task_seconds", Config().stage_seconds) - 30))
        except BaseException:
            registry.finish(node, context.run_id, "failed")
            raise
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            try:
                cleanup = release_journal(out)
            except Exception as exc:
                cleanup = {"errors": [repr(exc)]}
            save(out / "owner-cleanup.json", {"cleanup": cleanup, "finished_at": time.time(),
                 "status": "CLEANUP_FAILED" if cleanup.get("errors") else "CLEANED"})
            save(out / "watch.json", {"finished": not bool(cleanup.get("errors")), "node": node, "run_id": context.run_id})
            watcher.wait(timeout=10)
            watch_log.close()
        if result.get("returncode") != 0 or result.get("cleanup", {}).get("errors") or cleanup.get("errors"):
            registry.finish(node, context.run_id, "failed")
            save(root / "escalations" / (context.run_id + "-" + spec["id"] + ".json"),
                 {"node": node, "run_id": context.run_id, "out": str(out), "supervision": result})
            raise dg.Failure("worker or independent cleanup failed; no automatic retry")
        try:
            receipt, _ = validate_receipt(root, node, context.run_id)
        except BaseException:
            registry.finish(node, context.run_id, "failed")
            raise
        registry.finish(node, context.run_id, "worker-completed", str((out / "receipt.json").relative_to(root)), retain_lease=True)
        return dg.MaterializeResult(metadata={"receipt_sha256": sha(out / "receipt.json"),
            "node": node, "scope": manifest["mode"], "out": dg.MetadataValue.path(str(out))})

    assets, checks = [], []
    for role in roles:
        specs = [v for v in nodes.values() if v["role"] == role]
        resources = {v["resource"] for v in specs}
        if len(resources) != 1:
            raise ValueError("one asset role cannot mix resource classes")
        if role in singleton and (len(specs) != 1 or specs[0]["id"] != "all"):
            raise ValueError("singleton roles require exactly role:all")

        def factory(role):
            @dg.asset(name=role, deps=dependency_defs(role), partitions_def=partitions.get(role),
                group_name="real_vector_study", pool="realstudy_" + next(iter(resources)),
                # Initial static partitions already exist when the sensor first evaluates.
                # eager()/on_missing() alone wait for newly-missing events after that point.
                # Persistent missing + failure exclusion admits initial work without retries.
                automation_condition=(dg.AutomationCondition.missing()
                    & ~dg.AutomationCondition.in_progress()
                    & ~dg.AutomationCondition.execution_failed()
                    & ~dg.AutomationCondition.any_deps_missing()
                    & ~dg.AutomationCondition.any_deps_in_progress()
                    & dg.AutomationCondition.all_deps_blocking_checks_passed()))
            def asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                partition = "all" if role in singleton else context.partition_key
                return execute(context, role + ":" + partition)

            @dg.asset_check(asset=asset, name="evidence", blocking=True, partitions_def=partitions.get(role))
            def check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
                partition = "all" if role in singleton else context.op_execution_context.partition_key
                node = role + ":" + partition
                run_id = context.op_execution_context.run_id
                try:
                    if manifest["mode"] == "orchestration-fixture" and nodes[node].get("check_delay_seconds"):
                        check_started = time.time()
                        time.sleep(nodes[node]["check_delay_seconds"])
                        out = root / "cells" / node.replace(":", "__") / run_id
                        save(out / "check-window.json", {"started": check_started, "finished": time.time(),
                             "leases": Registry(root / "admission.sqlite").snapshot()["leases"]})
                    validate_receipt(root, node, run_id)
                finally:
                    Registry(root / "admission.sqlite").release_checked(node, run_id)
                return dg.AssetCheckResult(passed=True, metadata={"mode": manifest["mode"]})
            return asset, check

        asset, check = factory(role)
        assets.append(asset); checks.append(check)
    sensor = dg.AutomationConditionSensorDefinition("real_study_ready", target=dg.AssetSelection.assets(*assets),
        default_status=dg.DefaultSensorStatus.RUNNING, minimum_interval_seconds=10,
        run_tags={"realstudy/campaign": root.name, "dagster/max_runtime": str(manifest.get("task_seconds", Config().stage_seconds))})
    return dg.Definitions(assets=assets, asset_checks=checks, sensors=[sensor])


if os.environ.get("REAL_STUDY_ROOT"):
    defs = build(os.environ["REAL_STUDY_ROOT"])
