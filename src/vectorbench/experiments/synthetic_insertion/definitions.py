"""Dagster assets: one static partition per finite fixture/configuration/history/control."""
import os
from pathlib import Path
import time

import dagster as dg

from vectorbench.experiments.synthetic_insertion.contracts import (canonical, effective_sources, load, read_ack, save, sha,
                             source_amendment, source_revision, verify_files)
from vectorbench.orchestration.synthetic_supervision import supervised

ROOT = Path(os.environ["STUDY_ROOT"]).resolve()
MANIFEST = load(ROOT / "manifest.json")
CELLS = MANIFEST["cells"]
KINDS = ("fixture", "tune", "base", "incremental", "fresh")
PARTITIONS = {kind: dg.StaticPartitionsDefinition([s["id"] for s in CELLS.values() if s["kind"] == kind])
              for kind in KINDS if any(s["kind"] == kind for s in CELLS.values())}


def native_ack(instance, key):
    receipt, ack = read_ack(ROOT, key)
    record = instance.get_run_by_id(ack["run_id"])
    if record is None or record.status != dg.DagsterRunStatus.SUCCESS:
        raise AssertionError("no successful native Dagster run for acknowledged result: " + key)
    if record.tags.get("study/key") != key or record.tags.get("study/campaign") != ROOT.name:
        raise AssertionError("native run belongs to another execution scope")
    expected_revision = source_revision(effective_sources(ROOT, MANIFEST))
    recorded_revision = record.tags.get("study/source_revision", source_revision(MANIFEST["sources"]))
    if receipt.get("source_revision", source_revision(MANIFEST["sources"])) != recorded_revision:
        raise AssertionError("artifact source revision differs from native run")
    if recorded_revision != expected_revision:
        amendment = source_amendment(ROOT)
        reuse = amendment.get("reused_completed", {}).get(key) if amendment else None
        ack_path = ROOT / load(ROOT / "ledger.json")[key]["ack"]
        if not reuse or reuse["run_id"] != ack["run_id"] or reuse["receipt_sha256"] != ack["receipt_sha256"] or reuse["ack_sha256"] != sha(ack_path):
            raise AssertionError("older-source result lacks explicit verified reuse disposition")
    return receipt


def execute(context, key):
    spec = CELLS[key]
    for dep in spec["deps"]:
        native_ack(context.instance, dep)
    ledger = load(ROOT / "ledger.json")
    entry = ledger[key]
    if entry["status"] == "PASS":
        raise dg.Failure("completed work must be skipped by admission, not materialized again")
    attempt = len(entry["attempts"]) + 1
    out = ROOT / "cells" / key.replace(":", "__") / f"attempt-{attempt}"
    out.mkdir(parents=True, exist_ok=False)
    entry["status"] = "RUNNING"
    entry["run_id"] = context.run_id
    entry["attempts"].append({"run_id": context.run_id, "out": str(out.relative_to(ROOT)), "started": time.time()})
    save(ROOT / "ledger.json", ledger)
    controller = load(ROOT / "controller.json")
    revision = source_revision(effective_sources(ROOT, MANIFEST))
    if context.run.tags.get("study/source_revision") != revision:
        raise dg.Failure("source revision changed after controller admission")
    save(out / "source-admission.json", {"revision": revision, "run_id": context.run_id})
    controller["out"] = str(out)
    save(ROOT / "controller.json", controller)
    remaining = controller["deadline"] - time.time() - 30
    if remaining <= 0:
        raise dg.Failure("stage cap/cleanup reserve exhausted")
    command = [MANIFEST["measurement_python"], "-u", "-m", "vectorbench.experiments.synthetic_insertion.worker", str(ROOT), key,
               str(out), "--run-id", context.run_id]
    supervision = supervised(command, out, min(remaining, MANIFEST["config"]["stage_seconds"] - 30))
    if supervision.get("returncode") != 0 or supervision.get("cleanup", {}).get("errors"):
        failure = {"key": key, "run_id": context.run_id, "out": str(out), "supervision": supervision}
        save(ROOT / "escalations" / (context.run_id + ".json"), failure)
        raise dg.Failure(description="workload failed or cleanup failed; no automatic retry",
                         metadata={"out": dg.MetadataValue.path(str(out)),
                                   "returncode": supervision.get("returncode", -1)})
    receipt = load(out / "receipt.json")
    if receipt["campaign"] != ROOT.name or receipt["key"] != key or receipt["run_id"] != context.run_id:
        raise dg.Failure("workload returned a receipt for a different scope or run")
    verify_files(ROOT, receipt)
    return dg.MaterializeResult(metadata={"out": dg.MetadataValue.path(str(out)),
        "receipt_sha256": sha(out / "receipt.json"), "elapsed_s": receipt["elapsed_s"],
        "campaign": ROOT.name, "key": key})


def dependencies(kind):
    grouped = {}
    for s in CELLS.values():
        if s["kind"] != kind:
            continue
        for dep in s["deps"]:
            parent_kind, parent_id = dep.split(":", 1)
            grouped.setdefault(parent_kind, {}).setdefault(parent_id, []).append(s["id"])
    return [dg.AssetDep(parent, partition_mapping=dg.StaticPartitionMapping(mapping))
            for parent, mapping in grouped.items()]


def asset_for(kind):
    @dg.asset(name=kind, partitions_def=PARTITIONS[kind], deps=dependencies(kind), group_name="insertion_study")
    def result(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        return execute(context, kind + ":" + context.partition_key)
    return result


assets = [asset_for(kind) for kind in PARTITIONS]


@dg.asset(deps=assets, group_name="insertion_study")
def summary(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return execute(context, "summary:all")


@dg.asset_check(asset=summary)
def complete_evidence() -> dg.AssetCheckResult:
    ledger = load(ROOT / "ledger.json")
    running = [v for k, v in ledger.items() if k == "summary:all" and v["status"] == "RUNNING"]
    if len(running) != 1:
        return dg.AssetCheckResult(passed=False, metadata={"reason": "summary attempt not running"})
    out = ROOT / running[0]["attempts"][-1]["out"]
    receipt = load(out / "receipt.json")
    return dg.AssetCheckResult(passed=receipt["result"]["counts"] == MANIFEST["expected"],
                               metadata={"expected": canonical(MANIFEST["expected"])})


defs = dg.Definitions(assets=[*assets, summary], asset_checks=[complete_evidence])
