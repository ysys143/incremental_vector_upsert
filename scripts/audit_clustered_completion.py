"""Independent G5 audit of the frozen clustered native campaign.

Run in the Dagster interpreter after the controller and guardian are terminal.
This audits selected-scope evidence; final G5 delivery is a separate act.
"""
import argparse
from collections import Counter
from pathlib import Path
import time

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.evidence.registry import Registry
from vectorbench.experiments.real_vectors.clustered.campaign import catalogue
from vectorbench.experiments.real_vectors.clustered.contract import validate_window
from vectorbench.experiments.real_vectors.clustered.adopt import (
    qualified_evidence_checks, verify_exception, verify_static)

EXPECTED = {"histories": 48, "incremental_measurements": 192, "fresh_builds": 120,
            "additional_ef_measurements": 168, "total_quality_conditions": 480,
            "evaluation_queries": 211200}
ROLES = {"inputs": 2, "prepared": 1, "calibration": 8, "tuning": 4, "tuned": 1,
         "smoke_initial": 4, "smoke_history": 8, "smoke_fresh": 4, "path_guard": 2,
         "cost_gate": 1, "initial": 12, "initialized": 1, "history_slot0": 12,
         "history_slot1": 12, "history_slot2": 12, "history_slot3": 12,
         "histories_done": 1, "fresh": 120, "report": 1}
EFS = {100, 128, 256, 512, 1000}
KNOWN_FAILED = {
    ("history_slot0:msmarco-postgres-s11-C1", "a5026b92-e418-4d43-beb2-a8697782512d"),
    ("history_slot0:msmarco-postgres-s29-M1", "2762bc40-6afe-4fb4-bae8-7f579d5d6db4"),
    ("history_slot0:msmarco-postgres-s11-C1", "764ffbde-980b-4688-978d-31231301dbd8"),
    ("history_slot1:msmarco-postgres-s47-C1", "10ab92a3-a133-4b26-a5be-4b2348b0451a"),
    ("fresh:wikipedia-postgres-s47-final-fresh-1", "8bf51ce4-59a2-4606-9773-010e1e9a6fe3"),
    ("fresh:wikipedia-qdrant-s47-initial-fresh-1", "7fa6b500-4ba1-4530-a04f-d38cb6c2c595"),
    ("report:all", "0b80bc1b-e1b5-4f60-8e08-527daac112ff"),
}
KNOWN_OUTCOMES = {
    "a5026b92-e418-4d43-beb2-a8697782512d": "failed",
    "2762bc40-6afe-4fb4-bae8-7f579d5d6db4": "failed",
    "764ffbde-980b-4688-978d-31231301dbd8": "stopped",
    "10ab92a3-a133-4b26-a5be-4b2348b0451a": "failed",
    "8bf51ce4-59a2-4606-9773-010e1e9a6fe3": "failed",
    "7fa6b500-4ba1-4530-a04f-d38cb6c2c595": "failed",
    "0b80bc1b-e1b5-4f60-8e08-527daac112ff": "stopped",
}


def _authorized_retry(root, manifest, auth, registry):
    """Recognize only the seven preserved, explicitly authorized -02 failures/stops."""
    retries = manifest.get("authorized_retries")
    if (not isinstance(retries, list) or len(retries) != 7
            or {(r.get("node"), r.get("prior_run_id")) for r in retries} != KNOWN_FAILED):
        raise AssertionError("exactly seven known retry authorizations required")
    for retry in retries:
        if (set(retry) != {"node", "prior_run_id", "reason", "user_text", "recorded_at"}
                or not isinstance(retry["reason"], str) or not retry["reason"].strip()
                or not isinstance(retry["user_text"], str) or not retry["user_text"].strip()
                or not auth["t0"] <= retry["recorded_at"] <= auth.get("execution_deadline", auth["deadline"])):
            raise AssertionError("retry authorization fields or execution clock differ")
        if (retry["prior_run_id"] == "10ab92a3-a133-4b26-a5be-4b2348b0451a"
                and retry["user_text"] != "디스크 검사 빼라고"):
            raise AssertionError("disk-wait retry lacks the user's exact instruction")
        if (retry["prior_run_id"] == "8bf51ce4-59a2-4606-9773-010e1e9a6fe3"
                and retry["user_text"] != "재개해."):
            raise AssertionError("interrupted fresh-build retry lacks the user's exact instruction")
        if (retry["prior_run_id"] == "7fa6b500-4ba1-4530-a04f-d38cb6c2c595"
                and retry["user_text"] != "아니 시발 왜 멈추는데\n장난하냐고"):
            raise AssertionError("completion retry lacks the user's correction")
    attempts = registry["attempts"]
    if (len(attempts) != 225 or len({(a["node"], a["run_id"]) for a in attempts}) != 225
            or {a["node"] for a in attempts} != set(manifest["nodes"])):
        raise AssertionError("retry must add exactly seven attempts to 218 nodes")
    failed = [a for a in attempts if a["outcome"] != "worker-completed"]
    if (len(failed) != 7 or {(a["node"], a["run_id"]) for a in failed} != KNOWN_FAILED
            or any(a["outcome"] != KNOWN_OUTCOMES[a["run_id"]] or a["ended"] is None
                   for a in failed)):
        raise AssertionError("seven prior failed/stopped attempts are not isolated")
    latest = {a["node"]: a for a in sorted(attempts, key=lambda a: a["started"])}
    if (len(latest) != 218 or any(a["outcome"] != "worker-completed" or a["ended"] is None
                                   for a in latest.values())
            or any(latest[r["node"]]["run_id"] == r["prior_run_id"] for r in retries)):
        raise AssertionError("218 latest native attempts are not completed")
    dispositions = registry["retry_dispositions"]
    if (len(dispositions) != 7 or
            {(d["node"], d["prior_run"], d["reason"]) for d in dispositions} !=
            {(r["node"], r["prior_run_id"], r["reason"]) for r in retries}):
        raise AssertionError("registry retry disposition differs from authorization")
    failed_by_id = {a["run_id"]: a for a in failed}
    disposition_by_id = {d["prior_run"]: d for d in dispositions}
    for retry in retries:
        prior = failed_by_id[retry["prior_run_id"]]
        disposition = disposition_by_id[retry["prior_run_id"]]
        if (not prior["ended"] <= disposition["created"] <= latest[retry["node"]]["started"]
                or not retry["recorded_at"] <= disposition["created"] <= auth.get("execution_deadline", auth["deadline"])):
            raise AssertionError("retry disposition is outside the authorized interval")
        cell = root / "cells" / retry["node"].replace(":", "__") / retry["prior_run_id"]
        cleanup = load(cell / "owner-cleanup.json")
        if retry["prior_run_id"] == "0b80bc1b-e1b5-4f60-8e08-527daac112ff":
            recovered = load(cell / "post-collision-cleanup.json")
            supervision = load(cell / "supervision.json")
            if (cleanup.get("status") != "CLEANUP_FAILED"
                    or "KeyError('resources')" not in cleanup.get("cleanup", {}).get("errors", [])
                    or recovered.get("status") != "CLEANED" or recovered.get("cleanup", {}).get("errors")
                    or sha(recovered["summary_archive"]) != recovered["summary_sha256"]
                    or (cell / "resources.json").exists() or supervision.get("returncode") != 0
                    or load(cell / "receipt.json").get("status") != "PASS"):
                raise AssertionError("report resource-name collision recovery is not preserved")
        elif cleanup.get("status") != "CLEANED" or cleanup.get("cleanup", {}).get("errors"):
            raise AssertionError("prior failed/stopped owner cleanup changed")
        if retry["prior_run_id"] == "8bf51ce4-59a2-4606-9773-010e1e9a6fe3":
            supervision = load(cell / "supervision.json")
            orphan_guardian = load(root / "prelaunch-revisions" / "r4" / "guardian-receipt.json")
            if (prior["outcome"] != "failed" or
                    supervision.get("first_error") != "DagsterExecutionInterruptedError()" or
                    (cell / "receipt.json").exists() or
                    orphan_guardian.get("status") != "STOPPED_INCOMPLETE" or
                    orphan_guardian.get("parent_alive") is not False or
                    orphan_guardian.get("deadline_exceeded") is not False or
                    sum(item.get("node") == retry["node"] for item in
                        orphan_guardian.get("cleanup", [])) != 1):
                raise AssertionError("interrupted fresh build is not the preserved orphan")
        failure_path = cell / "failure.json"
        if retry["prior_run_id"] == "7fa6b500-4ba1-4530-a04f-d38cb6c2c595":
            failure = load(failure_path)
            if (failure.get("status") != "FAIL" or
                    "next cell plus cleanup does not fit original deadline" not in failure.get("traceback", "") or
                    (cell / "receipt.json").exists() or (cell / "point").exists()):
                raise AssertionError("original-deadline refusal is not preserved")
        if failure_path.exists():
            failure = load(failure_path)
            if failure.get("status") != "FAIL":
                raise AssertionError("prior failure receipt changed")
        elif retry["prior_run_id"] == "10ab92a3-a133-4b26-a5be-4b2348b0451a":
            wait = load(cell / "resource-wait.json")
            if (wait.get("status") != "WAITING" or
                    "original 8 GiB growth budget" not in wait.get("reason", "") or
                    (cell / "point-0").exists()):
                raise AssertionError("disk-wait cancellation crossed a measurement/write boundary")
        elif retry["prior_run_id"] == "8bf51ce4-59a2-4606-9773-010e1e9a6fe3":
            pass  # The supervisor recorded the interruption before a failure receipt existed.
        elif prior["outcome"] != "stopped":
            raise AssertionError("failed native run lacks failure receipt")
    return retries, failed, latest


def _retry_source_revision(root, manifest, auth):
    """Bind the pre-retry manifest and source snapshot without widening science."""
    refs = manifest.get("retry_revision_refs")
    if not isinstance(refs, dict) or set(refs) != {"manifest", "source_hashes"}:
        raise AssertionError("two pre-retry snapshot references required")
    archive = root / "prelaunch-revisions" / "r1"
    docs = {}
    for name, expected_name in (("manifest", "manifest.json"),
                                ("source_hashes", "source-hashes.json")):
        ref = refs[name]
        if (set(ref) != {"path", "sha256"} or Path(ref["path"]).resolve() != archive / expected_name
                or sha(ref["path"]) != ref["sha256"]):
            raise AssertionError("pre-retry snapshot reference changed: " + name)
        docs[name] = load(ref["path"])
    old = docs["manifest"]
    if (old["execution_window_end"] != auth["deadline"]
            or old["authorization"] != manifest["authorization"]
            or any(old.get(key) != manifest.get(key) for key in
                   ("mode", "config", "matrix", "nodes", "source_refs", "source_manifest", "expected"))):
        raise AssertionError("pre-retry scientific manifest or clock changed")
    hashes = docs["source_hashes"]
    if not isinstance(hashes, dict) or hashes.get("sources") != old["sources"]:
        raise AssertionError("pre-retry source-hash inventory differs")
    allowed = {"src/vectorbench/backends/server.py", "src/vectorbench/orchestration/control.py",
               "src/vectorbench/orchestration/guardian.py",
               "src/vectorbench/experiments/real_vectors/clustered/diagnostics.py",
               "src/vectorbench/experiments/real_vectors/clustered/report.py",
               "src/vectorbench/orchestration/isolation.py",
               "src/vectorbench/evidence/contracts.py", "src/vectorbench/evidence/registry.py",
               "src/vectorbench/experiments/real_vectors/clustered/gates.py",
               "src/vectorbench/experiments/real_vectors/clustered/run.py",
               "src/vectorbench/experiments/real_vectors/clustered/contract.py",
               "src/vectorbench/experiments/real_vectors/clustered/adopt.py",
               "scripts/audit_clustered_completion.py", "scripts/finalize_clustered.py"}
    if set(old["sources"]) != set(manifest["sources"]):
        raise AssertionError("retry added or removed frozen source files")
    if {name for name in old["sources"] if old["sources"][name] != manifest["sources"][name]} - allowed:
        raise AssertionError("retry changed a scientific source")
    for name, digest in old["sources"].items():
        if sha(archive / "source" / name) != digest:
            raise AssertionError("pre-retry source snapshot changed: " + name)
    return refs


def _point(point, db, queries, *, files):
    if (point.get("status") != "PASS" or point.get("metric") != "ip"
            or point.get("queries") != queries or point.get("ann", {}).get("status") != "PASS"):
        raise AssertionError("unqualified IP ANN point or wrong query count")
    ann = point["ann"]
    if db == "postgres":
        if ann.get("seqscan") != "off" or ann.get("verified_queries", 0) < queries:
            raise AssertionError("PostgreSQL HNSW/seqscan guard missing")
    elif db == "qdrant":
        if ann.get("query_count", 0) != queries or ann.get("delta", {}).get("unfiltered_hnsw", 0) < queries:
            raise AssertionError("Qdrant HNSW telemetry missing")
        if any(value for key, value in ann["delta"].items() if key != "unfiltered_hnsw"):
            raise AssertionError("Qdrant non-HNSW fallback occurred")
    else:
        raise AssertionError("undeclared database")
    if files:
        raw = Path(point["selected_read"]) / "raw.npz"
        oracle = point["oracle"]
        if sha(raw) != point["raw_sha256"] or sha(oracle["path"]) != oracle["sha256"]:
            raise AssertionError("raw returned IDs or exact oracle changed")
        if sha(Path(oracle["path"]).with_suffix(".npz.receipt.json")) != oracle["receipt_sha256"]:
            raise AssertionError("oracle producer receipt changed")
    return queries


def audit_inventory(manifest, receipts, *, files=True):
    """Pure inventory checks; native success and ownership are audited by audit()."""
    nodes = manifest["nodes"]
    if manifest.get("mode") != "real-clustered-insertion" or len(nodes) != 218 or set(nodes) != set(receipts):
        raise AssertionError("frozen 218-node inventory incomplete")
    if Counter(spec["role"] for spec in nodes.values()) != Counter(ROLES):
        raise AssertionError("clustered role inventory differs")
    if ({key: manifest.get("expected", {}).get(key) for key in EXPECTED} != EXPECTED
            or {key: manifest.get("matrix", {}).get("expected", {}).get(key) for key in EXPECTED} != EXPECTED):
        raise AssertionError("approved scientific counts changed")
    if {node: {key: value for key, value in spec.items() if key != "native_adoption"}
            for node, spec in nodes.items()} != catalogue(manifest["matrix"]):
        raise AssertionError("frozen node catalogue differs from approved matrix")
    counts = {"histories": 0, "incremental_measurements": 0, "fresh_builds": 0,
              "additional_ef_measurements": 0, "total_quality_conditions": 0,
              "evaluation_queries": 0}
    history_ids = set()
    fresh_ids = set()
    for node, spec in nodes.items():
        value = receipts[node]["result"]
        action = spec["action"]
        role = spec["role"]
        if action == "prepare":
            if value.get("dataset") != spec["dataset"] or set(value.get("seeds", {})) != {"7", "11", "29", "47"}:
                raise AssertionError("G1 input family incomplete")
            if value.get("source") != manifest.get("source_refs", {}).get(spec["dataset"]):
                raise AssertionError("G1 prepared input is not bound to the frozen source")
        elif action == "candidate":
            points = value.get("points", [])
            combos = {(p["state"], p["ef"]) for p in points}
            wanted = {(state, ef) for state in ("C5:0", "C5:1", "C5:3", "C5:5", "fresh") for ef in EFS}
            if value.get("status") != "PASS" or len(points) != 25 or combos != wanted:
                raise AssertionError("G2 25-point candidate grid incomplete")
            for item in points:
                _point(item["point"], spec["db"], 200, files=files)
        elif action == "select":
            if (value.get("status") != "PASS" or len(value.get("candidates", [])) != 10
                    or value.get("ef") not in EFS):
                raise AssertionError("G2 tuning decision incomplete")
        elif role == "path_guard":
            positive = value.get("positive")
            negatives = value.get("negative_controls", [])
            if value.get("status") != "PASS" or len(negatives) != 3 or len({v.get("name") for v in negatives}) != 3:
                raise AssertionError("G3 negative path controls missing")
            if any(v.get("status") != "PASS" or not v.get("rejected") for v in negatives):
                raise AssertionError("G3 negative path control was not rejected")
            _point(positive, spec["db"], 4, files=files)
        elif role == "cost_gate":
            if value.get("status") != "PASS" or value.get("deadline") != manifest["execution_window_end"]:
                raise AssertionError("full remaining-cost gate did not pass")
            if manifest.get("forecast_exception"):
                if (value.get("forecast_status") != "BLOCK"
                        or value.get("admission_status") != "USER_AUTHORIZED_FORECAST_EXCEPTION"
                        or value.get("forecast_exception") != manifest["forecast_exception"]):
                    raise AssertionError("explicit BLOCK forecast exception was not preserved")
        elif action == "initial":
            if value.get("status") != "PASS" or not value.get("archive", {}).get("archive_sha"):
                raise AssertionError("initial snapshot missing")
            for key in ("before_restart", "after_restart"):
                _point(value[key], spec["db"], 8, files=files)
        elif action == "history":
            expected_steps = list(range(2 if spec["path"].endswith("1") else 6))
            if value.get("status") != "PASS" or value.get("steps") != expected_steps:
                raise AssertionError("history state sequence incomplete")
            if len(value.get("writes", [])) != len(expected_steps)-1 or sum(w.get("written", 0) for w in value["writes"]) != 10000:
                raise AssertionError("history logical waves or total writes differ")
            if any(w.get("status") != "ACKNOWLEDGED" for w in value["writes"]):
                raise AssertionError("unacknowledged history write")
            points = value.get("points", [])
            if len(points) != len(expected_steps):
                raise AssertionError("history checkpoint missing")
            for index, point in enumerate(points):
                expected_queries = 600 if index == len(points)-1 and role.startswith("history_slot") else 200
                measured = _point(point, spec["db"], expected_queries, files=files)
                if role.startswith("history_slot"):
                    counts["incremental_measurements"] += 1
                    counts["total_quality_conditions"] += 1
                    counts["evaluation_queries"] += measured
            extra = value.get("ef_points", [])
            if role.startswith("history_slot"):
                if len(extra) != 2 or len({p.get("ef") for p in extra}) != 2:
                    raise AssertionError("incremental additional ef pair missing")
                for item in extra:
                    if item["ef"] not in EFS:
                        raise AssertionError("undeclared ef")
                    counts["evaluation_queries"] += _point(item["point"], spec["db"], 600, files=files)
                    counts["additional_ef_measurements"] += 1
                    counts["total_quality_conditions"] += 1
                counts["histories"] += 1
                history_ids.add((spec["dataset"], spec["db"], spec["seed"], spec["path"]))
            elif extra:
                raise AssertionError("smoke history mixed with evaluation extras")
        elif action == "fresh":
            state = spec["state"]
            primary_queries = 200 if ":step" in state or role == "smoke_fresh" else 600
            if value.get("status") != "PASS" or value.get("state") != state:
                raise AssertionError("fresh state differs from frozen specification")
            measured = _point(value["point"], spec["db"], primary_queries, files=files)
            extra = value.get("ef_points", [])
            if role == "fresh":
                counts["fresh_builds"] += 1
                counts["total_quality_conditions"] += 1
                counts["evaluation_queries"] += measured
                fresh_ids.add((spec["dataset"], spec["db"], spec["seed"], state, spec["repeat"]))
                expected_extra = 2 if state == "final" else 0
                if len(extra) != expected_extra or len({p.get("ef") for p in extra}) != expected_extra:
                    raise AssertionError("fresh additional ef inventory differs")
                for item in extra:
                    if item["ef"] not in EFS:
                        raise AssertionError("undeclared ef")
                    counts["evaluation_queries"] += _point(item["point"], spec["db"], 600, files=files)
                    counts["additional_ef_measurements"] += 1
                    counts["total_quality_conditions"] += 1
            elif extra:
                raise AssertionError("smoke fresh mixed with evaluation extras")
        elif action == "report":
            if value.get("status") != "PASS" or value.get("counts") != EXPECTED:
                raise AssertionError("report completion counts differ")
            if files and (not Path(value["report"]).is_file() or not Path(value["endpoints"]).is_file()
                          or not Path(value["contrasts"]).is_file()):
                raise AssertionError("report delivery artifacts missing")
        elif action == "barrier":
            if value.get("status") != "PASS":
                raise AssertionError("native barrier missing")
        else:
            raise AssertionError("undeclared clustered node action")
    matrix = manifest["matrix"]
    if history_ids != {(v["dataset"], v["db"], v["seed"], v["name"]) for v in matrix["histories"]}:
        raise AssertionError("matrix history conditions missing or duplicated")
    expected_fresh = {(v["group"].split("-")[0], v["group"].split("-")[1],
                       int(v["group"].split("-s")[1]), v["state"], v["repeat"]) for v in matrix["fresh_builds"]}
    if fresh_ids != expected_fresh:
        raise AssertionError("matrix fresh conditions missing or duplicated")
    if counts != EXPECTED:
        raise AssertionError("evaluation counts differ: " + str(counts))
    return counts


def _source_freeze(root, manifest):
    for name, digest in manifest["sources"].items():
        if sha(root / "source" / name) != digest or sha(name) != digest:
            raise AssertionError("frozen code differs: " + name)
    source = manifest["source_manifest"]
    if sha(source["path"]) != source["sha256"]:
        raise AssertionError("source manifest changed")
    if set(manifest["source_refs"]) != {"msmarco", "wikipedia"}:
        raise AssertionError("source dataset inventory changed")
    for dataset, ref in manifest["source_refs"].items():
        if ref.get("dataset") != dataset or ref.get("data_kind") != "real-precomputed" or ref.get("metric") != "ip":
            raise AssertionError("undeclared public source")
        for key in ("base", "query"):
            if sha(ref[key + "_path"]) != ref[key + "_sha256"]:
                raise AssertionError("public source bytes changed")
        producer = Path(ref["base_path"]).parent / "receipt.json"
        if sha(producer) != ref["receipt_sha256"] or load(producer).get("status") != "PASS":
            raise AssertionError("native source producer receipt changed")


def assert_db_exclusive(windows, label):
    """A DB lease and native check must not overlap any other heavy task."""
    for start, end, kind, node in windows:
        if end < start:
            raise AssertionError(label + " negative worker interval: " + node)
        if kind == "db" and any(other != node and a < end and b > start for a, b, _, other in windows):
            raise AssertionError(label + " DB interval overlapped another heavy worker: " + node)


def audit(root):
    import dagster as dg
    from vectorbench.orchestration.engine import native_receipt

    root = Path(root).resolve()
    manifest = load(root / "manifest.json")
    auth = validate_window(manifest)
    if manifest["execution_window_end"] != auth["deadline"] or auth["deadline"]-auth["t0"] != 43200:
        raise AssertionError("fixed twelve-hour clock changed")
    execution_deadline = auth.get("execution_deadline", auth["deadline"])
    policy = manifest.get("completion_policy")
    if (auth.get("original_window_status") != "MISSED"
            or not isinstance(policy, dict) or policy.get("mode") != "FINISH_APPROVED_SCOPE"
            or policy.get("original_deadline") != auth["deadline"]
            or policy.get("execution_deadline") != execution_deadline
            or execution_deadline <= auth["deadline"]
            or policy.get("user_text") != "아니 시발 왜 멈추는데\n장난하냐고"
            or policy.get("recorded_at", 0) <= auth["deadline"]):
        raise AssertionError("approved post-deadline completion policy missing")
    disk_policy = manifest.get("disk_policy")
    if (not isinstance(disk_policy, dict) or disk_policy.get("mode") != "OBSERVE_ONLY"
            or disk_policy.get("user_text") != "디스크 검사 빼라고"
            or not auth["t0"] <= disk_policy.get("recorded_at", float("inf")) <= auth["deadline"]):
        raise AssertionError("user-directed observe-only disk policy missing")
    controller = load(root / "controller.json")
    if controller.get("status") != "COMPLETE_SELECTED_SCOPE" or not controller.get("finished") or controller.get("nodes") != 218:
        raise AssertionError("native controller has not completed 218 selected nodes")
    if controller["finished_at"] > execution_deadline or controller["deadline"] > execution_deadline:
        raise AssertionError("native completion exceeded approved execution watchdog")
    if controller["finished_at"] <= auth["deadline"]:
        raise AssertionError("missed original twelve-hour target was not recorded truthfully")
    selected = load(root / "selected-scope-receipt.json")
    if selected != {"status": "COMPLETE_SELECTED_SCOPE", "scope": manifest["mode"], "nodes": 218}:
        raise AssertionError("selected scope receipt differs")
    guardian = load(root / "guardian-receipt.json")
    if guardian.get("status") != "RECONCILED" or len(guardian.get("cleanup", [])) != 225:
        raise AssertionError("owned resource cleanup incomplete")
    if any(item.get("cleanup", {}).get("errors") for item in guardian["cleanup"]):
        raise AssertionError("owned cleanup error remains")
    registry = Registry(root / "admission.sqlite").snapshot()
    attempts = registry["attempts"]
    if registry["leases"]:
        raise AssertionError("active admitted task remains")
    retries, failed_attempts, latest = _authorized_retry(root, manifest, auth, registry)
    if (Counter((item.get("node") for item in guardian["cleanup"])) !=
            Counter(a["node"] for a in attempts)):
        raise AssertionError("guardian did not reconcile every original and retry attempt")
    _source_freeze(root, manifest)
    revision_refs = _retry_source_revision(root, manifest, auth)
    adopted = {node: spec["native_adoption"] for node, spec in manifest["nodes"].items()
               if "native_adoption" in spec}
    if len(adopted) != 34 or "cost_gate:all" in adopted:
        raise AssertionError("successful-origin adoption inventory differs from 34")
    predecessors = {Path(ref["root"]).resolve() for ref in adopted.values()}
    if len(predecessors) != 1:
        raise AssertionError("adopted origins span different predecessors")
    predecessor = predecessors.pop()
    exception = verify_exception(manifest, predecessor)
    old_registry = Registry(predecessor / "admission.sqlite").snapshot()
    if {a["node"] for a in old_registry["attempts"] if a["outcome"] == "worker-completed"} != set(adopted):
        raise AssertionError("adopted set differs from all 34 predecessor successes")
    # One static check covers the full source-equivalence allowlist; each remaining
    # node is still checked against its exact original scientific specification.
    verify_static(root, next(iter(adopted)))
    old_manifest = load(predecessor / "manifest.json")
    for node, ref in adopted.items():
        if {key: value for key, value in manifest["nodes"][node].items() if key != "native_adoption"} != old_manifest["nodes"][node]:
            raise AssertionError("adopted node specification changed: " + node)
    g0 = load(root / "g0-source-gate" / "receipt.json")
    if (g0.get("status") != "PASS" or g0.get("worker_started_after_mutation") is not False
            or g0.get("positive_run") == g0.get("intentional_failed_run")
            or g0.get("finished_at", float("inf")) > auth["deadline"]
            or g0.get("deadline") != auth["deadline"]
            or sha(root / "check_g0_source.py") != g0.get("script_sha256")):
        raise AssertionError("isolated G0 source-mutation native gate missing")
    receipts, checks, check_ends = {}, [], {}
    with dg.DagsterInstance.from_config(str(root / "dagster-home")) as instance:
        runs = instance.get_runs(filters=dg.RunsFilter(tags={"realstudy/campaign": root.name}))
        if not runs:
            raise AssertionError("native run inventory missing")
        failed_runs = {r.run_id for r in runs if r.status in
                       {dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED}}
        if failed_runs != {retry["prior_run_id"] for retry in retries} or any(
                r.status not in {dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE,
                                 dg.DagsterRunStatus.CANCELED}
                for r in runs):
            raise AssertionError("native run inventory contains an unauthorized failure or unfinished run")
        for node, spec in manifest["nodes"].items():
            receipt = native_receipt(root, node, instance)  # deep artifact hashes + materialization hash
            events = [e for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
            evaluations = [e.asset_check_evaluation for e in events]
            target = qualified_evidence_checks(evaluations, node, spec, receipt["run_id"])
            cell = root / "cells" / node.replace(":", "__") / receipt["run_id"]
            owner_cleanup = load(cell / "owner-cleanup.json")
            if owner_cleanup.get("status") != "CLEANED" or owner_cleanup.get("cleanup", {}).get("errors"):
                raise AssertionError("cell owner cleanup failed: " + node)
            if receipt["resource"] != spec["resource"] or receipt["finished"] > execution_deadline:
                raise AssertionError("resource class or worker deadline differs: " + node)
            receipts[node] = receipt
            check_ends[node] = max(e.timestamp for e in events if e.asset_check_evaluation == target)
            checks.append({"node": node, "run_id": receipt["run_id"],
                           "asset_key": list(target.asset_key.path), "check_name": target.check_name,
                           "partition": target.partition})
        native_run_ids = {r.run_id for r in runs if r.status == dg.DagsterRunStatus.SUCCESS}
        if (native_run_ids != {receipt["run_id"] for receipt in receipts.values()}
                or {r.run_id for r in runs} != native_run_ids | {r["prior_run_id"] for r in retries}
                or any(receipts[node]["run_id"] != attempt["run_id"] for node, attempt in latest.items())):
            raise AssertionError("native success runs differ from latest selected receipt run IDs")
    old_checks, old_receipts = {}, {}
    with dg.DagsterInstance.from_config(str(predecessor / "dagster-home")) as old_instance:
        for node, ref in adopted.items():
            original = native_receipt(predecessor, node, old_instance)
            old_receipts[node] = original
            old_cell = predecessor / "cells" / node.replace(":", "__") / original["run_id"]
            if original["run_id"] != ref["run_id"] or sha(old_cell / "receipt.json") != ref["receipt_sha256"]:
                raise AssertionError("adopted predecessor receipt differs: " + node)
            old_event_records = [e for e in old_instance.all_logs(original["run_id"])
                                 if e.asset_check_evaluation]
            old_events = [e.asset_check_evaluation for e in old_event_records]
            old_target = qualified_evidence_checks(old_events, node, old_manifest["nodes"][node], original["run_id"])
            old_checks[node] = max(e.timestamp for e in old_event_records if e.asset_check_evaluation == old_target)
            cell = root / "cells" / node.replace(":", "__") / receipts[node]["run_id"]
            proof = load(cell / "adoption-proof.json")
            reference = proof["reference"]
            timing = receipts[node]["result"].get("original_native_timing")
            if (reference.get("run_id") != original["run_id"]
                    or reference.get("receipt_sha256") != ref["receipt_sha256"]
                    or reference.get("original_native_timing") != timing
                    or timing.get("started_at") != original["started"]
                    or timing.get("worker_finished_at") != original["finished"]
                    or timing.get("elapsed_s") != original["finished"]-original["lease_acquired"]
                    or timing.get("wall_elapsed_s") != original["finished"]-original["started"]
                    or proof.get("forecast_exception", {}).get("sha256") != exception["sha256"]):
                raise AssertionError("adopted original timing, receipt or exception differs: " + node)
        old_runs = old_instance.get_runs(filters=dg.RunsFilter(tags={"realstudy/campaign": predecessor.name}))
        old_success = {run.run_id for run in old_runs if run.status == dg.DagsterRunStatus.SUCCESS}
        old_failed = {run.run_id for run in old_runs if run.status in
                      {dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED}}
        failed_cost = {a["run_id"] for a in old_registry["attempts"] if a["node"] == "cost_gate:all"}
        if old_success != {receipt["run_id"] for receipt in old_receipts.values()} or old_failed != failed_cost:
            raise AssertionError("predecessor native runs differ from 34 origins and failed cost gate")
    windows = [(receipt["lease_acquired"], max(latest[node]["ended"], check_ends[node]),
                manifest["nodes"][node]["resource"], node) for node, receipt in receipts.items()]
    windows.extend((a["started"], a["ended"], manifest["nodes"][a["node"]]["resource"],
                    a["node"] + ":failed-prior:" + a["run_id"])
                   for a in failed_attempts)
    assert_db_exclusive(windows, "successor")
    old_attempts = {a["node"]: a for a in old_registry["attempts"] if a["node"] in adopted}
    old_windows = [(receipt["lease_acquired"], max(old_attempts[node]["ended"], old_checks[node]),
                    old_manifest["nodes"][node]["resource"], node)
                   for node, receipt in old_receipts.items()]
    assert_db_exclusive(old_windows, "adopted predecessor")
    counts = audit_inventory(manifest, receipts)
    report = receipts["report:all"]
    if report["finished"] > execution_deadline or report["result"].get("finished_at", float("inf")) > execution_deadline:
        raise AssertionError("report exceeded approved execution watchdog")
    return {"status": "PASS", "audited_at": time.time(), "t0": auth["t0"],
            "deadline": auth["deadline"], "execution_deadline": execution_deadline,
            "original_12h_target": {"status": "MISSED", "deadline": auth["deadline"],
                                    "actual_completed_at": controller["finished_at"]},
            "completion_policy": policy, "native_nodes": len(receipts),
            "native_runs": len(native_run_ids), "native_checks": checks,
            "adopted_native_origins": len(adopted), "forecast_status": "BLOCK",
            "admission_status": "USER_AUTHORIZED_FORECAST_EXCEPTION",
            "forecast_exception": exception, "authorized_retries": retries,
            "disk_policy": disk_policy,
            "retry_revision_refs": revision_refs,
            "counts": counts, "report": report["result"]["report"],
            "scope": "clustered selected-scope G5 independent audit; final delivery is separate"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    root = parser.parse_args().root.resolve()
    result = audit(root)
    target = root / "independent-completion-audit.json"
    if target.exists():
        raise FileExistsError("independent G5 audit is immutable; inspect existing result")
    save(target, result)
    print({key: value for key, value in result.items() if key != "native_checks"})
