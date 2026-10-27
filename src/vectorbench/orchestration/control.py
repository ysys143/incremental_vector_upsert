"""Finite native Dagster queue/automation lifecycle; never materialize cells in a loop."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

import dagster as dg

from vectorbench.orchestration.synthetic_supervision import kill_group
from vectorbench.evidence.contracts import load, save
from vectorbench.orchestration.engine import native_receipt
from vectorbench.evidence.registry import Registry


def authorized_retry_failures(root, manifest):
    """Recognize only explicitly reconciled failed attempts approved for retry."""
    registry = Registry(root / 'admission.sqlite')
    attempts = {(a['node'], a['run_id']): a for a in registry.snapshot()['attempts']}
    approved = set()
    with registry.connection() as connection:
        for approval in manifest.get('authorized_retries', []):
            key = (approval['node'], approval['prior_run_id'])
            prior = attempts.get(key)
            disposition = connection.execute('SELECT reason FROM retry_dispositions WHERE node=? AND prior_run=?', key).fetchone()
            if (prior is None or prior['outcome'] not in {'failed', 'stopped'} or prior['ended'] is None
                    or disposition is None or disposition['reason'] != approval['reason']
                    or not approval.get('user_text', '').strip() or key[0] not in manifest['nodes']):
                raise AssertionError('retry requires the exact reconciled failure and explicit disposition')
            if key[1] in approved: raise AssertionError('duplicate retry approval')
            approved.add(key[1])
    return approved


def run(root, seconds=14400):
    root = Path(root).resolve()
    manifest = load(root / "manifest.json")
    maximum = 43200 if manifest.get("mode") in {"real-twelve-hour", "real-clustered-insertion"} else 14400
    if not 0 < seconds <= maximum:
        raise ValueError("controller cap exceeds its declared scope")
    if manifest.get("mode") == "real-twelve-hour":
        from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window
        validate_window(manifest)
    elif manifest.get("mode") == "real-clustered-insertion":
        from vectorbench.experiments.real_vectors.clustered.contract import validate_window
        validate_window(manifest)
    home = root / "dagster-home"
    home.mkdir(exist_ok=True)
    config = home / "dagster.yaml"
    expected = ("telemetry:\n  enabled: false\nrun_monitoring:\n  enabled: true\n"
                f"  max_runtime_seconds: {manifest.get('task_seconds', 14400)}\n  poll_interval_seconds: 10\n"
                "concurrency:\n  runs:\n    max_concurrent_runs: 2\n")
    if config.exists() and config.read_text() != expected:
        raise AssertionError("existing native instance config differs; do not overwrite it")
    config.write_text(expected)
    environment = {**os.environ, "DAGSTER_HOME": str(home), "REAL_STUDY_ROOT": str(root)}
    python = Path(sys.executable)
    daemon_bin = python.parent / "dagster-daemon"
    dagster_bin = python.parent / "dagster"
    for name, limit in [("realstudy_cpu", 2), ("realstudy_io", 2), ("realstudy_db", 1)]:
        p = subprocess.run([str(dagster_bin), "instance", "concurrency", "set", name, str(limit)],
                           env=environment, capture_output=True, text=True, timeout=30)
        if p.returncode:
            raise RuntimeError("native concurrency setup failed: " + p.stderr[-1000:])
    if (root / "controller.json").exists():
        previous = load(root / "controller.json")
        if previous.get("status") == "COMPLETE_SELECTED_SCOPE":
            with dg.DagsterInstance.from_config(str(home)) as instance:
                for node in manifest["nodes"]:
                    native_receipt(root, node, instance)
            return {"status": "SKIPPED_VERIFIED_COMPLETE", "scope": manifest["mode"]}
        if not previous.get("finished"):
            raise AssertionError("prior controller is not reconciled; no second owner")
    window_end=manifest.get('completion_policy',{}).get('execution_deadline',manifest.get("execution_window_end",float('inf')))
    deadline = min(time.time() + seconds, window_end)
    if deadline <= time.time():
        raise TimeoutError("original declared execution window already expired")
    state = {"pid": os.getpid(), "started": time.time(), "deadline": deadline,
             "finished": False, "status": "RUNNING", "scope": manifest["mode"]}
    log = (root / "daemon.log").open("a")
    daemon = subprocess.Popen([str(daemon_bin), "run", "-m", "vectorbench.orchestration.engine", "--log-level", "warning"],
                              env=environment, stdout=log, stderr=log, start_new_session=True)
    state["daemon_pid"] = daemon.pid
    save(root / "controller.json", state)
    guardian_log = (root / "guardian.log").open("a")
    guardian = subprocess.Popen([manifest["measurement_python"], "-m", "vectorbench.orchestration.guardian", str(root)],
        stdout=guardian_log, stderr=guardian_log, start_new_session=True)
    result = None
    try:
        with dg.DagsterInstance.from_config(str(home)) as instance:
            approved_failures = authorized_retry_failures(root, manifest)
            seen = None
            verified_success = set()
            while True:
                if daemon.poll() is not None:
                    raise RuntimeError("native automation daemon exited; inspect daemon.log")
                native_failures = [r.run_id for r in instance.get_runs(
                    filters=dg.RunsFilter(tags={"realstudy/campaign": root.name},
                        statuses=[dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED]))
                    if r.run_id not in approved_failures]
                if native_failures:
                    raise RuntimeError("native run failed, including pre-admission failures; no automatic retry: "
                                       + ",".join(native_failures))
                attempts = Registry(root / "admission.sqlite").snapshot()["attempts"]
                latest = {a["node"]: a for a in sorted(attempts, key=lambda a: a["started"])}
                complete, running, failed = [], [], []
                for attempt in latest.values():
                    key = (attempt["node"], attempt["run_id"])
                    if key in verified_success:
                        complete.append(attempt["node"])
                        continue
                    native = instance.get_run_by_id(attempt["run_id"])
                    if native is None:
                        raise AssertionError("admission has no native run")
                    if native.status == dg.DagsterRunStatus.SUCCESS:
                        native_receipt(root, attempt["node"], instance, deep=False)
                        evaluations = [e.asset_check_evaluation for e in instance.all_logs(native.run_id) if e.asset_check_evaluation]
                        if not evaluations or not all(e.passed for e in evaluations):
                            raise AssertionError("native required checks are missing or failed")
                        complete.append(attempt["node"])
                        verified_success.add(key)
                    elif native.status in {dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED}:
                        if native.run_id not in approved_failures:
                            failed.append(attempt["node"])
                    else:
                        running.append(attempt["node"])
                counts = (len(set(complete)), len(running), len(failed))
                if counts != seen:
                    state.update(completed=counts[0], running=counts[1], failed=counts[2], selected=len(manifest["nodes"]))
                    save(root / "controller.json", state)
                    seen = counts
                if failed:
                    raise RuntimeError("failed selected nodes; no automatic retry: " + ",".join(failed))
                if set(complete) == set(manifest["nodes"]):
                    # No selected worker remains: final deep verification cannot overlap DB work.
                    if Registry(root / "admission.sqlite").snapshot()["leases"]:
                        raise AssertionError("completed native runs retained resource leases")
                    for node in manifest["nodes"]:
                        native_receipt(root, node, instance)
                    result = {"status": "COMPLETE_SELECTED_SCOPE", "scope": manifest["mode"], "nodes": len(complete)}
                    break
                if time.time() >= state["deadline"]:
                    raise TimeoutError("original controller deadline expired")
                if (root / "daemon.log").stat().st_size > 64 * 1024 ** 2:
                    raise RuntimeError("native daemon log exceeded 64 MiB cap")
                time.sleep(1 if manifest["mode"] == "orchestration-fixture" else 5)
    except BaseException:
        state.update(status="STOPPED_INCOMPLETE", error=traceback.format_exc())
        raise
    finally:
        kill_group(daemon.pid, daemon)
        daemon.wait(timeout=15)
        log.close()
        # Guardian stays alive until native workers have been reconciled/cleaned.
        state.update(finished=True, finished_at=time.time())
        if result is not None:
            state.update(result)
        save(root / "controller.json", state)
        guardian.wait(timeout=15)
        guardian_log.close()
    save(root / "selected-scope-receipt.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--seconds", type=int, default=14400)
    args = parser.parse_args()
    print(json.dumps(run(args.root, args.seconds)))
