"""Stop only the campaign's daemon/workers after controller death or cap violation."""
import argparse
import os
from pathlib import Path
import time

from vectorbench.orchestration.synthetic_cleanup import release_journal
from vectorbench.orchestration.synthetic_supervision import kill_group
from vectorbench.evidence.contracts import load, save
from vectorbench.evidence.registry import AdmissionConflict, Registry


def clean_active(root):
    registry = Registry(root / "admission.sqlite")
    results = []
    snapshot = registry.snapshot()
    retained = {(v["node"], v["run_id"]) for v in snapshot["leases"]}
    for attempt in snapshot["attempts"]:
        out = root / "cells" / attempt["node"].replace(":", "__") / attempt["run_id"]
        active = load(out / "active-worker.json") if (out / "active-worker.json").exists() else {}
        if active.get("pid") and attempt["ended"] is None:
            kill_group(active["pid"])
        try:
            released = release_journal(out)
        except Exception as exc:
            released = {"errors": [repr(exc)]}
        save(out / "guardian-cleanup.json", {"cleanup": released, "finished_at": time.time(),
             "status": "CLEANUP_FAILED" if released.get("errors") else "CLEANED"})
        if attempt["ended"] is None:
            try:
                registry.finish(attempt["node"], attempt["run_id"], "stopped")
            except AdmissionConflict:
                pass
        elif (attempt["node"], attempt["run_id"]) in retained and attempt["outcome"] == "worker-completed":
            registry.release_checked(attempt["node"], attempt["run_id"])
        results.append({"node": attempt["node"], "cleanup": released})
    return results


def watch(root):
    while True:
        state = load(root / "controller.json")
        if state["finished"]:
            cleanup = clean_active(root)
            save(root / "guardian-receipt.json", {"status": "RECONCILED", "cleanup": cleanup})
            return
        try:
            os.kill(state["pid"], 0)
            alive = True
        except ProcessLookupError:
            alive = False
        exceeded = time.time() >= state["deadline"]
        oversized = (root / "daemon.log").stat().st_size > 64 * 1024 ** 2
        if not alive or exceeded or oversized:
            kill_group(state["daemon_pid"])
            cleanup = clean_active(root)
            save(root / "guardian-receipt.json", {"status": "STOPPED_INCOMPLETE", "parent_alive": alive,
                 "deadline_exceeded": exceeded, "log_cap_exceeded": oversized, "cleanup": cleanup})
            return
        time.sleep(.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    watch(parser.parse_args().root.resolve())
