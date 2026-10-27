"""Per-attempt independent cleanup; no shared single-current-worker pointer."""
import argparse
import os
from pathlib import Path
import time

from vectorbench.orchestration.synthetic_cleanup import release_journal
from vectorbench.orchestration.synthetic_supervision import kill_group
from vectorbench.evidence.contracts import load, save
from vectorbench.evidence.registry import AdmissionConflict, Registry


def watch(root, out, parent, node, run_id, deadline):
    while True:
        path = out / "watch.json"
        state = load(path)
        if state.get("finished"):
            return
        try:
            os.kill(parent, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        expired = time.time() >= deadline
        if not alive or expired:
            active_path = out / "active-worker.json"
            active = load(active_path) if active_path.exists() else {}
            if active.get("pid"):
                kill_group(active["pid"])
            try:
                cleanup = release_journal(out)
            except Exception as exc:
                cleanup = {"errors": [repr(exc)]}
            registry = Registry(root / "admission.sqlite")
            try:
                registry.finish(node, run_id, "stopped")
            except AdmissionConflict:
                pass  # Native consumer may have completed its own cleanup at the same instant.
            save(out / "watchdog-receipt.json", {"parent_alive": alive, "deadline_exceeded": expired,
                 "cleanup": cleanup, "status": "STOPPED", "node": node, "run_id": run_id})
            if alive:
                # Native Dagster records its own run failure; do not kill an unrelated parent/run.
                save(out / "deadline-exceeded.json", {"deadline": deadline, "time": time.time()})
            return
        time.sleep(.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--parent", type=int, required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--deadline", type=float, required=True)
    args = parser.parse_args()
    watch(args.root, args.out, args.parent, args.node, args.run_id, args.deadline)
