"""Finite local post-processing after a successful controller exit; no write retries."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

from vectorbench.experiments.synthetic_insertion.contracts import effective_sources, load, save, sha


def await_measurements(root):
    """Observe persisted state only; never launch, resume, cancel, or clean a DB."""
    while True:
        controller = load(root / "controller.json")
        ledger = load(root / "ledger.json")
        if any(v["status"] in {"FAIL", "UNVERIFIED"} for v in ledger.values()):
            raise AssertionError("measurement failed; no analysis or automatic retry")
        if controller["finished"]:
            if not controller.get("completed_selection") or any(v["status"] != "PASS" for v in ledger.values()):
                raise AssertionError("controller stopped without full completion")
            return
        try:
            os.kill(controller["pid"], 0)
        except ProcessLookupError as exc:
            raise AssertionError("measurement controller died; no automatic restart") from exc
        if controller.get("deadline") and time.time() > controller["deadline"] + 60:
            raise TimeoutError("original measurement stage deadline expired")
        time.sleep(15)


def run(root, report):
    root = root.resolve()
    out = root / "analysis"
    out.mkdir(exist_ok=True)
    record = out / "finalization.json"
    if record.exists():
        raise AssertionError("finalization already admitted; inspect the original attempt")
    paths = [Path(__file__), *(Path(__file__).parent / n for n in
             ("native_audit.py", "quality_summary.py", "geometry_cohorts.py", "final_report.py"))]
    sources = {str(p.resolve()): sha(p) for p in paths}
    state = {"status": "WAITING_FOR_MEASUREMENTS", "pid": os.getpid(), "started": time.time(),
             "report": str(report.resolve()), "code": sources, "steps": []}
    save(record, state)
    try:
        await_measurements(root)
        for name, expected in effective_sources(root, load(root / "manifest.json")).items():
            if sha(name) != expected:
                raise AssertionError("measurement sources changed before post-processing: " + name)
        deadline = time.time() + 1800
        commands = [
            [str(Path(".venv/bin/python").absolute()), "-m", "vectorbench.reporting.synthetic.native_audit", str(root)],
            [sys.executable, "-u", "-m", "vectorbench.reporting.synthetic.geometry_cohorts", str(root)],
            [sys.executable, "-m", "vectorbench.reporting.synthetic.quality_summary", str(root)],
            [sys.executable, "-m", "vectorbench.reporting.synthetic.final_report", str(root), "--report", str(report)],
        ]
        for command in commands:
            if any(sha(path) != expected for path, expected in sources.items()):
                raise AssertionError("post-processing sources changed after admission")
            if shutil.disk_usage(root).free < load(root / "manifest.json")["config"]["free_floor_bytes"]:
                raise RuntimeError("post-processing free-space floor exceeded")
            state["status"] = "ANALYZING"
            step = {"command": command, "started": time.time()}
            state["steps"].append(step)
            save(record, state)
            if deadline <= time.time():
                raise TimeoutError("post-processing 30-minute cap exhausted")
            with (out / "finalization.log").open("a") as stream:
                result = subprocess.run(command, stdout=stream, stderr=stream, timeout=deadline - time.time())
            step.update(returncode=result.returncode, finished=time.time())
            if result.returncode:
                raise RuntimeError("post-processing command failed; original logs retained")
        state.update(status="PASS", finished=time.time(), report_sha256=sha(report))
        save(record, state)
    except BaseException:
        state.update(status="FAIL", finished=time.time(), error=traceback.format_exc())
        save(record, state)
        raise
    print(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--report", type=Path, default=Path("docs/synthetic-insertion/results.md"))
    args = parser.parse_args()
    run(args.root, args.report)
