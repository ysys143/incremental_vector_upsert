"""Verify real native automation and parallel processes, with no real DB workload."""
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
import traceback

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.orchestration.control import run
from vectorbench.evidence.registry import Registry
from vectorbench.evidence.source_manifest import source_hashes


def main():
    root = (Path("runs") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-queue-checks")).resolve()
    root.mkdir(parents=True)
    nodes = {"source:" + i: {"id": i, "role": "source", "resource": "cpu", "action": "fixture",
                            "phase": "CHECK", "deps": [], "sleep_seconds": 2} for i in ("a", "b", "c")}
    nodes["preparation:all"] = {"id": "all", "role": "preparation", "resource": "cpu", "action": "fixture",
        "phase": "CHECK", "singleton": True, "deps": list(nodes)}
    nodes["trial:a"] = {"id": "a", "role": "trial", "resource": "db", "action": "fixture",
                       "phase": "CHECK", "deps": ["preparation:all"], "sleep_seconds": 1,
                       "check_delay_seconds": 3}
    nodes["trial:b"] = {**nodes["trial:a"], "id": "b"}
    nodes["report:all"] = {"id": "all", "role": "report", "resource": "cpu", "action": "fixture",
        "phase": "CHECK", "singleton": True, "deps": ["trial:a", "trial:b"]}
    save(root / "manifest.json", {"mode": "orchestration-fixture", "nodes": nodes,
         "measurement_python": "/opt/homebrew/bin/python3", "deadlines": {"CHECK": time.time() + 240},
         "sources": source_hashes()})
    try:
        result = run(root, seconds=240)
        if result["status"] != "COMPLETE_SELECTED_SCOPE":
            raise AssertionError("native queue did not complete selected diagnostic scope")
        attempts = Registry(root / "admission.sqlite").snapshot()["attempts"]
        receipts = {a["node"]: load(root / a["receipt"]) for a in attempts}
        intervals = [(r["lease_acquired"], r["finished"]) for node, r in receipts.items() if node.startswith("source:")]
        times = sorted((t, delta) for start, end in intervals for t, delta in [(start, 1), (end, -1)])
        active, maximum = 0, 0
        for _, delta in times:
            active += delta; maximum = max(maximum, active)
        if maximum != 2:
            raise AssertionError("native source tasks did not actually overlap with a maximum of two")
        trial = receipts["trial:a"]
        prep = receipts["preparation:all"]
        report = receipts["report:all"]
        if trial["lease_acquired"] < prep["finished"] or report["lease_acquired"] < trial["finished"]:
            raise AssertionError("native phase dependency was bypassed")
        if Registry(root / "admission.sqlite").snapshot()["leases"]:
            raise AssertionError("native workers left active leases")
        for attempt in attempts:
            if not attempt["node"].startswith("trial:"):
                continue
            window = load(root / Path(attempt["receipt"]).parent / "check-window.json")
            if len(window["leases"]) != 1 or window["leases"][0]["node"] != attempt["node"]:
                raise AssertionError("native delayed DB check lost exclusive lease")
            for other, receipt in receipts.items():
                if other != attempt["node"] and receipt["lease_acquired"] < window["finished"] and receipt["finished"] > window["started"]:
                    raise AssertionError("heavy worker overlapped native DB blocking check")
        save(root / "receipt.json", {"status": "PASS", "scope": "ORCHESTRATION_ONLY; fake DB task, no scientific smoke",
             "native_runs": len(receipts), "max_parallel_cpu_workers": maximum,
             "checks": ["native daemon and queue executed all partitions without a cell launcher loop",
                        "three source workers overlap at maximum two", "preparation-to-DB-to-report barriers held",
                        "native required checks passed", "delayed DB blocking checks exclude all other heavy workers",
                        "leases released and selected daemon stopped"],
             "code_sha256": sha(Path(__file__))})
    except BaseException:
        save(root / "failure.json", {"status": "FAIL", "scope": "ORCHESTRATION_ONLY", "traceback": traceback.format_exc()})
        raise
    print(root / "receipt.json")


if __name__ == "__main__":
    main()
