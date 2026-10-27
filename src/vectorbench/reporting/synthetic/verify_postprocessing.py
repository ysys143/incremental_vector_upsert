"""Inject failures at post-processing consumers without touching measurement evidence."""
from datetime import datetime, timezone
from pathlib import Path
import shutil
import tempfile

from vectorbench.reporting.synthetic.finish_campaign import await_measurements, run
from vectorbench.reporting.synthetic.quality_summary import run as quality
from vectorbench.experiments.synthetic_insertion.contracts import load, save, sha


def main():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path("runs") / (stamp + "-postprocessing-checks")
    out.mkdir()
    checks = []
    with tempfile.TemporaryDirectory(prefix="ivu-analysis-check-") as temp:
        root = Path(temp) / "failed-controller"
        root.mkdir()
        save(root / "controller.json", {"finished": True, "completed_selection": False})
        save(root / "ledger.json", {"dummy": {"status": "NOT_STARTED"}})
        try:
            await_measurements(root)
        except AssertionError as exc:
            assert "without full completion" in str(exc)
            checks.append("stopped selected subset cannot publish a full report")
        else:
            raise AssertionError("stopped incomplete campaign accepted")
        save(root / "ledger.json", {"dummy": {"status": "FAIL"}})
        try:
            await_measurements(root)
        except AssertionError as exc:
            assert "measurement failed" in str(exc)
            checks.append("failed measurements stop before post-processing")
        else:
            raise AssertionError("failed measurement accepted")

        fake = out / "native-failure"
        fake.mkdir()
        save(fake / "controller.json", {"finished": True, "completed_selection": True})
        save(fake / "ledger.json", {"summary:all": {"status": "PASS", "run_id": "intentionally-absent"}})
        save(fake / "manifest.json", {"config": {"free_floor_bytes": 0}, "sources": {}, "cells": {}})
        report = fake / "MUST-NOT-EXIST.md"
        try:
            run(fake, report)
        except RuntimeError:
            state = load(fake / "analysis/finalization.json")
            assert state["status"] == "FAIL" and len(state["steps"]) == 1
            assert state["steps"][0]["returncode"] != 0
            assert not report.exists()
            checks.append("real failed native audit subprocess stops the command consumer; no report or later steps")
        else:
            raise AssertionError("missing native run was accepted")

        original = Path("study-results/entry-20261025-02")
        copied = Path(temp) / original.name
        copied.mkdir()
        for name in ("ledger.json", "manifest.json"):
            shutil.copy2(original / name, copied / name)
        entry = load(original / "ledger.json")["summary:all"]
        ack = load(original / entry["ack"])
        receipt = load(original / ack["receipt"])
        for name in [entry["ack"], ack["receipt"], *receipt["artifacts"]]:
            destination = copied / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original / name, destination)
        curves = copied / next(p for p in receipt["artifacts"] if p.endswith("curves.csv"))
        with curves.open("a") as stream:
            stream.write("intentionally corrupted\n")
        try:
            quality(copied)
        except AssertionError as exc:
            assert "missing or changed artifact" in str(exc)
            checks.append("modified acknowledged quality CSV rejected before any derived result")
        else:
            raise AssertionError("changed quality CSV accepted")
    save(out / "receipt.json", {"status": "PASS", "checks": checks, "count": len(checks),
         "code_sha256": sha(Path(__file__)), "negative_native_audit": "intentional failure retained under native-failure/"})
    print(out / "receipt.json")


if __name__ == "__main__":
    main()
