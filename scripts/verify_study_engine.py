"""Fault injection through native Dagster admission and the real independent cleanup consumer."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import numpy as np

from vectorbench.experiments.synthetic_insertion.contracts import config, load, save, sha
from vectorbench.experiments.synthetic_insertion.prepare import source_receipt
from vectorbench.orchestration.synthetic_supervision import supervised


def fake_campaign(root, fifo=False):
    root.mkdir(parents=True, exist_ok=False)
    c = config(True)
    if fifo:
        c["stage_seconds"] = 33
    cells = {}
    original = {}
    for name in ("first", "second", "third"):
        fk = "s11-" + name
        folder = root / "inputs" / fk
        folder.mkdir(parents=True)
        queries = np.array([[1, 2, 3]], dtype=np.float32)
        np.savez(folder / "fixture.npz", queries=queries)
        save(folder / "metadata.json", {"query_hash": hashlib.sha256(queries.tobytes()).hexdigest(), "states": {}})
        payload = folder / "payload"
        payload.write_bytes(name.encode())
        original[name] = payload.read_bytes()
        files = {str(p.relative_to(root)): sha(p) for p in folder.iterdir()}
        if fifo and name == "first":
            payload.unlink()
            os.mkfifo(payload)
        cells["fixture:" + fk] = {"id": fk, "fixture": fk, "kind": "fixture", "stage": "SETUP", "deps": [], "files": files}
    save(root / "manifest.json", {"id": root.name, "config": c, "cells": cells,
        "measurement_python": sys.executable, "sources": source_receipt(root),
        "expected": {"paths": 0, "incremental_checkpoints": 0, "fresh_checkpoints": 0, "quality_measurements": 0},
        "purpose": "ORCHESTRATION_ONLY_FAKE_INPUTS_NOT_DB_QUALITY"})
    save(root / "ledger.json", {k: {"status": "NOT_STARTED", "attempts": []} for k in cells})
    save(root / "preparation.json", {"status": "PASS"})
    return original


def invoke(root, selected, retry=(), expected=0):
    command = [".venv/bin/python", "-m", "vectorbench.experiments.synthetic_insertion.launch", str(root), "--stages", "A"]
    for key in selected:
        command += ["--select", key]
    for key in retry:
        command += ["--retry", key]
    p = subprocess.run(command, capture_output=True, text=True, timeout=120)
    number = len(list(root.glob("invoke-*.json"))) + 1
    save(root / f"invoke-{number}.json", {"command": command, "returncode": p.returncode,
         "stdout": p.stdout, "stderr": p.stderr})
    if expected == 0 and p.returncode != 0 or expected != 0 and p.returncode == 0:
        raise AssertionError(f"unexpected native entry result: {p.returncode}: {p.stderr[-3000:]}")
    return p


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = Path("runs") / (stamp + "-study-engine-checks")
    root.mkdir()
    campaign = root / "admission"
    original = fake_campaign(campaign)
    first, second, third = ["fixture:s11-" + s for s in ("first", "second", "third")]
    invoke(campaign, [first])
    first_entry = load(campaign / "ledger.json")[first]
    first_ack = (campaign / first_entry["ack"]).read_bytes()
    invoke(campaign, [first])
    if (campaign / first_entry["ack"]).read_bytes() != first_ack or len(load(campaign / "ledger.json")[first]["attempts"]) != 1:
        raise AssertionError("completed work executed again or acknowledgement changed")
    (campaign / "inputs/s11-second/payload").write_bytes(b"deliberate workload fault")
    invoke(campaign, [second], expected=1)
    ledger = load(campaign / "ledger.json")
    if ledger[second]["status"] != "FAIL" or len(ledger[second]["attempts"]) != 1 or ledger[third]["status"] != "NOT_STARTED":
        raise AssertionError("failure retried or unselected work executed")
    (campaign / "inputs/s11-second/payload").write_bytes(original["second"])
    invoke(campaign, [second], retry=[second])
    if len(load(campaign / "ledger.json")[second]["attempts"]) != 2:
        raise AssertionError("explicit retry did not preserve prior attempt")
    ledger = load(campaign / "ledger.json")
    # A valid-looking artifact/ack cannot borrow a successful run belonging to another cell.
    fake_out = campaign / "forged-result"
    fake_out.mkdir()
    receipt = load(campaign / load(campaign / first_entry["ack"])["receipt"])
    receipt["key"] = third
    save(fake_out / "receipt.json", receipt)
    save(fake_out / "ack.json", {"run_id": first_entry["run_id"], "receipt": "forged-result/receipt.json",
        "receipt_sha256": sha(fake_out / "receipt.json")})
    ledger[third].update(status="PASS", run_id=first_entry["run_id"], ack="forged-result/ack.json")
    save(campaign / "ledger.json", ledger)
    invoke(campaign, [third], expected=1)
    ledger[third] = {"status": "NOT_STARTED", "attempts": []}
    save(campaign / "ledger.json", ledger)
    cap = root / "timeout"
    fake_campaign(cap, fifo=True)
    invoke(cap, [first], expected=1)
    entry = load(cap / "ledger.json")[first]
    supervision = load(cap / entry["attempts"][-1]["out"] / "supervision.json")
    if not supervision["timed_out"]:
        raise AssertionError("native consumer did not enforce timeout")
    # Exercise cleanup of actual owned Docker resources after a failed/stopped process.
    for mode in ("failure", "timeout"):
        out = root / ("resource-" + mode)
        p = supervised([sys.executable, "-u", "-m", "scripts.study_resource_fault", str(out), mode], out,
                       seconds=3 if mode == "timeout" else 30)
        if mode == "failure" and p["returncode"] != 7 or mode == "timeout" and not p["timed_out"]:
            raise AssertionError("resource fault did not occur")
        if p["cleanup"]["errors"] or len(p["cleanup"]["released"]) != 2:
            raise AssertionError("independent cleanup did not release the owned container and volume")
        from vectorbench.orchestration.synthetic_cleanup import release_journal
        again = release_journal(out)
        if again["errors"] or again["released"]:
            raise AssertionError("cleanup was not idempotent")
    result = {"status": "PASS", "scope": "orchestration fake inputs only",
              "checks": ["native successful materialization", "completed skip without rewriting ack",
                         "real workload fault no auto retry", "explicit failed-only retry retains history",
                         "unselected NOT_STARTED retained", "foreign native-run adoption rejected",
                         "native consumer timeout", "failed process independent Docker cleanup",
                         "timed-out process independent Docker cleanup", "cleanup idempotence"]}
    save(root / "receipt.json", result)
    print(json.dumps({"root": str(root), **result}))


if __name__ == "__main__":
    main()
