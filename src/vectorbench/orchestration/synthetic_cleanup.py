"""Controller-safe independent cleanup, without NumPy or a live workload process."""
import json
from pathlib import Path
import subprocess

from vectorbench.experiments.synthetic_insertion.contracts import load, save


def declare(out, kind, name, owner):
    path = out / "resources.json"
    journal = load(path) if path.exists() else {"resources": []}
    item = {"kind": kind, "name": name, "owner": owner}
    if item not in journal["resources"]:
        journal["resources"].append(item)
    save(path, journal)


def release_journal(out):
    path = out / "resources.json"
    if not path.exists():
        return {"errors": [], "released": [], "absent": []}
    result = {"errors": [], "released": [], "absent": []}
    for r in reversed(load(path)["resources"]):
        inspect = ["docker"] + (["volume", "inspect"] if r["kind"] == "volume" else ["inspect"])
        p = subprocess.run([*inspect, r["name"]], capture_output=True, text=True, timeout=15)
        if p.returncode:
            if "no such" in p.stderr.lower() or "not found" in p.stderr.lower():
                result["absent"].append(r["name"])
                continue
            result["errors"].append(p.stderr)
            continue
        obj = json.loads(p.stdout)[0]
        labels = obj.get("Labels", {}) if r["kind"] == "volume" else obj["Config"].get("Labels", {})
        if (labels or {}).get("ivu.owner") != r["owner"]:
            result["errors"].append("ownership mismatch: " + r["name"])
            continue
        args = ["docker", "volume", "rm", r["name"]] if r["kind"] == "volume" else ["docker", "rm", "-f", r["name"]]
        p = subprocess.run(args, capture_output=True, text=True, timeout=20)
        (result["errors"] if p.returncode else result["released"]).append(p.stderr if p.returncode else r["name"])
    return result
