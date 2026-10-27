"""Freeze a source-only prerequisite campaign, then let native automation own readiness."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

from vectorbench.evidence.contracts import Config, load, save, sha
from vectorbench.evidence.registry import Registry
from vectorbench.evidence.source_manifest import freeze_sources


def prepare(root, probes, predecessor=None):
    inherited = None
    if predecessor is not None:
        predecessor = predecessor.resolve()
        prior = load(predecessor / "manifest.json")
        stop = load(predecessor / "owner-stop-disposition.json")
        guardian = load(predecessor / "guardian-receipt.json")
        state = Registry(predecessor / "admission.sqlite").snapshot()
        if prior["mode"] != "real-source-preparation" or stop["source_files_written"] != 0:
            raise AssertionError("source revision requires an unstarted data transfer")
        if guardian["status"] not in {"STOPPED_INCOMPLETE", "RECONCILED"} or state["leases"] or any(a["ended"] is None for a in state["attempts"]):
            raise AssertionError("prior source owner has not been reconciled")
        if list((predecessor / "cells").rglob("*.fbin")):
            raise AssertionError("prior source bytes require a separate adoption decision")
        inherited = {"root": str(predecessor), "stop_sha256": sha(predecessor / "owner-stop-disposition.json"),
                     "deadline": stop["original_deadline"], "nodes": prior["nodes"]}
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    nodes = {}
    for name in ("msmarco", "wikipedia"):
        probe = load(probes / name / "probe.json")
        if probe["dataset"] != name or probe["data_kind"] != "real-precomputed" or probe["metric"] != "ip":
            raise AssertionError("preflight is not the selected real IP dataset")
        nodes["source:" + name] = {"id": name, "role": "source", "dataset": name,
            "action": "source", "phase": "SOURCE", "resource": "io", "reserve_bytes": 512 * 1024**2,
            "deps": [], "expected": {k: {n: probe[k][n] for n in ("etag", "rows", "dimension")} for k in ("base", "queries")}}
    if inherited is not None and nodes != inherited["nodes"]:
        raise AssertionError("source revision cannot change selected input versions")
    sources = freeze_sources(root)
    manifest = {"mode": "real-source-preparation", "config": Config().validate(),
         "measurement_python": sys.executable, "nodes": nodes, "sources": sources,
         "goal_scope": "source prerequisite only; no scientific DB smoke or experiment completion"}
    if inherited is not None:
        manifest.update(deadlines={"SOURCE": inherited["deadline"]},
                        predecessor={k: v for k, v in inherited.items() if k != "nodes"})
    save(root / "manifest.json", manifest)
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--probes", type=Path, default=Path("real-results/readiness-20261025"))
    parser.add_argument("--predecessor", type=Path)
    args = parser.parse_args()
    print(prepare(args.out, args.probes, args.predecessor))
