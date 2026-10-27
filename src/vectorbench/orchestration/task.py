"""One admitted substantive task. Unknown actions fail rather than fake completion."""
import argparse
import json
import subprocess
import os
from pathlib import Path
import shutil
import time
import traceback
import resource
import sys

from vectorbench.evidence.contracts import Config, ResourceGate, load, save, sha, owned_output_bytes
from vectorbench.evidence.registry import Registry


def run(root, node, run_id):
    manifest = load(root / "manifest.json")
    spec = manifest["nodes"][node]
    registry = Registry(root / "admission.sqlite")
    out = root / "cells" / node.replace(":", "__") / run_id
    os.environ["REAL_STUDY_TASK_OUT"] = str(out)
    os.environ["REAL_STUDY_BUDGET_ROOT"] = str(root)
    if manifest["mode"].startswith("real-baseline-") or manifest['mode'] in {'real-twelve-hour','real-clustered-insertion'}:
        os.environ["REAL_STUDY_RESOURCE_SAMPLING"] = "1"
    if manifest['mode']=='real-clustered-insertion':
        os.environ['REAL_STUDY_CLUSTERED_RESOURCES']='1'
    admission = load(out / "admission.json")
    if admission["node"] != node or admission["run_id"] != run_id or admission["mode"] != manifest["mode"]:
        raise AssertionError("task admission scope mismatch")
    started = time.time()
    diagnostic = manifest["mode"] == "orchestration-fixture"
    if diagnostic != (spec["action"] == "fixture"):
        raise AssertionError("diagnostic tasks cannot qualify a real-data workload")
    while True:
        free = 40 * 1024 ** 3 if diagnostic else shutil.disk_usage(root).free
        try:
            registry.acquire(node, run_id, spec["resource"], spec.get("reserve_bytes", 0), free)
            break
        except ResourceGate as exc:
            save(out / "resource-wait.json", {"status": "WAITING", "reason": str(exc), "time": time.time()})
            if time.time() >= admission["deadline"] - 30:
                raise
            time.sleep(.25 if diagnostic else 10)
    lease_acquired = time.time()
    adopted_artifacts = {}
    try:
        if not diagnostic:
            # Scan only after admission: no preparation scan can overlap a DB lease elsewhere.
            storage=registry.check_owned_storage(node,run_id,owned_output_bytes(root.parent))
            save(out/"owned-storage-admission.json",storage)
        # Large dependency reads belong to this task's resource lease, never admission.
        for dependency in admission.get("dependencies", []):
            parent_path = root / "cells" / dependency["node"].replace(":", "__") / dependency["run_id"] / "receipt.json"
            if sha(parent_path) != dependency["sha256"]:
                raise AssertionError("admitted dependency receipt changed")
            parent = load(parent_path)
            for name, expected in parent["artifacts"].items():
                if sha(root / name) != expected:
                    raise AssertionError("admitted dependency artifact changed: " + name)
        if diagnostic:
            if spec.get("fail"):
                raise RuntimeError("injected real worker failure for orchestration boundary")
            time.sleep(spec.get("sleep_seconds", .01))
            result = {"scope": "ORCHESTRATION_ONLY", "node": node}
            save(out / "fixture-output.json", result)
        elif spec.get('native_adoption'):
            module=('vectorbench.experiments.real_vectors.clustered.adopt' if manifest['mode']=='real-clustered-insertion'
                    else 'vectorbench.experiments.real_vectors.twelve_hour.twelve_adopt')
            process=subprocess.run([manifest['orchestration_python'],'-m',module,str(root),node],
                capture_output=True,text=True,timeout=180)
            if process.returncode: raise AssertionError('native preparation adoption failed: '+process.stderr[-2000:])
            proof=json.loads(process.stdout)
            save(out/'adoption-proof.json',proof)
            result=proof['result'];adopted_artifacts=proof['artifacts']
            for name,expected in adopted_artifacts.items():
                if sha(root/name)!=expected: raise AssertionError('adopted artifact changed after native verification')
        elif spec["action"] == "source":
            from vectorbench.datasets.source import DATASETS, adopt_complete, fetch, probe
            dataset = DATASETS[spec["dataset"]]
            base, queries = probe(dataset.base), probe(dataset.queries)
            expected = spec["expected"]
            if any(value["etag"] != expected[label]["etag"] or value["rows"] != expected[label]["rows"]
                   or value["dimension"] != expected[label]["dimension"]
                   for label, value in [("base", base), ("queries", queries)]):
                raise AssertionError("public source version differs from the frozen preflight")
            save(out / "source-probe.json", {"base": base, "queries": queries})
            base_result = adopt_complete(base, spec["cache_base"], out / "base.fbin", dataset.name) if spec.get("cache_base") else fetch(base, out / "base.fbin", Config().pool_rows, Config().source_blocks)
            query_result = fetch(queries, out / "queries.fbin", queries["rows"], 1)
            result = {"dataset": dataset.name, "data_kind": "real-precomputed", "metric": dataset.metric,
                      "base": base_result, "queries": query_result}
        else:
            from vectorbench.experiments.real_vectors.actions import execute
            result = execute(root, node, run_id, out, manifest, spec)
        artifacts = {str(p.relative_to(root)): sha(p) for p in out.rglob("*") if p.is_file()
                     and p.name not in {"stdout.log", "stderr.log", "active-worker.json", "resources.json", "supervision.json",
                                        "watch.json", "watchdog.log", "resource-wait.json"}}
        artifacts.update(adopted_artifacts)
        save(out / "receipt.json", {"status": "PASS", "campaign": root.name, "node": node,
             "run_id": run_id, "mode": manifest["mode"], "spec_hash": admission["spec_hash"],
             "started": started, "lease_acquired": lease_acquired, "resource": spec["resource"],
             "finished": time.time(), "result": result, "artifacts": artifacts,
             "worker_peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)})
    finally:
        # Leases are released by the native consumer after worker exit/independent cleanup.
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("node")
    parser.add_argument("run_id")
    args = parser.parse_args()
    try:
        run(args.root.resolve(), args.node, args.run_id)
    except BaseException:
        out = args.root / "cells" / args.node.replace(":", "__") / args.run_id
        save(out / "failure.json", {"status": "FAIL", "traceback": traceback.format_exc()})
        raise
