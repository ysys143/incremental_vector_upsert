"""Finite Dagster materializations with durable acknowledgement and a separate watchdog."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import dagster as dg

from vectorbench.experiments.synthetic_insertion.contracts import canonical, effective_sources, load, read_ack, save, sha, source_revision, verify_files


def run(root, stages="ABCD", retry_keys=(), keys=None):
    root = root.resolve()
    os.environ["STUDY_ROOT"] = str(root)
    home = root / "dagster-home"
    home.mkdir(exist_ok=True)
    if not (home / "dagster.yaml").exists():
        (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    os.environ["DAGSTER_HOME"] = str(home)
    from vectorbench.experiments.synthetic_insertion import definitions
    manifest = load(root / "manifest.json")
    if not load(root / "preparation.json")["status"] == "PASS":
        raise AssertionError("input preparation incomplete")
    accepted_sources = effective_sources(root, manifest)
    revision = source_revision(accepted_sources)
    for path, expected in accepted_sources.items():
        if sha(path) != expected:
            raise AssertionError("workload sources changed since campaign freeze: " + path)
    instance = dg.DagsterInstance.get()
    if not manifest["config"]["smoke"]:
        previous_root = Path(manifest["prerequisite_smoke"])
        previous_manifest = load(previous_root / "manifest.json")
        previous_summary, previous_ack = read_ack(previous_root, "summary:all")
        prior_sources = effective_sources(previous_root, previous_manifest)
        if not previous_manifest["config"]["smoke"] or prior_sources != accepted_sources:
            raise AssertionError("smoke scope/source hashes differ from this campaign")
        with dg.DagsterInstance.from_config(str(previous_root / "dagster-home")) as previous_instance:
            native = previous_instance.get_run_by_id(previous_ack["run_id"])
            if native is None or native.status != dg.DagsterRunStatus.SUCCESS or native.tags.get("study/campaign") != previous_root.name or native.tags.get("study/source_revision") != source_revision(prior_sources):
                raise AssertionError("smoke summary has no successful native Dagster run")
    save(root / "controller.json", {"pid": os.getpid(), "deadline": None, "finished": False})
    watch_log = open(root / "watchdog.log", "a")
    watcher = subprocess.Popen([manifest["measurement_python"], "-m", "vectorbench.orchestration.synthetic_supervision", str(root),
                               "--parent", str(os.getpid())], stdout=watch_log, stderr=watch_log,
                               start_new_session=True)
    engine_log = open(root / "dagster-native.log", "a")
    completed = False
    try:
        selected = list(keys) if keys is not None else [k for k, s in manifest["cells"].items() if s["stage"] == "SETUP" or s["stage"] in stages]
        if any(k not in manifest["cells"] for k in selected):
            raise AssertionError("selected key is not in the frozen campaign")
        if stages == "ABCD" and keys is None:
            selected.append("summary:all")
        phase_order = {"SETUP": 0, "A": 1, "B": 2, "C": 3, "D": 4, "REPORT": 5}
        kind_order = {"fixture": 0, "tune": 1, "base": 2, "incremental": 3, "fresh": 4, "summary": 5}
        selected = sorted(set(selected), key=lambda key: (
            phase_order[manifest["cells"][key]["stage"]], kind_order[manifest["cells"][key]["kind"]],
            manifest["cells"][key].get("seed", 0),
            (0 if manifest["cells"][key].get("db") == ("postgres" if manifest["cells"][key].get("seed", 11) != 29 else "qdrant") else 1),
            manifest["cells"][key].get("geometry", ""), manifest["cells"][key].get("ratio", 0),
            manifest["cells"][key].get("order", ""), manifest["cells"][key].get("policy", ""),
            (-manifest["cells"][key].get("waves", 0) if manifest["cells"][key].get("seed") == 29
             else manifest["cells"][key].get("waves", 0)), key))
        with open(root / "selection-history.jsonl", "a") as stream:
            stream.write(canonical({"time": time.time(), "selected": selected, "retry_keys": list(retry_keys),
                                    "argv": sys.argv, "authority": "caller selection, not a new user approval"}) + "\n")
        clocks = load(root / "stage-clocks.json") if (root / "stage-clocks.json").exists() else {}
        for key in selected:
            ledger = load(root / "ledger.json")
            state = ledger[key]
            if state["status"] == "PASS":
                definitions.native_ack(instance, key)
                continue
            if state["status"] != "NOT_STARTED" and key not in retry_keys:
                raise AssertionError("explicit retry disposition required for prior incomplete attempt: " + key)
            spec = manifest["cells"][key]
            for dep in spec["deps"]:
                definitions.native_ack(instance, dep)
            phase = spec["stage"]
            if phase not in clocks:
                clocks[phase] = {"started": time.time(), "deadline": time.time() + manifest["config"]["stage_seconds"]}
                save(root / "stage-clocks.json", clocks)
            if time.time() >= clocks[phase]["deadline"] - 30:
                raise TimeoutError("original stage cap is consumed: " + phase)
            save(root / "controller.json", {"pid": os.getpid(), "stage": phase,
                 "deadline": clocks[phase]["deadline"], "out": None, "finished": False})
            key_kind, partition = key.split(":", 1)
            kwargs = {} if key_kind == "summary" else {"partition_key": partition}
            # Native Dagster event output is kept on disk; only useful progress is printed.
            original_stdout, original_stderr = sys.stdout, sys.stderr
            try:
                sys.stdout = sys.stderr = engine_log
                result = dg.materialize([*definitions.assets, definitions.summary, definitions.complete_evidence],
                    instance=instance, selection=dg.AssetSelection.assets(key_kind), raise_on_error=False,
                    tags={"study/campaign": root.name, "study/key": key, "study/source_revision": revision,
                          "dagster/max_runtime": str(manifest["config"]["stage_seconds"])}, **kwargs)
            finally:
                sys.stdout, sys.stderr = original_stdout, original_stderr
                engine_log.flush()
            ledger = load(root / "ledger.json")
            entry = ledger[key]
            record = instance.get_run_by_id(result.run_id)
            if not result.success or record.status != dg.DagsterRunStatus.SUCCESS:
                entry["status"] = "FAIL"
                save(root / "ledger.json", ledger)
                raise RuntimeError("native Dagster run failed: " + key + " " + result.run_id)
            out = root / entry["attempts"][-1]["out"]
            receipt = load(out / "receipt.json")
            verify_files(root, receipt)
            ack = {"campaign": root.name, "key": key, "run_id": result.run_id,
                   "receipt": str((out / "receipt.json").relative_to(root)),
                   "receipt_sha256": sha(out / "receipt.json"), "native_status": record.status.value}
            save(out / "ack.json", ack)
            entry.update(status="PASS", ack=str((out / "ack.json").relative_to(root)), run_id=result.run_id)
            save(root / "ledger.json", ledger)
            print(canonical({"key": key, "status": "PASS", "run_id": result.run_id,
                             "completed": sum(v["status"] == "PASS" for v in ledger.values()),
                             "selected": len(selected)}), flush=True)
        completed = True
    finally:
        save(root / "controller.json", {"pid": os.getpid(), "finished": True, "completed_selection": completed})
        watcher.wait(timeout=10)
        watch_log.close()
        engine_log.close()
        instance.dispose()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--stages", default="ABCD")
    parser.add_argument("--retry", action="append", default=[])
    parser.add_argument("--select", action="append")
    args = parser.parse_args()
    raise SystemExit(run(args.root, args.stages, args.retry, args.select))
