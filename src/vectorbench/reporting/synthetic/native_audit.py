"""Read persisted Dagster success/check events; never materialize or retry a cell."""
import argparse
from pathlib import Path

import dagster as dg

from vectorbench.experiments.synthetic_insertion.contracts import effective_sources, load, read_ack, save, sha, source_revision


def run(root):
    root = root.resolve()
    manifest = load(root / "manifest.json")
    ledger = load(root / "ledger.json")
    revision = source_revision(effective_sources(root, manifest))
    runs = []
    with dg.DagsterInstance.from_config(str(root / "dagster-home")) as instance:
        for key in manifest["cells"]:
            receipt, ack = read_ack(root, key)
            record = instance.get_run_by_id(ack["run_id"])
            if record is None or record.status != dg.DagsterRunStatus.SUCCESS:
                raise AssertionError("native run is not SUCCESS: " + key)
            if record.tags.get("study/key") != key or record.tags.get("study/campaign") != root.name:
                raise AssertionError("native run scope mismatch: " + key)
            recorded = record.tags.get("study/source_revision", source_revision(manifest["sources"]))
            if recorded != receipt.get("source_revision", source_revision(manifest["sources"])):
                raise AssertionError("native/artifact source revision mismatch: " + key)
            # Older results are admitted only by the existing amendment-aware consumer.
            if recorded != revision:
                from vectorbench.experiments.synthetic_insertion.contracts import source_amendment
                reused = source_amendment(root)["reused_completed"][key]
                if reused["run_id"] != ack["run_id"] or reused["receipt_sha256"] != ack["receipt_sha256"] or reused["ack_sha256"] != sha(root / ledger[key]["ack"]):
                    raise AssertionError("older result has no matching reuse disposition")
            runs.append({"key": key, "run_id": ack["run_id"], "status": record.status.value,
                         "source_revision": recorded, "receipt_sha256": ack["receipt_sha256"]})
        summary_id = ledger["summary:all"]["run_id"]
        checks = [event.asset_check_evaluation for event in instance.all_logs(summary_id)
                  if event.asset_check_evaluation is not None]
        if len(checks) != 1 or not checks[0].passed or checks[0].check_name != "complete_evidence" or checks[0].asset_key != dg.AssetKey("summary"):
            raise AssertionError("summary complete_evidence native asset check did not pass")
    folder = root / "analysis"
    save(folder / "native-audit.json", {"status": "PASS", "native_runs": len(runs),
        "summary_run_id": summary_id, "asset_check": "summary/complete_evidence",
        "asset_check_passed": True, "runs": runs, "code_sha256": sha(Path(__file__))})
    print(folder / "native-audit.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    run(parser.parse_args().root)
