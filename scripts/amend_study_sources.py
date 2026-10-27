"""Explicit source-only amendment; immutable results and manifests stay unchanged."""
import argparse
import ast
import datetime
from pathlib import Path
import uuid

from vectorbench.experiments.synthetic_insertion.contracts import canonical, effective_sources, load, read_ack, save, sha
from vectorbench.experiments.synthetic_insertion.prepare import source_receipt
from vectorbench.evidence.source_manifest import SYNTHETIC_PLAN, source_files

parser = argparse.ArgumentParser()
parser.add_argument("root", type=Path)
parser.add_argument("--allow", action="append", default=[])
parser.add_argument("--reason", required=True)
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()
root = args.root.resolve()
manifest = load(root / "manifest.json")
parent = effective_sources(root, manifest)
paths = set(source_files(SYNTHETIC_PLAN))
current = {str(p): sha(p) for p in sorted(paths)}
changed = sorted(p for p in set(parent) | set(current) if parent.get(p) != current.get(p))
if args.dry_run:
    print(canonical({"changed": changed}))
    raise SystemExit(0)
if set(changed) != set(args.allow):
    raise SystemExit("exact changed-file allowlist required: " + canonical(changed))
protected = ["src/vectorbench/experiments/synthetic_insertion/data.py", "src/vectorbench/evaluation/synthetic_measure.py", "src/vectorbench/reporting/synthetic_results.py", "src/vectorbench/orchestration/synthetic_resources.py",
             "src/vectorbench/evidence/synthetic_telemetry.py", "src/vectorbench/experiments/pilot/model.py", "src/vectorbench/backends/pilot.py", "requirements.txt",
             "requirements-orchestration.txt", "requirements/base.txt", "requirements/orchestration.txt",
             "pyproject.toml", "src/vectorbench/evidence/source_manifest.py",
             "docs/synthetic-insertion/plan.md"]
if any(parent.get(p) != current.get(p) for p in protected):
    raise SystemExit("scientific inputs/measurement/configuration changed; a new campaign is required")
prior_pointer = root / "source-amendment-pointer.json"
prior_source = (root / load(prior_pointer)["path"]).parent / "source" if prior_pointer.exists() else root / "source"
protected_worker_functions = {"tune", "initial_snapshot", "incremental", "fresh", "verify_fixture"}
def bodies(path, names=protected_worker_functions):
    return {node.name: ast.dump(node, include_attributes=False) for node in ast.parse(path.read_text()).body
            if isinstance(node, ast.FunctionDef) and node.name in names}
if bodies(prior_source / "src/vectorbench/experiments/synthetic_insertion/worker.py") != bodies(Path("src/vectorbench/experiments/synthetic_insertion/worker.py")):
    raise SystemExit("scientific workload function changed; a new campaign is required")
contract_functions = {"config", "case_specs", "expected_counts"}
if bodies(prior_source / "src/vectorbench/experiments/synthetic_insertion/contracts.py", contract_functions) != bodies(Path("src/vectorbench/experiments/synthetic_insertion/contracts.py"), contract_functions):
    raise SystemExit("experimental catalogue/configuration functions changed; a new campaign is required")
reused = {}
for key, entry in load(root / "ledger.json").items():
    if entry["status"] == "PASS":
        receipt, ack = read_ack(root, key)
        reused[key] = {"run_id": ack["run_id"], "receipt_sha256": ack["receipt_sha256"],
                       "ack_sha256": sha(root / entry["ack"])}
folder = root / "amendments" / (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
folder.mkdir(parents=True, exist_ok=False)
frozen = source_receipt(folder)
if frozen != current:
    raise AssertionError("sources changed during amendment freeze")
amendment = {"manifest_sha256": sha(root / "manifest.json"), "parent_sources": parent,
             "sources": frozen, "changed": changed, "reason": args.reason,
             "actor": "coordinating assistant/caller; not a new user approval",
             "verified_invariants": {"protected_files_unchanged": protected,
                                      "workload_function_bodies_unchanged": sorted(protected_worker_functions)},
             "reused_completed": reused}
save(folder / "amendment.json", amendment)
record = folder / "amendment.json"
save(root / "source-amendment-pointer.json", {"path": str(record.relative_to(root)), "sha256": sha(record)})
print(canonical({"record": str(record), "changed": changed, "verified_reused_results": len(reused)}))
