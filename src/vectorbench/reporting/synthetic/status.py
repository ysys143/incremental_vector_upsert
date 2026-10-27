"""Read-only status for an existing campaign; does not import or start Dagster."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from vectorbench.experiments.synthetic_insertion.contracts import load


def main(root):
    root = root.resolve()
    manifest, ledger = load(root / "manifest.json"), load(root / "ledger.json")
    stages = defaultdict(Counter)
    for key, value in ledger.items():
        stages[manifest["cells"][key]["stage"]][value["status"]] += 1
    result = {"campaign": root.name, "ledger_status": dict(Counter(v["status"] for v in ledger.values())),
              "stages": dict(stages), "controller": load(root / "controller.json"),
              "incomplete_attempts": [{"key": key, "status": value["status"], "attempts": value["attempts"]}
                                      for key, value in ledger.items() if value["status"] in {"FAIL", "UNVERIFIED"}]}
    finalizer = root / "analysis/finalization.json"
    if finalizer.exists():
        state = load(finalizer)
        result["post_processing"] = {k: state[k] for k in ("status", "pid", "report")}
        if "error" in state:
            result["post_processing"]["error"] = state["error"]
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    main(parser.parse_args().root)
