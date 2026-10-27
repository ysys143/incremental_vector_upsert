"""Native setup evidence reader, usable in the NumPy-free orchestration interpreter."""
import argparse
import json
from pathlib import Path
import dagster as dg
from vectorbench.evidence.contracts import load,sha
from vectorbench.orchestration.engine import native_receipt


def read(root,node):
    root=root.resolve()
    manifest=load(root/"manifest.json")
    if manifest["mode"]!="real-baseline-setup" or node not in manifest["nodes"]:
        raise AssertionError("uniform baseline requires its own real native setup")
    with dg.DagsterInstance.from_config(str(root/"dagster-home")) as instance:
        receipt=native_receipt(root,node,instance)
        checks=[e.asset_check_evaluation for e in instance.all_logs(receipt["run_id"]) if e.asset_check_evaluation]
        if len(checks)!=1 or not checks[0].passed: raise AssertionError("setup blocking check missing")
    path=root/"cells"/node.replace(":","__")/receipt["run_id"]/"receipt.json"
    return {"result":receipt["result"],"reference":{"root":str(root),"node":node,"run_id":receipt["run_id"],"receipt_sha256":sha(path)}}


if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("root",type=Path); p.add_argument("node")
    a=p.parse_args(); print(json.dumps(read(a.root,a.node)))
