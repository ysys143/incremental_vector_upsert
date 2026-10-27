"""Consume qualified setup and execute one dataset/seed uniform-baseline shard."""
import json
from pathlib import Path
import subprocess
import numpy as np

from vectorbench.experiments.real_vectors.actions import parents,pool_from,snapshot
from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases
from vectorbench.evidence.contracts import Config,load,save,sha
from vectorbench.datasets.storage import load_state,load_events
from vectorbench.workloads.workload import history,fresh


def qualified(manifest,node):
    p=subprocess.run([manifest["orchestration_python"],"-m","vectorbench.experiments.real_vectors.baseline.baseline_native",manifest["setup_root"],node],
                     capture_output=True,text=True,timeout=180)
    if p.returncode: raise AssertionError("native setup not qualified: "+p.stderr[-2000:])
    return json.loads(p.stdout)


def bound(context,path):
    expected=context["input_bindings"].get(str(Path(path).resolve()))
    if expected is None or sha(path)!=expected:
        raise AssertionError("qualified baseline input changed: "+str(path))


def execute(root,node,run_id,out,manifest,spec):
    deps=parents(root,out); action=spec["action"]
    if action=="context":
        setup=load(Path(manifest["setup_root"])/"manifest.json")
        if setup["config"]!=manifest["config"] or setup["source_refs"]!=manifest["source_refs"]:
            raise AssertionError("setup scientific source/config differs")
        ready=qualified(manifest,"setup_report:all")
        if ready["result"].get("calibrations")!=4 or ready["result"].get("inputs_and_oracles")!="PASS":
            raise AssertionError("shared setup is incomplete")
        inputs=qualified(manifest,"inputs:"+manifest["dataset"])
        ref=inputs["reference"]; prior=Path(ref["root"])/"cells"/ref["node"].replace(":","__")/ref["run_id"] /"receipt.json"
        if sha(prior)!=ref["receipt_sha256"]: raise AssertionError("native input receipt changed")
        bindings={str((Path(ref["root"])/name).resolve()):value for name,value in load(prior)["artifacts"].items()}
        bindings.update(inputs["result"].get("qualified_input_bindings",{}))
        tunings={db:qualified(manifest,"tuning:"+manifest["dataset"]+"-"+db) for db in ("postgres","qdrant")}
        result={"prepared":inputs["result"],"input_reference":ref,"input_bindings":bindings,
                "tunings":{db:value["result"] for db,value in tunings.items()},"tuning_references":{db:value["reference"] for db,value in tunings.items()},
                "setup_report":ready["reference"],"dataset":manifest["dataset"],"seed":manifest["seed"]}
        save(out/"context.json",result); return result
    if action=="barrier": return {"scope":"UNIFORM_BASELINE_SHARD_ONLY","native_parents":list(deps)}
    context=deps["study_context:all"]
    if action=="report":
        from vectorbench.reporting.baseline_report import report
        return report(root,manifest,deps,context,out)
    prepared=context["prepared"]; seed=prepared["seeds"][str(manifest["seed"])]; c=Config(**manifest["config"])
    bound(context,prepared["query_rows"])
    with np.load(prepared["query_rows"]) as q: query_rows=q["evaluation"]
    pool=pool_from(prepared); tuning=context["tunings"][spec["db"]]
    if action=="initial":
        bound(context,seed["initial"])
        adapted={"initial":seed["initial"],"query_rows":prepared["query_rows"],
                 "paths":{"insert":{"controls":[seed["initial_control"]]}}}
        return snapshot(spec["db"],pool,adapted,tuning,out,c)
    if action=="history":
        path=seed["paths"][spec["case"]]
        for file in (seed["initial"],path["events"],path["cohorts"]): bound(context,file)
        with np.load(path["cohorts"]) as data:
            groups={step:[path["cohort_names"][int(v)] for v in data[step]] for step in data.files}
        initial=load_state(seed["initial"],len(pool.vectors))
        frozen=deps["initial:"+spec["db"]]
        result=history(spec["db"],pool,initial.records(),load_events(path["events"]),query_rows,groups,path["controls"],
                       tuning,frozen["archive"],out,config=c)
        if result["final_ledger_sha256"]!=path["final_ledger_sha256"]:
            raise AssertionError("actual history final ledger differs from declared input")
        return result
    if action=="fresh":
        ref=seed["fresh_states"][spec["state"]]
        bound(context,ref["state"]["path"])
        live=load_state(ref["state"]["path"],len(pool.vectors))
        if live.digest()!=spec["state"]: raise AssertionError("fresh current ledger differs")
        return fresh(spec["db"],pool,live,query_rows,["all"]*len(query_rows),ref["control"],tuning,out,c)
    raise ValueError(action)
