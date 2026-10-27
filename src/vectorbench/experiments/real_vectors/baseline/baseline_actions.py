"""Uniform baseline preparation/calibration; full-study completion is a separate gate."""
import json
import subprocess
from pathlib import Path
from vectorbench.evidence.contracts import Config,save
from vectorbench.experiments.real_vectors.actions import parents,pool_from
from vectorbench.datasets.data import Pool


def execute(root,node,run_id,out,manifest,spec):
    if manifest["mode"]=="real-baseline-run":
        from vectorbench.experiments.real_vectors.baseline.baseline_run import execute as run_execute
        return run_execute(root,node,run_id,out,manifest,spec)
    if manifest["mode"]!="real-baseline-setup":
        raise NotImplementedError("baseline execution shards must be compiled from completed native setup")
    deps=parents(root,out); action=spec["action"]; c=Config(**manifest["config"])
    if action=="baseline_inputs":
        if spec.get("adopt_inputs"):
            ref=spec["adopt_inputs"]
            p=subprocess.run([manifest["orchestration_python"],"-m","vectorbench.experiments.real_vectors.baseline.baseline_native",ref["root"],"inputs:"+spec["dataset"]],
                             capture_output=True,text=True,timeout=180)
            if p.returncode: raise AssertionError("native input adoption failed: "+p.stderr[-1000:])
            value=json.loads(p.stdout); source_ref=value["reference"]
            prior=Path(source_ref["root"])/"cells"/source_ref["node"].replace(":","__")/source_ref["run_id"]/"receipt.json"
            from vectorbench.evidence.contracts import load,sha
            old_manifest=load(Path(ref["root"])/"manifest.json")
            if old_manifest["config"]!=manifest["config"] or old_manifest["source_refs"]!=manifest["source_refs"]:
                raise AssertionError("adopted input source/config differs")
            if sha(prior)!=source_ref["receipt_sha256"]: raise AssertionError("adopted input receipt changed")
            result=value["result"]
            result["qualified_input_bindings"]={str((Path(ref["root"])/name).resolve()):digest for name,digest in load(prior)["artifacts"].items()}
            result["native_input_adoption"]=source_ref
            save(out/"native-input-adoption.json",source_ref)
            return result
        p=subprocess.run([manifest["orchestration_python"],"-m","vectorbench.evidence.native_read",manifest["source_root"],spec["dataset"]],
                         capture_output=True,text=True,timeout=120)
        if p.returncode: raise AssertionError("real source qualification failed: "+p.stderr[-1000:])
        source=json.loads(p.stdout)
        if source!=manifest["source_refs"][spec["dataset"]]: raise AssertionError("source differs from setup freeze")
        pool=Pool.open(source["dataset"],source["base_path"],source["query_path"])
        from vectorbench.experiments.real_vectors.baseline.baseline_data import prepare
        result=prepare(pool,out,c); result["source"]=source
        return result
    if action=="barrier": return {"scope":"UNIFORM_BASELINE_SETUP_ONLY","native_parents":list(deps)}
    if action=="baseline_tune":
        prepared=deps["inputs:"+spec["dataset"]]
        from vectorbench.experiments.real_vectors.baseline.baseline_tuning import tune
        return tune(spec["db"],pool_from(prepared),prepared,out,c)
    if action=="baseline_load_probe":
        prepared=deps["inputs:"+spec["dataset"]]
        from vectorbench.experiments.real_vectors.baseline.baseline_tuning import load_probe
        return load_probe(spec["db"],pool_from(prepared),prepared,out,c)
    if action=="baseline_calibration_group":
        prepared=deps["inputs:"+spec["dataset"]]
        probe=deps["load_probe:"+spec["dataset"]+"-"+spec["db"]]
        from vectorbench.experiments.real_vectors.baseline.baseline_tuning import calibration_group
        return calibration_group(spec["db"],pool_from(prepared),prepared,spec["state_name"],spec["build"],probe,out,c)
    if action=="baseline_select":
        from vectorbench.experiments.real_vectors.baseline.baseline_tuning import select
        probe=deps["load_probe:"+spec["dataset"]+"-"+spec["db"]]
        groups=[v for k,v in deps.items() if k.startswith("calibration_group:")]
        return select(groups,probe,out)
    if action=="setup_report":
        expected={"inputs:"+d for d in ("msmarco","wikipedia")}|{"tuning:"+d+"-"+db for d in ("msmarco","wikipedia") for db in ("postgres","qdrant")}
        if set(deps)!=expected: raise AssertionError("baseline setup misses an input/calibration")
        counts={}
        for dataset in ("msmarco","wikipedia"):
            prepared=deps["inputs:"+dataset]
            if set(prepared["seeds"])!={str(s) for s in c.seeds}: raise AssertionError("evaluation seed missing")
            counts[dataset]={seed:{"paths":len(value["paths"]),"fresh_builds_per_database":value["fresh_builds_per_database"]}
                             for seed,value in prepared["seeds"].items()}
        result={"scope":"UNIFORM_BASELINE_SETUP_ONLY","inputs_and_oracles":"PASS","calibrations":4,"seed_counts":counts,
                "uniform_baseline_execution":"NOT RUN","geometry_filters_online_recovery":"NOT RUN"}
        save(out/"setup-summary.json",result); return result
    raise ValueError(action)
