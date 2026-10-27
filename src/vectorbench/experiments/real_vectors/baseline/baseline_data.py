"""Uniform baseline inputs for every declared seed, with global calibration holdout."""
from dataclasses import asdict
from pathlib import Path
import numpy as np

from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases, config
from vectorbench.evidence.checkpoints import Checkpoints
from vectorbench.evidence.contracts import save, sha
from vectorbench.datasets.data import oracle
from vectorbench.workloads.events import Event, Live, Write, initial
from vectorbench.workloads.history_plan import Recipe, compile_history
from vectorbench.datasets.selection import unique_rows
from vectorbench.datasets.storage import save_state


def split(pool, c):
    documents, doc_keys, duplicates = unique_rows(pool.vectors)
    _, query_keys, query_duplicates = unique_rows(pool.queries)
    eligible=np.asarray([row for key,row in query_keys.items() if key not in doc_keys],dtype=np.int64)
    if len(documents)<8*c.n or len(eligible)<c.calibration_queries+c.query_rows:
        raise ValueError("real pool cannot supply held-out calibration and complete 50-round histories")
    reserved=np.random.default_rng(c.calibration_seed).permutation(documents)
    qorder=np.random.default_rng(20261025).permutation(eligible)
    return {"calibration_documents":reserved[:2*c.n],"evaluation_documents":reserved[2*c.n:],
            "calibration_queries":qorder[:c.calibration_queries],
            "evaluation_queries":qorder[c.calibration_queries:c.calibration_queries+c.query_rows],
            "diagnostics":{"duplicate_documents":duplicates,"duplicate_queries":query_duplicates,
                           "document_query_overlap_values":len(set(doc_keys)&set(query_keys)),
                           "calibration_document_count":2*c.n,"global_holdout_across_all_seeds":True}}


def cohorts(operation, live, initial_truth, truth, n):
    if operation=="read_only": return np.full(len(truth),0,dtype=np.uint8),["unchanged"]
    if operation=="delete":
        deleted=set(range(n))-set(live.current)
        fraction=np.isin(initial_truth[:,:10],list(deleted)).mean(axis=1)
        names=["stable_previous_neighborhood","previously_deleted_neighborhood","boundary"]
    else:
        touched={w.id for w in live.records() if w.id>=n or w.version>0}
        fraction=np.isin(truth[:,:10],list(touched)).mean(axis=1)
        changed="metadata_changed_neighbor" if operation=="metadata" else "reinserted_ID_neighbor" if operation=="reinsert" else "new_or_updated_vector_neighbor"
        names=["old_or_untouched_neighbor",changed,"boundary"]
    labels=np.full(len(truth),2,dtype=np.uint8)
    labels[fraction<=.2]=0; labels[fraction>=.8]=1
    return labels,names


def prepare(pool, out, c=None):
    c=c or config(); selected=split(pool,c)
    labels=np.arange(len(pool.vectors),dtype=np.int64)%10
    qcal,qeval=selected["calibration_queries"],selected["evaluation_queries"]
    np.savez_compressed(out/"query-rows.npz",calibration=qcal,evaluation=qeval)
    save(out/"duplicate-diagnostics.json",selected["diagnostics"])
    calrows=selected["calibration_documents"]
    calibration={}
    cal_states={"initial":initial(calrows[:c.n],labels),"growth":initial(calrows,labels),
                "delete":initial(calrows[:c.n//2],labels),"replacement":initial(calrows[c.n:],labels)}
    for name,records in cal_states.items():
        live=Live(records,len(pool.vectors)); path=out/("calibration-"+name+".npz")
        save_state(path,live)
        truth=out/("calibration-"+name+"-oracle.npz")
        oracle(pool,live,qcal,truth,c)
        calibration[name]={"state":str(path),"control":{"path":str(truth),"sha256":sha(truth),
            "receipt_sha256":sha(truth.with_suffix(truth.suffix+".receipt.json"))}}
    cache=Checkpoints(pool,qeval,out/"checkpoints",c)
    seeds={}
    for seed in c.seeds:
        rows=np.random.default_rng(seed).permutation(selected["evaluation_documents"])
        start=initial(rows[:c.n],labels); replacements=rows[c.n:c.n+5*c.n]
        seed_dir=out/f"seed-{seed}"; seed_dir.mkdir()
        live=Live(start,len(pool.vectors)); initial_point=cache.get(live)
        with np.load(initial_point["control"]["path"]) as data: initial_truth=data["truth"]
        paths,fresh_states={},{}
        for identity,spec in cases().items():
            if spec["operation"]=="read_only":
                events=[Event(operation="read_only") for _ in range(50)]
                compiled={"events":events,"observed_steps":list(range(51)),"final":Live(start,len(pool.vectors)),
                          "counts":{"waves":50,"writes":0,"deletes":0,"final_count":c.n}}
            else:
                recipe=Recipe(spec["operation"],spec["waves"],spec["ratio"],spec["long"],spec["hot"],seed,spec["policy"])
                compiled=compile_history(start,replacements,recipe,labels)
            prepared=cache.path(start,compiled,seed_dir/identity)
            codes={}; names=None; current=Live(start,len(pool.vectors))
            for step in prepared["observed_steps"]:
                if step: current.apply(compiled["events"][step-1])
                with np.load(prepared["controls"][step]["path"]) as data: truth=data["truth"]
                codes[str(step)],names=cohorts(spec["operation"],current,initial_truth,truth,c.n)
                state=prepared["states"][str(step)]; digest=state["ledger_sha256"]
                terminal=step==prepared["observed_steps"][-1]
                if digest not in fresh_states:
                    fresh_states[digest]={"state":state,"control":prepared["controls"][step],"terminal":terminal}
                elif terminal: fresh_states[digest]["terminal"]=True
            group_path=seed_dir/identity/"cohorts.npz"
            np.savez_compressed(group_path,**codes)
            paths[identity]={**prepared,"spec":spec,"cohorts":str(group_path),"cohort_names":names}
        seeds[str(seed)]={"initial":initial_point["state"]["path"],"initial_control":initial_point["control"],
                         "paths":paths,"fresh_states":fresh_states,
                         "fresh_builds_per_database":sum(3 if v["terminal"] else 1 for v in fresh_states.values())}
    result={"dataset":pool.dataset,"scope":"UNIFORM_BASELINE_INPUTS; advanced geometry/filter families pending",
            "config":c.validate(),"query_rows":str(out/"query-rows.npz"),"calibration":calibration,
            "seeds":seeds,"unique_oracles":len(cache.oracles),"unique_ledgers":len(cache.states),
            "holdout":selected["diagnostics"],"metadata_kind":"controlled labels, not natural document metadata"}
    save(out/"prepared-inputs.json",result)
    return result
