"""Quality-qualified calibration on held-out initial/growth/delete/replacement states."""
import numpy as np
from vectorbench.experiments.pilot.model import validate_snapshot
from vectorbench.evidence.contracts import save
from vectorbench.orchestration.isolation import measured
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.backends.server import Server
from vectorbench.datasets.storage import load_state
from vectorbench.workloads.workload import oracle_for, read_point


from vectorbench.experiments.real_vectors.baseline.baseline_tuning_spec import BUILDS,EFS


def finish_server(server,out,first_error):
    errors=server.cleanup()
    if errors:
        save(out/"cleanup-failure.json",{"errors":errors,"first_error":repr(first_error)})
        if first_error is None: raise RuntimeError("baseline calibration cleanup failed: "+repr(errors))


def load_probe(db,pool,prepared,out,c):
    live=load_state(prepared["calibration"]["initial"]["state"],len(pool.vectors))
    ids,rows,attrs=metadata_arrays(live); server=Server(db,out/"server",config=c); error=None; probes=[]
    try:
        adapter=server.start()
        for batch in (100,1000,10000):
            adapter.batch=batch
            setup=measured(server,out/f"load-{batch}",lambda:adapter.setup(ids,pool.vectors[rows],BUILDS[0],attributes=attrs))
            from vectorbench.evaluation.measure import verify_snapshot
            verify_snapshot(ids,pool.vectors[rows],*adapter.snapshot())
            probes.append({"batch":batch,"setup":setup}); save(out/"load-probes.json",probes)
        return {"initial_batch":min(probes,key=lambda v:v["setup"]["load_s"])["batch"],"probes":probes}
    except BaseException as exc: error=exc; raise
    finally: finish_server(server,out,error)


def calibration_group(db,pool,prepared,name,build,probe,out,c):
    ref=prepared["calibration"][name]; live=load_state(ref["state"],len(pool.vectors))
    with np.load(prepared["query_rows"]) as q: query_rows=q["calibration"]
    truth=oracle_for(ref["control"],pool,live,query_rows)
    server=Server(db,out/"server",config=c); error=None
    try:
        adapter=server.start(); adapter.batch=probe["initial_batch"]
        ids,rows,attrs=metadata_arrays(live)
        try:
            setup=measured(server,out/"build",lambda:adapter.setup(ids,pool.vectors[rows],build,attributes=attrs))
        except RuntimeError:
            text=(server.root/"server-stderr.log").read_text() if (server.root/"server-stderr.log").exists() else ""
            if db=="postgres" and "canceling statement due to statement timeout" in text and "CREATE INDEX" in text:
                result={"status":"BUILD_TIME_LIMIT","build":list(build),"state":name,"points":[],"index_build_limit_s":600,
                        "scope":"candidate considered infeasible under declared build-time budget; no ANN quality claim"}
                save(out/"group.json",result); return result
            raise
        points=[]
        for ef in EFS:
            adapter.set_ef(ef)
            point=read_point(server,adapter,live,pool,np.asarray(pool.queries[query_rows]),truth,
                ["calibration"]*len(query_rows),out/f"ef{ef}",c,ref["control"])
            points.append({"ef":ef,"point":point})
            save(out/"points.json",points)
        result={"status":"MEASURED","build":list(build),"state":name,"points":points,"setup":setup,"initial_batch":probe["initial_batch"]}
        save(out/"group.json",result); return result
    except BaseException as exc: error=exc; raise
    finally: finish_server(server,out,error)


def select(groups,probe,out):
    if len(groups)!=16 or {(tuple(v["build"]),v["state"]) for v in groups}!={(b,s) for b in BUILDS for s in ("initial","growth","delete","replacement")}:
        raise AssertionError("calibration selection is missing a build/state group")
    candidates=[]
    for build in BUILDS:
        selected=[v for v in groups if v["build"]==list(build)]
        limited=any(v["status"]!="MEASURED" for v in selected)
        for ef in EFS:
            if limited:
                candidates.append({"build":list(build),"ef":ef,"status":"INCOMPLETE_BUILD_BUDGET"}); continue
            points=[next(p["point"] for p in v["points"] if p["ef"]==ef) for v in selected]
            candidates.append({"build":list(build),"ef":ef,"status":"MEASURED",
                "min_recall10":min(p["recall10"] for p in points),"min_recall100":min(p["recall100"] for p in points),
                "max_p95_ms":max(p["p95_ms"] for p in points),"build_s":sum(v["setup"]["build_s"] for v in selected),
                "shortfall":max(p["shortfall100"] for p in points)})
    choices={}; frontiers={}
    for target in (.95,.99):
        eligible=[v for v in candidates if v["status"]=="MEASURED" and min(v["min_recall10"],v["min_recall100"])>=target and v["shortfall"]==0]
        if not eligible: raise AssertionError(f"no complete calibration candidate meets {target} across all four states")
        choices[str(target)]=min(eligible,key=lambda v:v["max_p95_ms"])
        frontiers[str(target)]=[a for a in eligible if not any(b["max_p95_ms"]<=a["max_p95_ms"] and b["build_s"]<=a["build_s"] and
            (b["max_p95_ms"]<a["max_p95_ms"] or b["build_s"]<a["build_s"]) for b in eligible)]
    best=choices['0.99']
    result={"build":best["build"],"ef":best["ef"],"initial_batch":probe["initial_batch"],"selected_target":.99,
        "grid":{"builds":[list(b) for b in BUILDS],"efs":list(EFS),"initial_batches":[100,1000,10000]},
        "states":["initial","growth","delete","replacement"],"candidates":candidates,"choices":choices,"pareto":frontiers,
        "index_build_limit_s":600,"scope":"uniform baseline, one native build/state group per calibration; fixed choices for all three evaluation seeds"}
    save(out/"tuning.json",result); return result


def tune(db,pool,prepared,out,c):
    with np.load(prepared["query_rows"]) as q: query_rows=q["calibration"]
    queries=np.asarray(pool.queries[query_rows])
    states={name:load_state(ref["state"],len(pool.vectors)) for name,ref in prepared["calibration"].items()}
    truths={name:oracle_for(prepared["calibration"][name]["control"],pool,live,query_rows) for name,live in states.items()}
    server=Server(db,out/"server",config=c); error=None
    try:
        adapter=server.start(); probes=[]; all_points=[]
        ids,rows,attrs=metadata_arrays(states["initial"])
        for batch in (100,1000,10000):
            adapter.batch=batch
            setup=measured(server,out/f"load-{batch}",lambda:adapter.setup(ids,pool.vectors[rows],BUILDS[0],attributes=attrs))
            validate_snapshot(ids,pool.vectors[rows],*adapter.snapshot())
            probes.append({"batch":batch,"setup":setup}); save(out/"load-probes.json",probes)
        batch=min(probes,key=lambda v:v["setup"]["load_s"])["batch"]
        for build in BUILDS:
            for name,live in states.items():
                ids,rows,attrs=metadata_arrays(live); adapter.batch=batch
                setup=measured(server,out/f"build-{build[0]}-{name}",lambda:adapter.setup(ids,pool.vectors[rows],build,attributes=attrs))
                for ef in EFS:
                    adapter.set_ef(ef)
                    point=read_point(server,adapter,live,pool,queries,truths[name],["calibration"]*len(queries),
                        out/f"m{build[0]}-{name}-ef{ef}",c,prepared["calibration"][name]["control"])
                    all_points.append({"build":list(build),"ef":ef,"state":name,"setup":setup,"point":point})
                    save(out/"candidates.json",all_points)
        candidates=[]
        for build in BUILDS:
            for ef in EFS:
                values=[v for v in all_points if v["build"]==list(build) and v["ef"]==ef]
                if len(values)!=4: raise AssertionError("calibration candidate misses a held-out state")
                candidates.append({"build":list(build),"ef":ef,"min_recall10":min(v["point"]["recall10"] for v in values),
                    "min_recall100":min(v["point"]["recall100"] for v in values),"max_p95_ms":max(v["point"]["p95_ms"] for v in values),
                    "build_s":sum(v["setup"]["build_s"] for v in values),
                    "shortfall":max(v["point"]["shortfall100"] for v in values)})
        choices={}; frontiers={}
        for target in (.95,.99):
            eligible=[v for v in candidates if min(v["min_recall10"],v["min_recall100"])>=target and v["shortfall"]==0]
            if not eligible: raise AssertionError(f"no baseline ANN candidate qualifies for {target} across all four states")
            choices[str(target)]=min(eligible,key=lambda v:v["max_p95_ms"])
            frontiers[str(target)]=[a for a in eligible if not any(b["max_p95_ms"]<=a["max_p95_ms"] and b["build_s"]<=a["build_s"]
                and (b["max_p95_ms"]<a["max_p95_ms"] or b["build_s"]<a["build_s"]) for b in eligible)]
        best=choices['0.99']
        result={"build":best["build"],"ef":best["ef"],"initial_batch":batch,"selected_target":.99,
            "grid":{"builds":[list(v) for v in BUILDS],"efs":list(EFS),"initial_batches":[100,1000,10000]},
            "states":list(states),"candidates":candidates,"choices":choices,"pareto":frontiers,
            "scope":"uniform baseline calibration, disjoint documents and queries; fixed settings across all three evaluation seeds"}
        save(out/"tuning.json",result)
        return result
    except BaseException as exc:
        error=exc; raise
    finally:
        errors=server.cleanup()
        if errors:
            save(out/"cleanup-failure.json",{"errors":errors,"first_error":repr(error)})
            if error is None: raise RuntimeError("baseline calibration cleanup failed: "+repr(errors))
