"""Freeze a complete native shard using the prepared unique-ledger catalogue."""
import argparse
from pathlib import Path
import subprocess
import sys

from vectorbench.experiments.real_vectors.baseline.baseline_campaign import freeze_sources
from vectorbench.experiments.real_vectors.baseline.baseline_native import read
from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases
from vectorbench.evidence.contracts import GIB,check_space,load,save


def catalogue(selected):
    nodes={}
    def add(role,identity,resource,action,phase,deps=(),**fields):
        key=role+':'+identity
        if key in nodes: raise AssertionError('duplicate scientific node')
        nodes[key]={'id':identity,'role':role,'resource':resource,'action':action,'phase':phase,'deps':list(deps),
                    'reserve_bytes':2*GIB if resource=='db' else 64*1024**2,**fields}
        return key
    context=add('study_context','all','cpu','context','CONTEXT',singleton=True)
    initial=[add('initial',db,'db','initial','INITIAL',[context],db=db) for db in ('postgres','qdrant')]
    gate=add('initialized','all','cpu','barrier','INITIAL',initial,singleton=True)
    histories=[add('history',db+'-'+case,'db','history','UPDATE',[context,gate,'initial:'+db],db=db,case=case)
               for db in ('postgres','qdrant') for case in cases()]
    hgate=add('updated','all','cpu','barrier','UPDATE',histories,singleton=True)
    rebuilt=[]
    for db in ('postgres','qdrant'):
        for digest,ref in sorted(selected['fresh_states'].items()):
            for repeat in range(3 if ref['terminal'] else 1):
                rebuilt.append(add('fresh',db+'-'+digest[:24]+'-r'+str(repeat),'db','fresh','FRESH',
                                   [context,hgate],db=db,state=digest,repeat=repeat))
    add('report','all','cpu','report','REPORT',[context,*histories,*rebuilt],singleton=True)
    if len(histories)!=58 or len(rebuilt)!=2*selected['fresh_builds_per_database']:
        raise AssertionError('declared baseline shard cardinality differs')
    return nodes,{'histories':58,'incremental_points':702,'fresh_points':len(rebuilt),'quality_points':702+len(rebuilt)}


def prepare(root,setup,dataset,seed):
    original=load(setup/"manifest.json")
    if dataset not in original["source_refs"] or seed not in original["config"]["seeds"]:
        raise ValueError("unknown declared dataset/seed")
    ready=read(setup,"setup_report:all")
    if ready["result"].get("calibrations")!=4: raise AssertionError("all setup calibrations must pass first")
    inputs=read(setup,"inputs:"+dataset)["result"]
    selected=inputs["seeds"][str(seed)]
    check_space(root.parent,baseline_free=original["space_baseline_free"],reserve=2*GIB)
    if subprocess.run(['docker','ps','--format','{{.Names}}'],check=True,capture_output=True,text=True,timeout=15).stdout.strip():
        raise AssertionError("baseline shard cannot start during another DB workload")
    root=root.resolve(); root.mkdir(parents=True,exist_ok=False)
    nodes,expected=catalogue(selected)
    save(root/'manifest.json',{'mode':'real-baseline-run','config':original['config'],'dataset':dataset,'seed':seed,
        'nodes':nodes,'sources':freeze_sources(root),'measurement_python':'/opt/homebrew/bin/python3','orchestration_python':sys.executable,
        'setup_root':str(setup.resolve()),'source_refs':original['source_refs'],'space_baseline_free':original['space_baseline_free'],
        'expected':expected,
        'goal_scope':'UNIFORM_BASELINE_SHARD_ONLY; all advanced study families remain required'})
    import dagster as dg
    from vectorbench.orchestration.engine import build
    dg.Definitions.validate_loadable(build(root))
    return root


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--out',type=Path,required=True); p.add_argument('--setup-root',type=Path,required=True)
    p.add_argument('--dataset',choices=('msmarco','wikipedia'),required=True); p.add_argument('--seed',type=int,choices=(11,29,47),required=True)
    a=p.parse_args(); print(prepare(a.out,a.setup_root,a.dataset,a.seed))
