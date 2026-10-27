"""Freeze a finite native uniform-baseline setup; later shards consume its native output."""
import argparse
from pathlib import Path
import subprocess
import sys
import dagster as dg
from vectorbench.experiments.real_vectors.baseline.baseline_specs import config,counts
from vectorbench.evidence.contracts import GIB,canonical,check_space,load,save,sha
from vectorbench.orchestration.engine import native_receipt
from vectorbench.evidence.native_read import read
from vectorbench.evidence.registry import Registry
from vectorbench.evidence.source_manifest import freeze_sources


def setup(root,source,prerequisite,resume=None):
    if load(prerequisite/'manifest.json')['mode']!='real-request-smoke': raise AssertionError('large request smoke prerequisite missing')
    with dg.DagsterInstance.from_config(str(prerequisite.resolve()/'dagster-home')) as instance:
        receipt=native_receipt(prerequisite.resolve(),'report:all',instance)
        result=receipt['result']
        if result.get('scope')!='REQUEST_SMOKE_ONLY' or any(result.get(k)!=v for k,v in {'paths':28,'incremental_points':56,'fresh_points':28}.items()):
            raise AssertionError('large request smoke scope incomplete')
        checks=[e.asset_check_evaluation for e in instance.all_logs(receipt['run_id']) if e.asset_check_evaluation]
        if len(checks)!=1 or not checks[0].passed: raise AssertionError('large request native blocking check missing')
    source_refs={d:read(source,d) for d in ('msmarco','wikipedia')}
    if source_refs!=load(prerequisite/'manifest.json')['source_refs']: raise AssertionError('baseline source differs from qualified source')
    baseline=Registry(source/'admission.sqlite').baseline_free()
    predecessor=None
    if resume is not None:
        predecessor=load(resume/'manifest.json')
        if canonical(predecessor['config'])!=canonical(config().validate()) or predecessor['source_refs']!=source_refs:
            raise AssertionError('resume changes scientific input/config')
        state=Registry(resume/'admission.sqlite').snapshot()
        if state['leases'] or any(v['ended'] is None for v in state['attempts']): raise AssertionError('old setup not reconciled')
        input_paths=(
            'experiments/real_vectors/baseline/baseline_data.py',
            'experiments/real_vectors/baseline/baseline_specs.py',
            'workloads/events.py', 'evaluation/metrics.py', 'datasets/selection.py',
            'datasets/storage.py', 'evidence/checkpoints.py', 'datasets/data.py',
            'workloads/history_plan.py',
        )
        for relative in input_paths:
            path=Path('src/vectorbench')/relative
            name=path.stem
            if sha(path)!=sha(resume/'source'/path): raise AssertionError('input preparation changed: '+name)
        import ast
        def input_contracts(path):
            tree=ast.parse(path.read_text())
            tree.body=[n for n in tree.body if not (isinstance(n,ast.FunctionDef) and n.name=='owned_output_bytes')]
            return ast.dump(tree,include_attributes=False)
        if input_contracts(Path('src/vectorbench/evidence/contracts.py'))!=input_contracts(resume/'source/src/vectorbench/evidence/contracts.py'):
            raise AssertionError('input contracts changed beyond independent owned-output accounting')
    check_space(root.parent,baseline_free=baseline,reserve=2*GIB)
    running=subprocess.run(['docker','ps','--format','{{.Names}}'],check=True,capture_output=True,text=True,timeout=15).stdout
    if running.strip(): raise AssertionError('foreign DB workload present')
    root=root.resolve(); root.mkdir(parents=True,exist_ok=False)
    nodes={}
    for dataset in ('msmarco','wikipedia'):
        nodes['inputs:'+dataset]={'id':dataset,'role':'inputs','resource':'cpu','action':'baseline_inputs',
            'phase':'INPUTS','dataset':dataset,'deps':[],'reserve_bytes':256*1024**2}
        if resume is not None: nodes['inputs:'+dataset]['adopt_inputs']={'root':str(resume.resolve())}
    nodes['prepared:all']={'id':'all','role':'prepared','singleton':True,'resource':'cpu','action':'barrier','phase':'INPUTS','deps':list(nodes)}
    from vectorbench.experiments.real_vectors.baseline.baseline_tuning_spec import BUILDS,STATES
    for dataset in ('msmarco','wikipedia'):
        for db in ('postgres','qdrant'):
            key=dataset+'-'+db
            nodes['load_probe:'+key]={'id':key,'role':'load_probe','resource':'db','action':'baseline_load_probe','phase':'TUNE',
                'dataset':dataset,'db':db,'deps':['inputs:'+dataset,'prepared:all'],'reserve_bytes':2*GIB}
            groups=[]
            for build in BUILDS:
                for state in STATES:
                    identity=key+'-m'+str(build[0])+'-'+state; node='calibration_group:'+identity
                    nodes[node]={'id':identity,'role':'calibration_group','resource':'db','action':'baseline_calibration_group','phase':'TUNE',
                        'dataset':dataset,'db':db,'state_name':state,'build':list(build),
                        'deps':['inputs:'+dataset,'load_probe:'+key],'reserve_bytes':2*GIB}
                    groups.append(node)
            nodes['tuning:'+key]={'id':key,'role':'tuning','resource':'cpu','action':'baseline_select','phase':'TUNE',
                'dataset':dataset,'db':db,'deps':['load_probe:'+key,*groups],'reserve_bytes':64*1024**2}
    nodes['setup_report:all']={'id':'all','role':'setup_report','singleton':True,'resource':'cpu','action':'setup_report',
        'phase':'REPORT','deps':['inputs:'+d for d in ('msmarco','wikipedia')]+['tuning:'+d+'-'+db for d in ('msmarco','wikipedia') for db in ('postgres','qdrant')]}
    manifest={'mode':'real-baseline-setup','config':config().validate(),'nodes':nodes,'sources':freeze_sources(root),
        'source_root':str(source.resolve()),'source_refs':source_refs,'space_baseline_free':baseline,
        'measurement_python':'/opt/homebrew/bin/python3','orchestration_python':sys.executable,
        'prerequisite_smoke':{'root':str(prerequisite.resolve()),'native_run':receipt['run_id']},'declared_baseline_counts':counts(),
        'goal_scope':'uniform baseline preparation/calibration only; all advanced study families retained as incomplete'}
    if resume is not None:
        end=load(resume/'controller.json')['deadline']
        phases=Registry(resume/'admission.sqlite').snapshot()['phases']
        manifest.update(execution_window_end=end,deadlines={v['phase']:min(end,v['deadline']) for v in phases},
            predecessor={'root':str(resume.resolve()),'reason':'separate 600s DDL budget; native build/state groups; preserve successful input nodes and original window; partial failed calibration measurements excluded'})
    save(root/'manifest.json',manifest)
    return root


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--out',type=Path,required=True); p.add_argument('--source-root',type=Path,required=True)
    p.add_argument('--request-smoke-root',type=Path,required=True)
    p.add_argument('--resume-setup',type=Path)
    a=p.parse_args(); print(setup(a.out,a.source_root,a.request_smoke_root,a.resume_setup))
