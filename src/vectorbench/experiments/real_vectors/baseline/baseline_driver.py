"""Finite setup-to-six-shard execution; Dagster owns each shard's cells and queue."""
import argparse
from pathlib import Path
import os
import subprocess
import sys
import time

from vectorbench.experiments.real_vectors.baseline.baseline_shard import prepare
from vectorbench.evidence.contracts import load,save


def run(setup,out):
    setup=setup.resolve(); out=out.resolve(); out.mkdir(parents=True,exist_ok=False)
    original=load(setup/'controller.json'); owner=original['pid']; cap=original['deadline']
    state={'status':'WAITING_FOR_QUALIFIED_SETUP','setup':str(setup),'pid':os.getpid(),'started':time.time(),'completed_shards':[]}
    save(out/'driver.json',state)
    while True:
        current=load(setup/'controller.json')
        if current['pid']!=owner: raise AssertionError('setup owner changed; no blind restart')
        if current.get('finished'):
            if current['status']!='COMPLETE_SELECTED_SCOPE': raise AssertionError('setup failed; inspect original evidence')
            break
        try: os.kill(owner,0)
        except ProcessLookupError: raise RuntimeError('setup owner is no longer live; no auto restart')
        if time.time()>=cap: raise TimeoutError('original setup window exhausted')
        time.sleep(5)
    for dataset in ('msmarco','wikipedia'):
        for seed in (11,29,47):
            target=out/f'{dataset}-s{seed}'
            state.update(status='PREPARING_SHARD',dataset=dataset,seed=seed); save(out/'driver.json',state)
            prepare(target,setup,dataset,seed)
            state.update(status='NATIVE_SHARD_RUNNING',current_root=str(target)); save(out/'driver.json',state)
            with (out/(target.name+'-controller.log')).open('w') as log:
                p=subprocess.Popen([sys.executable,'-m','vectorbench.orchestration.control',str(target)],stdout=log,stderr=log)
                state['current_controller_pid']=p.pid; save(out/'driver.json',state)
                try:
                    code=p.wait(timeout=14460)
                except subprocess.TimeoutExpired:
                    p.terminate()
                    try: p.wait(timeout=15)
                    except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=5)
                    state.update(status='STOPPED_INCOMPLETE',failed_root=str(target),reason='native controller exceeded its bounded window')
                    save(out/'driver.json',state)
                    raise TimeoutError('owned shard controller deadline exhausted')
            if code!=0:
                state.update(status='STOPPED_INCOMPLETE',failed_root=str(target),returncode=code)
                save(out/'driver.json',state)
                raise RuntimeError('native shard failed; no automatic write retry: '+str(target))
            result=load(target/'selected-scope-receipt.json')
            if result['status']!='COMPLETE_SELECTED_SCOPE': raise AssertionError('native shard did not qualify')
            state['completed_shards'].append(str(target)); save(out/'driver.json',state)
    state.update(status='UNIFORM_BASELINE_SIX_SHARDS_COMPLETE',finished=time.time(),
                 full_study='INCOMPLETE; geometry, ordering, continuous, recovery, online, retuning and filters remain')
    from vectorbench.reporting.baseline_aggregate import aggregate
    state['aggregate']=aggregate(state['completed_shards'],out)
    save(out/'driver.json',state)
    return state


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--setup-root',type=Path,required=True); p.add_argument('--out',type=Path,required=True)
    a=p.parse_args()
    try: print(run(a.setup_root,a.out))
    except BaseException:
        import traceback
        save(a.out/'driver-error.json',{'status':'STOPPED_INCOMPLETE','traceback':traceback.format_exc()})
        raise
