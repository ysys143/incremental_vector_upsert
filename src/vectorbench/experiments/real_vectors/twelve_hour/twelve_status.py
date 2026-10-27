"""Read-only status: PID and native runs take precedence over cached RUNNING JSON."""
import argparse
import json
import subprocess
from pathlib import Path
import time
import dagster as dg
from vectorbench.evidence.contracts import load
from vectorbench.evidence.registry import Registry


def status(root):
    auth=load(root/'authorization.json'); state=load(root/'controller.json') if (root/'controller.json').exists() else {}
    pid=state.get('pid')
    p=subprocess.run(['ps','-p',str(pid),'-o','stat=,command='],capture_output=True,text=True) if pid else None
    alive=bool(p and p.returncode==0 and p.stdout.strip() and not p.stdout.lstrip().startswith('Z') and 'vectorbench.orchestration.control' in p.stdout)
    actual=state.get('status','IMPLEMENTING')
    if actual=='RUNNING' and not alive: actual='ORPHANED/STOPPED_INCOMPLETE'
    counts={}; running=[];failed=[]
    if (root/'dagster-home/dagster.yaml').exists():
        with dg.DagsterInstance.from_config(str(root/'dagster-home')) as instance:
            for run in instance.get_runs(filters=dg.RunsFilter(tags={'realstudy/campaign':root.name})):
                counts[run.status.value]=counts.get(run.status.value,0)+1
                if run.status in {dg.DagsterRunStatus.STARTED,dg.DagsterRunStatus.STARTING}: running.append(run.run_id)
                if run.status in {dg.DagsterRunStatus.FAILURE,dg.DagsterRunStatus.CANCELED}: failed.append(run.run_id)
    attempts=Registry(root/'admission.sqlite').snapshot() if (root/'admission.sqlite').exists() else {}
    return {'status':actual,'controller_alive':alive,'cached_status':state.get('status'),'native_counts':counts,
            'running_native_run_ids':running,'failed_native_run_ids':failed,'leases':attempts.get('leases',[]),
            'elapsed_s':time.time()-auth['t0'],'remaining_s':max(0,auth['deadline']-time.time()),'deadline':auth['deadline']}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();print(json.dumps(status(a.root.resolve()),indent=2))
