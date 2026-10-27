"""Observe the existing owner; run the independent audit only after native completion."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
from vectorbench.evidence.contracts import load,save
from scripts.audit_twelve_completion import audit


def main(root):
    auth=load(root/'authorization.json')
    while True:
        state=load(root/'controller.json')
        probe=subprocess.run(['ps','-p',str(state['pid']),'-o','stat=,command='],capture_output=True,text=True)
        alive=probe.returncode==0 and 'vectorbench.orchestration.control' in probe.stdout and not probe.stdout.lstrip().startswith('Z')
        if state['finished']:
            if state['status']!='COMPLETE_SELECTED_SCOPE':
                save(root/'finalizer.json',{'status':'STOPPED_INCOMPLETE','controller_status':state['status'],'time':time.time()})
                return 1
            # The controller writes finished before guardian reconciliation, so wait for that exact receipt.
            guardian=root/'guardian-receipt.json'
            if guardian.exists() and load(guardian).get('status')=='RECONCILED':
                try:
                    result=audit(root)
                    if time.time()>auth['deadline']: raise TimeoutError('completion audit exceeded original deadline')
                    save(root/'independent-completion-audit.json',result)
                    save(root/'finalizer.json',{'status':'PASS','audit':str(root/'independent-completion-audit.json'),'time':time.time()})
                    return 0
                except Exception as exc:
                    save(root/'finalizer.json',{'status':'AUDIT_FAILED','error':repr(exc),'time':time.time()})
                    raise
        elif not alive:
            save(root/'finalizer.json',{'status':'ORPHANED/STOPPED_INCOMPLETE','pid':state['pid'],'time':time.time()})
            return 1
        if time.time()>auth['deadline']:
            save(root/'finalizer.json',{'status':'DEADLINE_EXCEEDED','time':time.time(),'deadline':auth['deadline']})
            return 1
        time.sleep(20)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();sys.exit(main(a.root.resolve()))
