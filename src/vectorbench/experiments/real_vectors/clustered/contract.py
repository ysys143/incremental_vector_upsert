"""Pure-stdlib clock and authorization validation for both Python runtimes."""
from pathlib import Path
import time
from vectorbench.evidence.contracts import load,sha


def validate_window(manifest):
    ref=manifest['authorization']
    if sha(ref['path'])!=ref['sha256']:raise AssertionError('authorization changed')
    auth=load(ref['path']);root=Path(ref['path']).parent
    if auth['deadline']-auth['t0']!=43200 or manifest['execution_window_end']!=auth['deadline']:
        raise AssertionError('original twelve-hour deadline changed')
    if sha(root/'approved-plan.md')!=auth['approved_plan_sha256'] or sha(root/'approved-matrix.json')!=auth['approved_matrix_sha256']:
        raise AssertionError('approved plan/matrix changed')
    policy=manifest.get('completion_policy')
    execution_deadline=auth['deadline']
    if policy:
        if (policy.get('mode')!='FINISH_APPROVED_SCOPE' or policy.get('original_deadline')!=auth['deadline']
                or not policy.get('user_text','').strip() or policy.get('recorded_at',0)<auth['deadline']
                or policy.get('execution_deadline',0)<=policy['recorded_at']):
            raise AssertionError('completion policy must preserve the missed original window')
        execution_deadline=policy['execution_deadline']
    if time.time()>=execution_deadline:raise TimeoutError('execution watchdog window exhausted')
    if any(v>execution_deadline for v in manifest.get('deadlines',{}).values()):raise AssertionError('phase exceeds execution watchdog')
    return {**auth,'execution_deadline':execution_deadline,
            'original_window_status':'MISSED' if policy else 'ORIGINAL_WINDOW'}

