"""The approved twelve-hour clock is inherited, never restarted by a child."""
from pathlib import Path
from vectorbench.evidence.contracts import load, sha


def validate_window(manifest):
    reference = manifest['authorization']
    if sha(reference['path']) != reference['sha256']:
        raise AssertionError('execution authorization changed')
    auth = load(reference['path'])
    if auth['deadline'] - auth['t0'] != 43200:
        raise AssertionError('authorized wall budget must be twelve hours')
    if manifest['execution_window_end'] != auth['deadline']:
        raise AssertionError('successor cannot reset the original deadline')
    if not 0 < manifest['task_seconds'] <= 43200:
        raise AssertionError('worker cap exceeds global budget')
    if any(end > auth['deadline'] for end in manifest.get('deadlines', {}).values()):
        raise AssertionError('phase exceeds global deadline')
    return auth
