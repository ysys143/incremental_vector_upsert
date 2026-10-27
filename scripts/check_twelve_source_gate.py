"""Exercise the frozen-source gate in isolated native runs after DB completion."""
import argparse
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
import time

import dagster as dg

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.orchestration.engine import build
from vectorbench.evidence.registry import Registry


def check(campaign):
    audit = load(campaign / 'independent-completion-audit.json')
    auth = load(campaign / 'authorization.json')
    if audit['status'] != 'PASS' or time.time() >= auth['deadline']:
        raise AssertionError('completed campaign within original deadline required')
    if Registry(campaign / 'admission.sqlite').snapshot()['leases']:
        raise AssertionError('measurement still active')
    root = campaign / 'source-gate-diagnostic'
    root.mkdir(exist_ok=False)
    home = root / 'dagster-home'; home.mkdir()
    (home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    source = root / 'frozen-fixture.py'
    source.write_text('VALUE = 1\n')
    original = sha(source)
    manifest = {'mode': 'orchestration-fixture', 'nodes': {
        'source:a': {'id': 'a', 'role': 'source', 'resource': 'cpu',
                     'action': 'fixture', 'phase': 'CHECK', 'deps': []}},
        'sources': {str(source): original},
        'measurement_python': '/opt/homebrew/bin/python3',
        'deadlines': {'CHECK': min(time.time() + 180, auth['deadline'])}}
    save(root / 'manifest.json', manifest)
    defs = build(root)
    with dg.DagsterInstance.from_config(str(home)) as instance, (root / 'native.log').open('w') as log:
        with redirect_stdout(log), redirect_stderr(log):
            good = dg.materialize([*defs.assets, *defs.asset_checks],
                selection=dg.AssetSelection.assets('source'), partition_key='a',
                instance=instance, raise_on_error=False)
            if not good.success:
                raise AssertionError('unchanged source did not pass native control')
            before = Registry(root / 'admission.sqlite').snapshot()
            source.write_text('VALUE = 2\n')
            bad = dg.materialize([*defs.assets, *defs.asset_checks],
                selection=dg.AssetSelection.assets('source'), partition_key='a',
                instance=instance, raise_on_error=False)
            events = instance.all_logs(bad.run_id)
            messages = '\n'.join(e.message for e in events)
            for event in events:
                if event.dagster_event and event.dagster_event.is_step_failure:
                    error = event.dagster_event.step_failure_data.error
                    if error:
                        messages += '\n' + error.to_string()
            if bad.success or 'frozen workload sources changed' not in messages:
                raise AssertionError('source mutation failed to reach its rejection path')
            if Registry(root / 'admission.sqlite').snapshot() != before:
                raise AssertionError('mutated source reached worker registration')
            if any(e.asset_materialization for e in instance.all_logs(bad.run_id)):
                raise AssertionError('mutated source materialized a result')
    value = {'status': 'PASS', 'scope': 'ISOLATED_NATIVE_GATE_ONLY; no DB measurements',
             'positive_run': good.run_id, 'intentional_failed_run': bad.run_id,
             'original_source_sha256': original, 'mutated_source_sha256': sha(source),
             'worker_started_after_mutation': False, 'finished_at': time.time(),
             'deadline': auth['deadline'], 'script_sha256': sha(Path(__file__))}
    if value['finished_at'] > auth['deadline']:
        raise TimeoutError('original deadline exceeded')
    save(root / 'receipt.json', value)
    return value


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('campaign', type=Path)
    print(check(parser.parse_args().campaign.resolve()))
