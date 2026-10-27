"""Verify the delivered Markdown against completed measurements and fixed clock."""
import argparse
from collections import Counter
from datetime import datetime
from pathlib import Path
import re
import shutil
import subprocess
import time
from zoneinfo import ZoneInfo

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.evidence.registry import Registry


def signed(value):
    return f'{value:+.3f}'.replace('-', '−')


def verify(root):
    auth = load(root / 'authorization.json')
    audit = load(root / 'independent-completion-audit.json')
    supplement = load(root / 'cohort-supplement/supplement-receipt.json')
    tests = load(root / 'postmeasurement-tests.json')
    source = load(root / 'source-gate-diagnostic/receipt.json')
    for result in (audit, supplement, tests, source):
        assert result['status'] == 'PASS'
    assert tests['tests'] == 14 and not any(tests[k] for k in ('failures', 'errors', 'skipped'))
    assert tests['log_sha256'] == sha(root / 'postmeasurement-tests.log')
    assert supplement['script_sha256'] == sha('scripts/supplement_twelve_report.py')
    assert source['script_sha256'] == sha('scripts/check_twelve_source_gate.py')
    for name, digest in supplement['outputs'].items():
        assert sha(root / 'cohort-supplement' / name) == digest
    for name, digest in load(root / 'manifest.json')['sources'].items():
        assert sha(name) == digest and sha(root / 'source' / name) == digest
    assert not Registry(root / 'admission.sqlite').snapshot()['leases']
    assert not Registry(root / 'source-gate-diagnostic/admission.sqlite').snapshot()['leases']
    guardian = load(root / 'guardian-receipt.json')
    assert guardian['status'] == 'RECONCILED'
    assert not any(v.get('cleanup', {}).get('errors') for v in guardian['cleanup'])
    running = subprocess.run(['docker', 'ps', '--filter', 'name=ivu-', '--format', '{{.Names}}'],
                             check=True, capture_output=True, text=True).stdout.splitlines()
    assert not running, 'running ivu container remains'
    folder = Path(audit['report']).parent
    finals = {v['id']: v for v in load(folder / 'finals.json')}
    trajectories = load(folder / 'trajectories.json')
    batches = load(folder / 'batch-comparisons.json')
    filters = load(folder / 'filters.json'); curves = load(folder / 'ef-curves.json')
    assert (len(finals), len(trajectories), len(batches), len(filters), len(curves)) == (66, 316, 18, 64, 24)
    assert audit['fresh_builds'] == 138 and audit['queries'] == 190000 and audit['native_nodes'] == 233
    body = Path('docs/real-vectors/12h/results.md').read_text()
    checked_rows = 0

    def require(row):
        nonlocal checked_rows
        assert row in body, 'Markdown value differs: ' + row
        checked_rows += 1

    for db, label in [('postgres', 'pgvector'), ('qdrant', 'Qdrant')]:
        for operation, name in [('churn', 'churn'), ('upsert', '같은 ID upsert')]:
            v = finals[f'msmarco-s11-{db}-{operation}-long-uniform']; d = v['fresh_delta100']
            values = v['fresh_repeat_range100']
            require(f"| {label} / {name} | {100*v['recall100']:.3f} | {100*min(values):.4f}–{100*max(values):.4f} | {signed(d['mean_pp'])} | [{signed(d['lower_pp'])}, {signed(d['upper_pp'])}] |")
        for dataset, title in [('msmarco', 'MS MARCO'), ('wikipedia', 'Wikipedia')]:
            key = f'{dataset}-s11-{db}-insert-r100-w5'
            v = finals[key]; t = next(x for x in trajectories if x['id'] == key and x['step'] == 5)
            require(f"| {title} / {label} | {signed(t['matched_initial_delta100']['mean_pp'])} | {100*v['recall100']:.3f} | {100*v['fresh_mean100']:.3f} | {signed(v['fresh_delta100']['mean_pp'])} |")
            changes = [next(b['delta100']['mean_pp'] for b in batches if b['one'] == f'{dataset}-s11-{db}-{op}-r{ratio}-w1')
                       for op, ratio in [('insert', 100), ('delete', 50), ('churn', 50), ('upsert', 50)]]
            require(f"| {title} / {label} | " + ' | '.join(signed(x) for x in changes) + ' |')
        for geometry in ('near', 'far'):
            v = finals[f'msmarco-s11-{db}-insert-r100-w5-{geometry}']; d = v['fresh_delta100']
            require(f"| {label} / {geometry} | {100*v['recall100']:.3f} | {100*v['fresh_mean100']:.3f} | {signed(d['mean_pp'])} | [{signed(d['lower_pp'])}, {signed(d['upper_pp'])}] |")
        final_point = next(v['point'] for v in curves if v['scope'] == f'msmarco-s11-{db}-upsert-r50-w5:step5')
        for ef in ((100, 256, 1000) if db == 'postgres' else (100, 256, 512)):
            inc = next(v['point'] for v in curves if v['scope'] == f'msmarco-s11-{db}-upsert-r50-w5:step5' and v['ef'] == ef)
            fresh = next(v['point'] for v in curves if v['scope'].startswith(f'msmarco-s11-{db}-')
                         and v['scope'].endswith('-r0') and v['ef'] == ef
                         and v['point']['ledger_sha256'] == final_point['ledger_sha256'])
            require(f"| {label} | {ef} | {100*inc['recall100']:.3f} | {100*fresh['recall100']:.3f} | {signed(100*(inc['recall100']-fresh['recall100']))} |")
    assert sum(v['eligible_count'] > 0 for v in filters) == 56
    assert all(v['recall10'] == v['recall100'] == 1 for v in filters if v['eligible_count'])
    assert all(v['recall100'] is None and v['empty_correct'] for v in filters if not v['eligible_count'])
    paths = Counter()
    for value in filters:
        path = value['path']
        paths[path.get('classification', '/'.join(path.get('classifications', [])))] += 1
    assert paths == {'NATIVE_FILTERED_EXACT': 32, 'HNSW': 24, 'NATIVE_FILTERED_PLAIN_OR_MIXED': 8}
    plan = Path('docs/real-vectors/12h/plan.md').read_text().split('## 승인 후 완료 판정')[1]
    assert plan.count('- [x]') == 7 and '- [ ]' not in plan
    documents = [Path(p) for p in ('README.md', 'docs/real-vectors/12h/results.md',
                 'docs/real-vectors/12h/execution.md', 'docs/real-vectors/12h/plan.md')]
    links = 0
    for path in documents:
        for target in re.findall(r'\]\(([^)]+)\)', path.read_text()):
            if '://' in target or target.startswith('#'):
                continue
            assert (path.parent / target.split('#')[0]).exists(), (path, target)
            links += 1
    evidence = [root / name for name in ('independent-completion-audit.json',
        'cohort-supplement/supplement-receipt.json', 'postmeasurement-tests.json',
        'postmeasurement-tests.log', 'source-gate-diagnostic/receipt.json', 'guardian-receipt.json')]
    evidence += [folder / name for name in ('completion.json', 'finals.json', 'trajectories.json',
                 'batch-comparisons.json', 'filters.json', 'ef-curves.json')]
    files = documents + evidence + [Path(__file__), Path('scripts/supplement_twelve_report.py'),
        Path('scripts/check_twelve_source_gate.py'), Path('tests/test_twelve_report_supplement.py'),
        Path('tests/test_twelve_completion_gates.py')]
    finished = time.time()
    assert finished < auth['deadline']
    receipt = {'status': 'PASS', 'scope': 'APPROVED_12H_PLAN_ONLY', 'finished_at': finished,
        'finished_kst': datetime.fromtimestamp(finished, ZoneInfo('Asia/Seoul')).isoformat(),
        't0': auth['t0'], 'deadline': auth['deadline'], 'elapsed_seconds': finished-auth['t0'],
        'histories': 66, 'history_points': 316, 'fresh_builds': 138, 'filter_probes': 64,
        'ef_probes': 24, 'quality_points': 542, 'queries': 190000, 'native_nodes': 233,
        'markdown_numeric_rows_checked': checked_rows, 'local_links_checked': links,
        'filter_paths': dict(paths), 'postmeasurement_tests': 14, 'running_ivu_containers': running,
        'host_free_bytes': shutil.disk_usage('.').free,
        'owned_result_logical_bytes': sum(p.stat().st_size for p in Path('real-results').rglob('*') if p.is_file()),
        'sha256': {str(p): sha(p) for p in files}, 'broader_research': 'INCOMPLETE_AS_APPROVED'}
    save(root / 'delivery-receipt.json', receipt)
    return {k: v for k, v in receipt.items() if k != 'sha256'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('root', type=Path)
    print(verify(parser.parse_args().root.resolve()))
