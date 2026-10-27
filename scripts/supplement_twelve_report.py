"""Post-completion cohort reporting; never changes or reruns a DB measurement."""
import argparse
from pathlib import Path
import time

import numpy as np

from vectorbench.reporting.baseline_report import paired_interval
from vectorbench.evidence.contracts import load, save, sha
from vectorbench.evidence.registry import Registry
from vectorbench.reporting.twelve_report import scores


def cohort_rows(codes, names, values, reported, key, fresh=None):
    codes = np.asarray(codes)
    if codes.shape != values[100].shape or not np.isin(codes, np.arange(len(names))).all():
        raise AssertionError('cohort/query alignment differs')
    nonempty = {name for code, name in enumerate(names) if np.any(codes == code)}
    if set(reported) != nonempty:
        raise AssertionError('reported cohort membership differs')
    rows = []
    for code, name in enumerate(names):
        chosen = np.flatnonzero(codes == code)
        row = {'cohort': name, 'queries': len(chosen)}
        for k in (10, 100):
            value = float(values[k][chosen].mean()) if len(chosen) else None
            row['recall' + str(k)] = value
            if len(chosen) and not np.isclose(value, reported[name]['recall' + str(k)], atol=1e-12, rtol=0):
                raise AssertionError('cohort scores differ from measured groups')
            if fresh is not None:
                repeats = [float(v[chosen].mean()) for v in fresh[k]] if len(chosen) else []
                row['fresh_repeats' + str(k)] = repeats
                row['fresh_min' + str(k)] = min(repeats) if repeats else None
                row['fresh_max' + str(k)] = max(repeats) if repeats else None
                row['fresh_delta' + str(k)] = paired_interval(
                    values[k][chosen] - fresh[k][:, chosen].mean(axis=0), key + ':' + name + ':' + str(k)
                ) if len(chosen) else None
        rows.append(row)
    if sum(row['queries'] for row in rows) != len(codes):
        raise AssertionError('cohort counts do not cover queries')
    return rows


def supplement(root):
    audit_path = root / 'independent-completion-audit.json'
    audit = load(audit_path)
    controller = load(root / 'controller.json')
    if audit['status'] != 'PASS' or not controller['finished'] or controller['status'] != 'COMPLETE_SELECTED_SCOPE':
        raise AssertionError('complete native run and independent audit required')
    manifest = load(root / 'manifest.json')
    auth = load(root / 'authorization.json')
    if time.time() > auth['deadline']:
        raise TimeoutError('original twelve-hour deadline exceeded')
    if Registry(root / 'admission.sqlite').snapshot()['leases']:
        raise AssertionError('postprocessing cannot overlap DB or preparation work')
    out = root / 'cohort-supplement'
    out.mkdir(exist_ok=False)
    checked = {r['node']: r['run_id'] for r in audit['native_checks']}
    if set(checked) != set(manifest['nodes']):
        raise AssertionError('independent audit does not cover manifest')
    receipts = {}; bindings = {}; inputs = {str(audit_path): sha(audit_path)}
    for node, run_id in checked.items():
        path = root / 'cells' / node.replace(':', '__') / run_id / 'receipt.json'
        receipt = load(path); receipts[node] = receipt['result']; inputs[str(path)] = sha(path)
        for name, digest in receipt['artifacts'].items():
            resolved = str((root / name).resolve())
            if resolved in bindings and bindings[resolved] != digest:
                raise AssertionError('conflicting artifact hashes')
            bindings[resolved] = digest
    for node, spec in manifest['nodes'].items():
        if spec['action'] == 'context':
            for name, digest in receipts[node]['input_bindings'].items():
                resolved = str(Path(name).resolve())
                if resolved in bindings and bindings[resolved] != digest:
                    raise AssertionError('conflicting input hashes')
                bindings[resolved] = digest
    fresh = {}
    for node, spec in manifest['nodes'].items():
        if spec['action'] != 'fresh':
            continue
        value = receipts[node]
        key = (value['dataset'], value['seed'], value['db'], value['family'], value['state'])
        fresh.setdefault(key, []).append(value)
    for key, values in fresh.items():
        values.sort(key=lambda v: v['repeat'])
        if [v['repeat'] for v in values] != [0, 1, 2]:
            raise AssertionError('three independent fresh builds required')
    checkpoint_rows = []; final_rows = []; cached_fresh = {}; cohort_hashes = {}
    for node, spec in manifest['nodes'].items():
        if spec['action'] != 'history':
            continue
        value = receipts[node]; path = Path(value['path']['cohorts']).resolve()
        digest = sha(path)
        if digest != bindings.get(str(path)):
            raise AssertionError('cohort input has no matching native artifact binding')
        cohort_hashes[str(path)] = digest
        with np.load(path) as data:
            labels = {step: data[step] for step in data.files}
        context = receipts['study_context:' + spec['dataset']]
        for step, point in zip(value['steps'], value['points']):
            indexes = np.asarray(point['oracle']['query_subset_indices'])
            expected_indexes = np.arange(600) if point['queries'] == 600 else np.asarray(context['subset_indices'])
            if indexes.ndim != 1 or not np.array_equal(indexes, expected_indexes):
                raise AssertionError('cohort query subset differs from frozen selection')
            if len(indexes) != point['queries'] or len(np.unique(indexes)) != len(indexes):
                raise AssertionError('query subset is not one-to-one')
            codes = labels[str(step)][indexes]
            per_query = {k: scores(point, k) for k in (10, 100)}
            rebuilt = None
            if step == spec['waves']:
                key = (spec['dataset'], spec['seed'], spec['db'], spec['family'], point['ledger_sha256'])
                controls = fresh[key]
                for control in controls:
                    if any(point[field] != control['point'][field] for field in ('metric', 'count', 'query_sha256', 'vector_state_sha256', 'ledger_sha256')):
                        raise AssertionError('inc/fresh data, query, version or metric differs')
                if key not in cached_fresh:
                    cached_fresh[key] = {k: np.stack([scores(v['point'], k) for v in controls]) for k in (10, 100)}
                rebuilt = cached_fresh[key]
            row = {'id': spec['id'], 'step': step, 'queries': point['queries'], 'cohort_file_sha256': digest,
                   'groups': cohort_rows(codes, value['path']['cohort_names'], per_query, point['groups'], spec['id'] + ':' + str(step), rebuilt)}
            checkpoint_rows.append(row)
            if rebuilt is not None:
                final_rows.append(row)
    if (len(checkpoint_rows), len(final_rows), sum(r['queries'] for r in checkpoint_rows)) != (316, 66, 89600):
        raise AssertionError('cohort reporting inventory differs')
    save(out / 'cohort-checkpoints.json', checkpoint_rows)
    save(out / 'cohort-finals.json', final_rows)
    lines = ['# 집단별 표본 수와 최종 inc–fresh 비교', '',
             '검증된 원시 검색 결과를 재분석했다. 새 DB 측정이나 설정 변경은 없다.',
             '316개 이력 측정 시점의 모든 집단 수는 cohort-checkpoints.json, 66개 최종 비교는 cohort-finals.json에 있다.', '',
             '집단은 exact top-10 이웃 중 갱신 대상의 비율로 구분한다. 20% 이하는 기존/비대상, 80% 이상은 신규/갱신 대상, 그 사이는 boundary다.',
             '삭제 조건은 초기 exact 이웃 중 삭제된 비율을 사용한다. read-only는 unchanged 집단 하나다.',
             '집단은 시점에 따라 달라질 수 있다. 공간적 경계나 자연 의미 범주를 직접 관측한 분류가 아니다.',
             '최종 비교는 같은 600질의를 집단별로 나눈다. n=0은 N/A다. 신뢰구간은 2,000회 조건부 질의 bootstrap이며 다중 비교 보정이나 seed 수준 일반화가 아니다.', '',
             '| 이력 | 집단 | 질의 수 | inc R@100 (%) | fresh 3회 최소–최대 (%) | inc−fresh (%p) | 95% 질의 bootstrap (%p) |',
             '|---|---|---:|---:|---:|---:|---:|']
    for row in final_rows:
        for group in row['groups']:
            d = group['fresh_delta100']
            metrics = f"{100*group['recall100']:.4f} | {100*group['fresh_min100']:.4f}–{100*group['fresh_max100']:.4f} | {d['mean_pp']:+.4f} | [{d['lower_pp']:+.4f}, {d['upper_pp']:+.4f}]" if d else 'N/A | N/A | N/A | N/A'
            lines.append(f"| {row['id']} | {group['cohort']} | {group['queries']} | {metrics} |")
    (out / 'cohort-report.md').write_text('\n'.join(lines) + '\n')
    finished = time.time()
    if finished > auth['deadline']:
        raise TimeoutError('cohort report exceeded original twelve-hour deadline')
    result = {'status': 'PASS', 'finished_at': finished, 'deadline': auth['deadline'], 'history_points': 316,
              'final_histories': 66, 'queries': 89600, 'input_receipts': inputs, 'cohort_inputs': cohort_hashes,
              'script_sha256': sha(Path(__file__)), 'outputs': {p.name: sha(p) for p in out.iterdir() if p.is_file()},
              'scope': 'Post-completion reporting only; exact native data/query/version pairing verified; no DB measurement'}
    save(out / 'supplement-receipt.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('root', type=Path)
    result = supplement(parser.parse_args().root.resolve())
    print({key: result[key] for key in ('status', 'history_points', 'final_histories', 'queries', 'finished_at')})
