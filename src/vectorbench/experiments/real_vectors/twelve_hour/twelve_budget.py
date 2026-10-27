"""Component accounting from both actual DB smokes; never silently trim the matrix."""
import time
import numpy as np
from vectorbench.evidence.contracts import load,save
from vectorbench.datasets.storage import load_events
from vectorbench.experiments.real_vectors.twelve_hour.twelve_run import path_for
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


def conservative(values):
    if not values: raise AssertionError('missing measured cost component')
    return float(max(values) if len(values)<5 else np.percentile(values,90))


def assess(root,manifest,deps,out):
    auth=validate_window(manifest); now=time.time(); models={}; components=[]
    for db in ('postgres','qdrant'):
        histories=[v for k,v in deps.items() if manifest['nodes'][k]['action']=='history' and manifest['nodes'][k]['db']==db]
        fresh=[v for k,v in deps.items() if manifest['nodes'][k]['action']=='fresh' and manifest['nodes'][k]['db']==db]
        initials=[v for k,v in deps.items() if k.startswith('initial:') and manifest['nodes'][k]['db']==db]
        if len(histories)!=1 or len(fresh)!=3: raise AssertionError('both DB full-long and size costs required')
        points=[p for h in histories for p in h['points']]+[v['point'] for v in fresh]+[p for v in initials for p in (v['before_restart'],v['after_restart'])]
        measured={q:conservative([p['finished_at']-p['started_at'] for p in points if p['queries']==q]) for q in (200,600)}
        verify=conservative([v['finished_at']-v['started_at'] for h in histories for v in h['unmeasured_wave_verifications']])
        writes=[w for h in histories for w in h['writes'] if w['written']]
        # Churn has deletion plus insertion. Use its total cost per inserted row also for upsert.
        # Doubling protects unmeasured transactional replacement and large-batch overhead.
        per_write=2*conservative([w['acknowledgement_s']/w['written'] for w in writes])
        per_delete=conservative([sum(r.get('finished_at',r['started_at'])-r['started_at'] for r in w['requests'] if r['kind']=='DELETE')/max(1,w['deleted']) for w in writes]) if db=='postgres' else per_write
        builds={v['point']['count']:v['setup']['load_s']+v['setup']['build_s'] for v in fresh}
        if set(builds)!={5000,10000,20000}: raise AssertionError('fresh size smoke missing')
        model={'point200_s':measured[200],'point600_s':measured[600],'verify_s':verify,'write_row_s':per_write,
               'delete_row_s':per_delete,'fresh_sizes_s':builds,'node_overhead_s':30,
               'filtered_point_s':2*measured[200]+5,'initial_s':conservative([v['elapsed_s'] for v in initials])}
        models[db]=model
    completed=set(deps)
    for key,spec in manifest['nodes'].items():
        if key in completed or spec['action'] in {'context','barrier','calibration','cost_gate','report'}: continue
        model=models[spec['db']]; seconds=model['node_overhead_s']
        context=deps['study_context:'+spec['dataset']]
        if spec['action']=='history':
            events=load_events(path_for(context,spec)['events'])
            seconds+=sum(model['point600_s'] if q==600 else model['point200_s'] for q in spec['queries_by_step'].values())
            seconds+=(spec['waves']+1-len(spec['quality_steps']))*model['verify_s']
            for e in events:
                seconds+=len(e.writes)*model['write_row_s']+len(e.deletes)*model['delete_row_s']
        elif spec['action']=='fresh':
            n=spec['state']['count'] if spec['family']=='baseline' else 20000 if spec['family']=='geometry' else 10000
            seconds+=model['fresh_sizes_s'][n]+model['point600_s']
        elif spec['action']=='initial': seconds+=model['initial_s']
        components.append({'node':key,'seconds':seconds})
    # All 64 filter and 24 ef probes remain after admission.
    extra=sum(32*m['filtered_point_s']+12*m['point200_s'] for m in models.values())
    remaining=(sum(v['seconds'] for v in components)+extra+1800)*1.2
    expected=now+remaining
    result={'status':'PASS' if expected<=auth['admission_deadline'] and now<=auth['preparation_deadline'] else 'BUDGET_CONFLICT',
            'models':models,'remaining_components':components,'extra_probe_seconds':extra,'margin':1.2,'report_seconds':1800,
            'predicted_remaining_s':remaining,'predicted_finish':expected,'assessed_at':now,
            'admission_deadline':auth['admission_deadline'],'deadline':auth['deadline'],
            'scope':'component forecast, not a statistical runtime guarantee; no automatic scope reduction'}
    save(out/'cost-gate.json',result)
    if result['status']!='PASS': raise RuntimeError('twelve-hour cost admission failed; preserve scope and request user decision')
    return result
