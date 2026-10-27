"""Pre-evaluation sensitivity and measured resource summaries."""
import json
import math
from pathlib import Path
import numpy as np
from vectorbench.evidence.contracts import load,save,sha


def prior_sensitivity(refs,out):
    from .report import raw_scores
    rows=[]
    for ref in refs:
        if sha(ref['path'])!=ref['sha256']:raise AssertionError('historical calibration changed')
        group=load(ref['path'])
        if group['status']!='MEASURED':continue
        for v in group['points']:
            scores=raw_scores(v['point'])[100]
            sd=float(np.std(scores,ddof=1))
            # The SD of X-Y is at most SD(X)+SD(Y). No actual cross-path effect is claimed.
            half=1.96*2*sd/math.sqrt(600)*100
            rows.append({'group':ref['path'],'group_sha256':ref['sha256'],'ef':v['ef'],
                         'calibration_queries':len(scores),'query_sd':sd,'evaluation_queries':600,
                         'conservative_normal_halfwidth_pp':half,'halfwidth_le_0_5pp':half<=.5})
    if not rows:raise AssertionError('prior calibration query evidence missing')
    result={'status':'PASS','rows':rows,'target_effect_pp':.5,
            'scope':'pre-evaluation variability diagnostic using previous held-out calibration; normal approximation and a covariance bound, not measured power for clustered paths; previous indexes and inputs differ'}
    save(out/'prior-sensitivity.json',result);return result


def resources(root,out):
    rows=[]
    for path in (root/'cells').glob('*/*/receipt.json'):
        r=load(path)
        if r.get('status')!='PASS' or r.get('node')=='report:all':continue
        origin={'kind':'current','root':str(root),'node':r['node'],'run_id':r['run_id']}
        evidence_path=path
        proof_path=path.parent/'adoption-proof.json'
        if proof_path.exists():
            proof=load(proof_path);ref=proof['reference']
            predecessor=Path(ref['root']).resolve()
            evidence_path=predecessor/'cells'/r['node'].replace(':','__')/ref['run_id']/'receipt.json'
            if (ref['node']!=r['node'] or sha(evidence_path)!=ref['receipt_sha256']
                    or proof['result'].get('original_native_timing')!=ref.get('original_native_timing')):
                raise AssertionError('adopted resource origin differs from the native proof: '+r['node'])
            old=load(evidence_path)
            if old.get('status')!='PASS' or old['run_id']!=ref['run_id'] or old['resource']!=r['resource']:
                raise AssertionError('adopted resource receipt is not the original successful node')
            origin={'kind':'adopted','root':str(predecessor),'node':r['node'],'run_id':old['run_id'],
                    'receipt_sha256':sha(evidence_path)}
            r=old
        sample_paths=list(evidence_path.parent.rglob('resource-samples.jsonl'))
        rss=[];disk=[];free=[];samples=0
        for p in sample_paths:
            for line in p.read_text().splitlines():
                sample=json.loads(line);samples+=1
                if 'summed_process_rss_bytes' in sample:rss.append(sample['summed_process_rss_bytes'])
                if 'database_disk_bytes' in sample:disk.append(sample['database_disk_bytes'])
                if 'host_free_bytes' in sample:free.append(sample['host_free_bytes'])
        if r['resource']=='db' and (not rss or not disk):raise AssertionError('DB RSS/disk samples missing: '+r['node'])
        rows.append({'node':r['node'],'started_at':r['started'],'finished_at':r['finished'],
                     'elapsed_s':r['finished']-r['started'],'worker_peak_rss_bytes':r['worker_peak_rss_bytes'],
                     'origin':origin,
                     'sampled_database_summed_rss_peak_bytes':max(rss) if rss else None,
                     'sampled_database_disk_peak_bytes':max(disk) if disk else None,
                     'minimum_host_free_bytes':min(free) if free else None,'docker_samples':samples})
    result={'status':'PASS','rows':rows,'scope':'worker OS high-water RSS; DB process VmRSS sums sampled roughly every five Docker samples and may count shared pages repeatedly; sampled disk is not continuous peak'}
    save(out/'resource-summary.json',result);return result
