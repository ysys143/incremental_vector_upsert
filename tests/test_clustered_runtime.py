"""Guards for approved clustered execution, independent of actual DB qualification."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
import numpy as np
from vectorbench.workloads.events import Event,Write
from vectorbench.evidence.contracts import Config,sha
from vectorbench.experiments.pilot.model import packed_results,score
from vectorbench.experiments.real_vectors.clustered.runtime import write_wave
from vectorbench.experiments.real_vectors.clustered.campaign import catalogue
from vectorbench.experiments.real_vectors.clustered.tuning import select
from vectorbench.experiments.real_vectors.clustered.report import raw_scores


class Adapter:
    name='postgres'
    def __init__(self,fail=False):self.statements=[];self.fail=fail
    def sql(self,text):
        self.statements.append(text)
        if self.fail and text.startswith('COPY '):raise RuntimeError('connection lost')
        return ''
    def insert(self,ids,vectors,attrs):
        for i in range(0,len(ids),self.batch):
            rows='\n'.join(str(v) for v in ids[i:i+self.batch])
            self.sql('COPY items FROM STDIN;\n'+rows+'\n\\.')


class ClusteredRuntimeTests(unittest.TestCase):
    def test_one_commit_multiple_transport_chunks(self):
        with tempfile.TemporaryDirectory() as folder:
            a=Adapter();p=SimpleNamespace(vectors=np.ones((2001,2),dtype=np.float32))
            e=Event(writes=tuple(Write(i,i,1) for i in range(2001)),operation='insert')
            r=write_wave(a,e,p,Path(folder),1,Config())
            self.assertEqual([x['rows'] for x in r['requests']],[1000,1000,1])
            self.assertEqual(len(r['commits']),1)
            self.assertEqual(a.statements[0],'BEGIN;');self.assertEqual(a.statements[-1],'COMMIT;')

    def test_uncertain_write_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as folder:
            a=Adapter(True);p=SimpleNamespace(vectors=np.ones((2,2),dtype=np.float32))
            e=Event(writes=(Write(0,0,1),),operation='insert')
            with self.assertRaises(RuntimeError):write_wave(a,e,p,Path(folder),1,Config())
            self.assertEqual(sum(s.startswith('COPY ') for s in a.statements),1)
            self.assertNotIn('COMMIT;',a.statements)
            self.assertEqual(json.loads((Path(folder)/'write-1.json').read_text())['status'],'OUTCOME_REQUIRES_RECONCILIATION')

    def test_full_catalogue_and_no_asset_self_dependencies(self):
        matrix=json.loads(Path('docs/real-vectors/clustered-insertion-matrix.json').read_text());nodes=catalogue(matrix)
        self.assertEqual(sum(v['action']=='history' and v['seed']!=7 for v in nodes.values()),48)
        self.assertEqual(sum(v['action']=='fresh' and v['seed']!=7 for v in nodes.values()),120)
        for key,node in nodes.items():
            for dep in node['deps']:
                self.assertIn(dep,nodes);self.assertNotEqual(node['role'],nodes[dep]['role'])
        bad=copy.deepcopy(matrix);bad['histories'].pop()
        with self.assertRaises(AssertionError):catalogue(bad)

    def test_calibration_failure_cannot_select_configuration(self):
        groups=[]
        for build in ([16,100],[32,200]):
            points=[{'state':s,'ef':ef,'point':{'recall10':.98,'recall100':.98,'p95_ms':1.,'shortfall100':0}} for s in range(5) for ef in (100,128,256,512,1000)]
            groups.append({'build':build,'points':points,'build_write_s':1,'peak_memory_usage_bytes':1024**3,'started_at':0})
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(AssertionError):select(groups,Path(d))

    def test_report_rejects_non_ann_before_reading_artifacts(self):
        with self.assertRaises(AssertionError):raw_scores({'status':'PASS','ann':{'status':'UNVERIFIED'},'metric':'ip'})

    def test_report_checks_raw_quality_and_oracle_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);ids=np.arange(101);truth=np.arange(100)[None,:]
            rows=[list(range(100))];np.savez(root/'oracle.npz',ids=ids,truth=truth)
            np.savez(root/'raw.npz',**packed_results(rows))
            p={'status':'PASS','ann':{'status':'PASS'},'metric':'ip','selected_read':d,
               'queries':1,'raw_sha256':sha(root/'raw.npz'),
               'oracle':{'path':str(root/'oracle.npz'),'sha256':sha(root/'oracle.npz')},**score(ids,truth,rows)}
            self.assertEqual(raw_scores(p)[100].tolist(),[1.0])
            wrong=copy.deepcopy(p);wrong['recall100']=.98
            with self.assertRaises(AssertionError):raw_scores(wrong)
            np.savez(root/'oracle.npz',ids=ids,truth=truth[:,::-1])
            with self.assertRaises(AssertionError):raw_scores(p)


if __name__=='__main__':unittest.main()
