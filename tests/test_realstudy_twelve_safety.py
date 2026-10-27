import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import numpy as np
from vectorbench.evidence.contracts import save, sha
from vectorbench.workloads.events import Event, Write
from vectorbench.orchestration.guardian import clean_active
from vectorbench.evidence.registry import Registry
from vectorbench.workloads.writes import wave
from vectorbench.experiments.real_vectors.twelve_hour.twelve_contract import validate_window


class TwelveSafetyTests(unittest.TestCase):
    def test_failed_ended_attempt_still_gets_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); r=Registry(root/'admission.sqlite')
            r.claim('history:test','r','hash'); r.finish('history:test','r','failed')
            out=root/'cells/history__test/r'; out.mkdir(parents=True)
            with patch('vectorbench.orchestration.guardian.release_journal',return_value={'errors':[],'released':['owned']} ) as release:
                value=clean_active(root)
            release.assert_called_once_with(out)
            self.assertEqual(value[0]['cleanup']['released'],['owned'])
            self.assertEqual(r.snapshot()['attempts'][0]['outcome'],'failed')
            self.assertEqual(json.loads((out/'guardian-cleanup.json').read_text())['status'],'CLEANED')

    def test_copy_failure_preserves_delete_ack_and_never_replays(self):
        class Adapter:
            name='postgres'
            def __init__(self): self.calls=[]
            def sql(self,statement):
                self.calls.append(statement)
                if statement.startswith('COPY'): raise RuntimeError('injected transport interruption')
            def delete(self,ids): self.sql('DELETE FROM items WHERE id=1;')
            def insert(self,ids,vectors,attrs): self.sql('COPY items FROM STDIN;\n2\n\\.')
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder); a=Adapter()
            with self.assertRaisesRegex(RuntimeError,'injected'):
                wave(a,Event((1,),(Write(2,0,0,0,0),),'churn'),np.zeros((1,768),dtype=np.float32),out,1)
            value=json.loads((out/'write-1.json').read_text())
            self.assertEqual(value['status'],'OUTCOME_REQUIRES_RECONCILIATION')
            self.assertEqual([r['status'] for r in value['requests']],['ACKNOWLEDGED','INTENT'])
            self.assertEqual(len(a.calls),2)

    def test_window_cannot_be_extended_or_authorization_replaced(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'auth.json'; save(p,{'t0':100,'deadline':43300})
            m={'authorization':{'path':str(p),'sha256':sha(p)},'execution_window_end':43300,'task_seconds':43200}
            validate_window(m)
            with self.assertRaisesRegex(AssertionError,'reset'): validate_window({**m,'execution_window_end':43301})
            with self.assertRaisesRegex(AssertionError,'phase'): validate_window({**m,'deadlines':{'UPDATE':43301}})
            save(p,{'t0':101,'deadline':43301})
            with self.assertRaisesRegex(AssertionError,'changed'): validate_window(m)

if __name__=='__main__': unittest.main()

class FilterBoundaryTests(unittest.TestCase):
    def test_filtered_seqscan_and_wrong_setting_rejected(self):
        from vectorbench.evaluation.twelve_filters import filtered_plan
        for setting,node in [('off',{'Node Type':'Seq Scan'}),('on',{'Node Type':'Index Scan','Index Name':'items_hnsw'})]:
            with self.assertRaises(AssertionError): filtered_plan(setting+'\n'+json.dumps([{'Plan':node}]))

    def test_partial_query_oracle_mapping_is_verified_from_raw(self):
        from vectorbench.reporting.twelve_report import scores
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder);truth=np.array([[1,2],[3,4],[5,6]])
            np.savez_compressed(out/'oracle.npz',truth=truth)
            np.savez_compressed(out/'raw.npz',returned=np.array([[1,2],[5,6]]),returned_lengths=np.array([2,2]))
            p={'selected_read':str(out),'raw_sha256':sha(out/'raw.npz'),'oracle':{'path':str(out/'oracle.npz'),'sha256':sha(out/'oracle.npz'),'query_subset_indices':[0,2]},
               'queries':2,'recall10':1.,'status':'PASS','ann':{'status':'PASS'}}
            np.testing.assert_array_equal(scores(p,10),[1.,1.])
            p['oracle']['query_subset_indices']=[0,1]
            with self.assertRaisesRegex(AssertionError,'raw recall'): scores(p,10)
