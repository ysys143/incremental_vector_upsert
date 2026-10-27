"""A forecast waiver preserves rates and the clock; disk limits are observational."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from vectorbench.evidence.contracts import load,save,sha,GIB
from vectorbench.experiments.real_vectors.clustered.gates import cost_gate


class ForecastExceptionTests(unittest.TestCase):
    def test_scoped_exception_and_rejections(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);out=root/'out';out.mkdir()
            auth={'t0':1000,'deadline':44200};now=43000
            deps={}
            point={'started_at':1,'finished_at':11,'queries':200}
            for db in ('postgres','qdrant'):
                for dataset in ('msmarco','wikipedia'):
                    stem=dataset+'-'+db
                    for path in ('M1','C5'):
                        deps['smoke_history:'+stem+'-'+path]={'db':db,'path':path,'native_timing':{'elapsed_s':40},'points':[point]}
                    deps['smoke_fresh:'+stem]={'db':db,'native_timing':{'elapsed_s':20},'point':point}
                    deps['smoke_initial:'+stem]={'native_timing':{'elapsed_s':10}}
            manifest={}
            with patch('vectorbench.experiments.real_vectors.clustered.gates.validate_window',return_value=auth), \
                 patch('vectorbench.experiments.real_vectors.clustered.gates.time.time',return_value=now), \
                 patch('vectorbench.experiments.real_vectors.clustered.gates.shutil.disk_usage',return_value=SimpleNamespace(free=30*GIB)), \
                 patch('vectorbench.experiments.real_vectors.clustered.gates.owned_output_bytes',return_value=GIB):
                with self.assertRaises(AssertionError):cost_gate(root,manifest,deps,out)
                prior=root/'failed.json';save(prior,load(out/'cost-gate.json'))
                approval={'action':'PROCEED_DESPITE_FORECAST_BLOCK','original_t0':1000,'original_deadline':44200,
                          'user_text':'그냥 진행해.','failed_cost_gate':{'path':str(prior),'sha256':sha(prior)}}
                ap=root/'approval.json';save(ap,approval)
                manifest['forecast_exception']={'path':str(ap),'sha256':sha(ap)}
                # The user exception never lifts the report reserve / hard deadline.
                with self.assertRaises(AssertionError):cost_gate(root,manifest,deps,out)
                with patch('vectorbench.experiments.real_vectors.clustered.gates.time.time',return_value=30000):
                    result=cost_gate(root,manifest,deps,out)
                    self.assertEqual(result['status'],'PASS')
                    self.assertEqual(result['forecast_status'],'BLOCK')
                    self.assertEqual(result['admission_status'],'USER_AUTHORIZED_FORECAST_EXCEPTION')
                    altered=copy.deepcopy(deps);altered['smoke_history:msmarco-postgres-M1']['native_timing']['elapsed_s']=400
                    with self.assertRaises(AssertionError):cost_gate(root,manifest,altered,out)
                    with patch('vectorbench.experiments.real_vectors.clustered.gates.shutil.disk_usage',return_value=SimpleNamespace(free=11*GIB)):
                        self.assertEqual(cost_gate(root,manifest,deps,out)['status'],'PASS')
                    approval['original_deadline']+=1;save(ap,approval)
                    manifest['forecast_exception']['sha256']=sha(ap)
                    with self.assertRaises(AssertionError):cost_gate(root,manifest,deps,out)


if __name__=='__main__':unittest.main()
