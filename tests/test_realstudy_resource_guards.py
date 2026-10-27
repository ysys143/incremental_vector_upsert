"""Resource guards and native-stat decoding; no scientific performance claims."""
from pathlib import Path
import os
import tempfile
import unittest
from vectorbench.evidence.contracts import GIB,ResourceGate,owned_output_bytes
from vectorbench.evidence.registry import Registry
from vectorbench.orchestration.resource_monitor import parse_sample
from vectorbench.experiments.real_vectors.baseline.baseline_tuning import select


class GuardTests(unittest.TestCase):
    def test_owned_storage_is_observational_above_old_quota(self):
        with tempfile.TemporaryDirectory() as d:
            r=Registry(Path(d)/'admission.sqlite'); r.claim('a','a','sha'); r.acquire('a','a','db',2*GIB,80*GIB)
            self.assertEqual(r.check_owned_storage('a','a',5*GIB)['cap_bytes'],8*GIB)
            self.assertEqual(r.check_owned_storage('a','a',7*GIB)['disk_policy'],'OBSERVE_ONLY')

    def test_hardlinks_count_once_and_symlinks_do_not_expand_scope(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d); (p/'data').write_bytes(b'a'*1024); os.link(p/'data',p/'linked')
            (p/'outside').symlink_to('/Users/jaesolshin')
            s=(p/'data').stat()
            self.assertEqual(owned_output_bytes(p),max(s.st_size,s.st_blocks*512))

    def test_actual_docker_terminal_sequences_decode_as_one_json_sample(self):
        self.assertEqual(parse_sample('\x1b[H{"CPUPerc":"1.2%"}\x1b[K\n'),{'CPUPerc':'1.2%'})
        self.assertIsNone(parse_sample('\x1b[K\n'))
        with self.assertRaises(ValueError): parse_sample('not a Docker JSON frame')

    def test_timed_out_build_is_not_fabricated_as_a_quality_candidate(self):
        groups=[]
        for build in ((16,100),(32,200),(48,300),(64,400)):
            for state in ('initial','growth','delete','replacement'):
                groups.append({'status':'BUILD_TIME_LIMIT' if build[0]==64 and state=='growth' else 'MEASURED',
                    'build':list(build),'state':state,'setup':{'build_s':1},
                    'points':[{'ef':ef,'point':{'recall10':1.,'recall100':1.,'shortfall100':0,'p95_ms':float(build[0])}} for ef in (100,128,256,512,1000)]})
        with tempfile.TemporaryDirectory() as d:
            value=select(groups,{'initial_batch':1000},Path(d))
            self.assertEqual(value['build'],[16,100])
            unavailable=[v for v in value['candidates'] if v['build']==[64,400]]
            self.assertEqual(len(unavailable),5)
            self.assertTrue(all(v['status']=='INCOMPLETE_BUILD_BUDGET' for v in unavailable))


if __name__=='__main__': unittest.main()
