"""Uniform baseline preparation on tiny generated fixtures; no DB qualification."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import numpy as np

from vectorbench.experiments.real_vectors.baseline.baseline_specs import cases,counts
from vectorbench.experiments.real_vectors.baseline.baseline_data import prepare
from vectorbench.evidence.contracts import Config
from vectorbench.datasets.data import Pool
from vectorbench.workloads.events import Event,Live,Write,initial
from vectorbench.datasets.storage import load_state,load_events
from vectorbench.evaluation.measure import verify_snapshot


class BaselineTests(unittest.TestCase):
    def test_actual_float32_readback_rejects_one_ulp_change(self):
        x=np.ones((2,768),dtype=np.float32)
        verify_snapshot(np.arange(2),x,[0,1],x.tolist())
        altered=x.copy(); altered[0,0]=np.nextafter(altered[0,0],np.float32(2))
        with self.assertRaises(AssertionError): verify_snapshot(np.arange(2),x,[0,1],altered)

    def test_declared_complete_uniform_phase_counts(self):
        self.assertEqual(len(cases()),29)
        self.assertEqual(counts()["histories"],348)
        self.assertEqual(counts()["incremental_points"],4212)
        self.assertEqual(counts()["queries"],600)
        self.assertEqual(counts()["seeds"],[11,29,47])

    def test_read_only_control_rejects_actual_mutation(self):
        live=Live(initial(range(100)),600); before=live.digest()
        live.apply(Event(operation="read_only")); self.assertEqual(before,live.digest())
        with self.assertRaisesRegex(ValueError,"cannot mutate"):
            live.apply(Event(writes=(Write(100,100,0),),operation="read_only"))

    def test_all_recipes_use_current_state_truth_and_global_document_holdout(self):
        rng=np.random.default_rng(91)
        x,q=rng.normal(size=(900,768)).astype("<f4"),rng.normal(size=(50,768)).astype("<f4")
        pool=Pool("fixture",x,q,np.arange(len(x)),np.arange(len(q)),"x","q")
        c=replace(Config(),n=100,query_rows=20,calibration_queries=5,seeds=(11,))
        with tempfile.TemporaryDirectory() as d,patch("vectorbench.datasets.data.check_space"),patch("vectorbench.evidence.checkpoints.check_space"):
            out=prepare(pool,Path(d),c); seed=out["seeds"]["11"]
            self.assertEqual(len(seed["paths"]),29)
            self.assertEqual(sum(len(p["observed_steps"]) for p in seed["paths"].values()),351)
            cal=set(w.row for w in load_state(out["calibration"]["growth"]["state"],len(x)).records())
            for key,path in seed["paths"].items():
                events=load_events(path["events"])
                self.assertEqual(len(events),path["spec"]["waves"])
                live=load_state(seed["initial"],len(x))
                for event in events: live.apply(event)
                self.assertEqual(live.digest(),path["final_ledger_sha256"])
                self.assertFalse(cal & {w.row for w in live.records()})
                with np.load(path["controls"][-1]["path"]) as truth:
                    self.assertTrue(set(truth["truth"].ravel())<=set(live.current))
                with np.load(path["cohorts"]) as groups:
                    self.assertEqual(len(groups.files),path["spec"]["waves"]+1)
                    self.assertEqual(groups["0"].shape,(20,))
            for operation in ("insert","delete","churn","upsert","reinsert","same_vector","metadata"):
                ratios=(10,25,50,100) if operation=="insert" else (10,25,50) if operation=="delete" else (50,)
                for ratio in ratios:
                    self.assertEqual(seed["paths"][f"{operation}-r{ratio}-w1"]["final_ledger_sha256"],
                                     seed["paths"][f"{operation}-r{ratio}-w5"]["final_ledger_sha256"])


if __name__=="__main__": unittest.main()
