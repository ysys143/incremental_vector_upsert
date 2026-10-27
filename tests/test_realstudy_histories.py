"""Event/checkpoint consumers only; no DB or real-embedding conclusions."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import numpy as np

from vectorbench.evidence.checkpoints import Checkpoints
from vectorbench.datasets.data import Pool
from vectorbench.workloads.events import Live, initial
from vectorbench.workloads.history_plan import Recipe, compile_history
from vectorbench.datasets.storage import load_events, load_state
from vectorbench.workloads.workload import oracle_for


class HistoryTests(unittest.TestCase):
    def test_same_fixed_stream_reaches_identical_one_five_final_state_for_all_operations(self):
        start = initial(range(1000))
        for operation in ("insert", "delete", "churn", "upsert", "reinsert", "same_vector", "metadata"):
            ratios = (10,25,50,100) if operation == "insert" else (10,25,50) if operation == "delete" else (50,)
            for ratio in ratios:
                outputs = [compile_history(start, range(1000, 6000), Recipe(operation, waves, ratio),
                                           target_order=range(999,-1,-1)) for waves in (1,5)]
                self.assertEqual(outputs[0]["final"].digest(), outputs[1]["final"].digest(), (operation,ratio))
                self.assertEqual(outputs[0]["counts"]["waves"], 1)
                self.assertEqual(outputs[1]["counts"]["waves"], 5)
                self.assertEqual(outputs[0]["counts"]["writes"], outputs[1]["counts"]["writes"])
                if operation != "insert":
                    wanted = set(range(1000-ratio*10,1000))
                    targeted = {i for e in outputs[1]["events"] for i in (e.deletes or [w.id for w in e.writes])}
                    if operation not in {"churn", "reinsert"}:
                        self.assertEqual(targeted, wanted)

    def test_long_real_scale_event_counts_and_hot_versions(self):
        start = initial(range(10000))
        for operation in ("churn", "upsert"):
            recipe = Recipe(operation,50,10,long=True,hot=True)
            result = compile_history(start, range(10000,60000), recipe)
            self.assertEqual(result["counts"]["waves"], 50)
            self.assertEqual(result["counts"]["writes"], 50000)
            self.assertEqual(result["counts"]["final_count"], 10000)
            seen = set(range(10000))
            live = Live(start,60000)
            for event in result["events"]:
                self.assertFalse(seen & {w.row for w in event.writes})
                seen.update(w.row for w in event.writes)
                live.apply(event)
                self.assertEqual(len(live.current),10000)
            if operation == "upsert":
                self.assertEqual(sum(w.version==50 for w in live.records()),1000)
                self.assertEqual(set(live.current),set(range(10000)))

    def test_explicit_cluster_or_bridge_targets_are_used_without_reselection(self):
        start = initial(range(100))
        targets = [list(range(90,100))]*50
        result = compile_history(start,range(100,600),Recipe("upsert",50,10,long=True,hot=True),long_targets=targets)
        for event in result["events"]:
            self.assertEqual([w.id for w in event.writes],list(range(90,100)))
        bad = [list(range(90,100))]*49+[list(range(80,90))]
        with self.assertRaisesRegex(ValueError,"hot history"):
            compile_history(start,range(100,600),Recipe("upsert",50,10,long=True,hot=True),long_targets=bad)

    def test_invalid_batch_or_repeated_target_disguises_are_rejected(self):
        start = initial(range(100))
        for recipe in (Recipe("delete",50,10,long=True), Recipe("upsert",1,50,hot=True), Recipe("delete",5,100)):
            with self.assertRaises(ValueError):
                compile_history(start,range(100,600),recipe)
        with self.assertRaises(ValueError):
            compile_history(start,range(100,600),Recipe("upsert",5),target_order=[0]*100)
        with self.assertRaises(ValueError):
            compile_history(start,list(range(100,150))+[100]*50,Recipe("insert",1,100))

    def test_checkpoint_reuses_truth_without_merging_metadata_versions(self):
        rng = np.random.default_rng(3)
        x,q=rng.normal(size=(600,768)).astype("<f4"),rng.normal(size=(12,768)).astype("<f4")
        pool=Pool("fixture",x,q,np.arange(600),np.arange(12),"x","q")
        start=initial(range(100)); queries=np.arange(12)
        with tempfile.TemporaryDirectory() as d, patch("vectorbench.evidence.checkpoints.check_space"), patch("vectorbench.datasets.data.check_space"):
            cache=Checkpoints(pool,queries,Path(d)/"cache")
            result=compile_history(start,range(100,600),Recipe("metadata",5))
            prepared=cache.path(start,result,Path(d)/"metadata")
            self.assertEqual(len(cache.oracles),1)
            self.assertEqual(len(cache.states),6)
            self.assertEqual(len({v["path"] for v in prepared["controls"]}),1)
            for step,state in prepared["states"].items():
                live=load_state(state["path"],len(x))
                self.assertEqual(oracle_for(prepared["controls"][int(step)],pool,live,queries).shape,(12,100))
            self.assertEqual(len(load_events(prepared["events"])),5)
            original=Path(prepared["controls"][0]["path"])
            receipt=original.with_suffix(original.suffix+".receipt.json")
            original_receipt=receipt.read_bytes()
            receipt.write_bytes(original_receipt+b'\n')
            with self.assertRaisesRegex(AssertionError,"receipt changed"):
                oracle_for(prepared["controls"][0],pool,result["final"],queries)
            receipt.write_bytes(original_receipt)
            original.write_bytes(b'changed acknowledged truth')
            with self.assertRaisesRegex(AssertionError,"oracle"):
                cache.get(result["final"])

    def test_continuous_preparation_only_creates_observed_state_oracles(self):
        rng=np.random.default_rng(4)
        x,q=rng.normal(size=(600,768)).astype("<f4"),rng.normal(size=(12,768)).astype("<f4")
        pool=Pool("fixture",x,q,np.arange(600),np.arange(12),"x","q")
        with tempfile.TemporaryDirectory() as d, patch("vectorbench.evidence.checkpoints.check_space"), patch("vectorbench.datasets.data.check_space"):
            start=initial(range(100))
            cache=Checkpoints(pool,np.arange(12),Path(d)/"cache")
            compiled=compile_history(start,range(100,600),Recipe("upsert",50,10,long=True,policy="continuous"))
            result=cache.path(start,compiled,Path(d)/"continuous")
            self.assertEqual(result["observed_steps"],[0,50])
            self.assertEqual(len(cache.states),2)
            self.assertEqual(len(cache.oracles),2)
            self.assertTrue(all(v is None for v in result["controls"][1:50]))


if __name__ == "__main__":
    unittest.main()
