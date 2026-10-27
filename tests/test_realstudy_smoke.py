"""Offline entry consumers; generated test vectors are never scientific evidence."""
import os
from dataclasses import replace
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vectorbench.experiments.real_vectors.actions import report
from vectorbench.evidence.contracts import Config, load, sha
from vectorbench.datasets.data import Pool
from vectorbench.experiments.real_vectors.smoke_campaign import nodes
from vectorbench.experiments.real_vectors.smoke_data import OPERATIONS, prepare
from vectorbench.datasets.storage import load_events, load_state
from vectorbench.workloads.workload import oracle_for
from vectorbench.datasets.selection import entry_split


class SmokeTests(unittest.TestCase):
    def test_large_request_preparation_preserves_one_wave_and_operation_counts(self):
        rng = np.random.default_rng(41)
        x,q = rng.normal(size=(400,768)).astype("<f4"),rng.normal(size=(40,768)).astype("<f4")
        pool=Pool("fixture",x,q,np.arange(400),np.arange(40),"x","q")
        c=replace(Config(),n=100,query_rows=10,calibration_queries=10)
        with tempfile.TemporaryDirectory() as d, patch("vectorbench.experiments.real_vectors.smoke_data.config",return_value=c),patch("vectorbench.datasets.data.check_space"),patch("vectorbench.evidence.checkpoints.check_space"):
            prepared=prepare(pool,Path(d),large_requests=True)
            for op,spec in prepared["paths"].items():
                events=load_events(spec["events"])
                self.assertEqual(len(events),1)
                self.assertEqual(len(spec["controls"]),2)
                self.assertEqual(len(events[0].writes),100 if op=="insert" else 0 if op=="delete" else 50)
                self.assertEqual(len(events[0].deletes),50 if op in {"delete","churn","reinsert"} else 0)
                final=load_state(spec["final"],len(x))
                self.assertEqual(len(final.current),200 if op=="insert" else 50 if op=="delete" else 100)

    def test_duplicate_vectors_and_document_query_leakage_are_excluded_by_content(self):
        rng = np.random.default_rng(31)
        x = rng.normal(size=(100, 8)).astype("<f4")
        q = rng.normal(size=(40, 8)).astype("<f4")
        x[3] = x[4]
        q[0] = x[9]
        q[1] = q[2]
        pool = Pool("fixture", x, q, np.arange(len(x)), np.arange(len(q)), "x", "q")
        split = entry_split(pool, 20, 10, 10, 10)
        docs = np.concatenate([split["initial"], split["replacement"], split["calibration_documents"]])
        queries = np.concatenate([split["evaluation_queries"], split["calibration_queries"]])
        self.assertEqual(len({x[i].tobytes() for i in docs}), len(docs))
        self.assertEqual(len({q[i].tobytes() for i in queries}), len(queries))
        self.assertNotIn(0, queries)
        self.assertFalse({q[i].tobytes() for i in queries} & {v.tobytes() for v in x})
        self.assertEqual(split["diagnostics"]["duplicate_document_rows"], 1)
        self.assertEqual(split["diagnostics"]["duplicate_query_rows"], 1)
        self.assertEqual(split["diagnostics"]["query_values_present_in_document_pool"], 1)
        pool = Pool("fixture", x, x, np.arange(len(x)), np.arange(len(x)), "x", "x")
        with self.assertRaisesRegex(AssertionError, "document-disjoint"):
            entry_split(pool, 20, 10, 10, 10)

    def test_native_catalogue_preserves_all_entry_paths_and_phase_barriers(self):
        graph = nodes()
        counts = {role: sum(s["role"] == role for s in graph.values()) for role in {s["role"] for s in graph.values()}}
        self.assertEqual(counts["preparation"], 2)
        self.assertEqual(counts["tuning"], 4)
        self.assertEqual(counts["initial"], 4)
        self.assertEqual(counts["history"], 28)
        self.assertEqual(counts["fresh"], 28)
        self.assertEqual(len(graph), 71)
        for key, spec in graph.items():
            for parent in spec["deps"]:
                self.assertIn(parent, graph)
            if spec["action"] == "history":
                self.assertIn("snapshotted:all", spec["deps"])
            if spec["action"] == "fresh":
                self.assertIn("histories_done:all", spec["deps"])
        expected = {k for k in graph if k.startswith(("history:", "fresh:"))}
        self.assertEqual(set(graph["report:all"]["deps"]), expected)
        visited = set()
        while len(visited) < len(graph):
            ready = {k for k, v in graph.items() if k not in visited and set(v["deps"]) <= visited}
            self.assertTrue(ready, "cycle in actual scientific DAG")
            visited.update(ready)

    def test_real_source_shaped_input_reaches_all_write_and_oracle_consumers(self):
        rng = np.random.default_rng(19)
        x, q = rng.normal(size=(6000, 768)).astype("<f4"), rng.normal(size=(800, 768)).astype("<f4")
        pool = Pool("msmarco", x, q, np.arange(len(x)), np.arange(len(q)), "fixture-base", "fixture-query")
        with tempfile.TemporaryDirectory() as d, patch("vectorbench.datasets.data.check_space"), patch("vectorbench.evidence.checkpoints.check_space"):
            out = Path(d)
            prepared = prepare(pool, out)
            self.assertLess(prepared["unique_oracles"], 21)
            start = load_state(prepared["initial"], len(x))
            calibration = load_state(prepared["calibration"], len(x))
            with np.load(prepared["query_rows"]) as split:
                queries = split["evaluation"]
                self.assertFalse(set(queries) & set(split["calibration"]))
            occupied = set(w.row for w in start.records())
            for operation in OPERATIONS:
                spec = prepared["paths"][operation]
                live = load_state(prepared["initial"], len(x))
                events = load_events(spec["events"])
                self.assertEqual(len(events), 2)
                for step, event in enumerate(events, 1):
                    live.apply(event)
                    occupied.update(w.row for w in live.records())
                    truth = oracle_for(spec["controls"][step], pool, live, queries)
                    self.assertEqual(truth.shape, (60, 100))
                    self.assertTrue(set(truth.ravel()) <= set(live.current))
                self.assertEqual(live.digest(), load_state(spec["final"], len(x)).digest())
                self.assertEqual(len(live.records()), 1200 if operation == "insert" else 800 if operation == "delete" else 1000)
                if operation in {"same_vector", "metadata"}:
                    self.assertEqual([w.row for w in start.records()], [w.row for w in live.records()])
                if operation == "upsert":
                    self.assertEqual(sum(w.version == 1 for w in live.records()), 200)
            self.assertFalse(occupied & {w.row for w in calibration.records()})

    def test_report_rejects_partial_and_wrong_current_state(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(AssertionError, "missing"):
                report({}, Path(d))
            deps = {}
            for dataset in ("msmarco", "wikipedia"):
                for db in ("postgres", "qdrant"):
                    for op in OPERATIONS:
                        key = dataset+"-"+db+"-"+op
                        p = {"status": "PASS", "ann": {"status": "PASS"}, "metric": "ip", "vector_state_sha256": "v",
                             "query_sha256": "q", "ledger_sha256": "l", "recall10": .98, "recall100": .97}
                        deps["history:"+key] = {"steps": [0,1,2], "writes": [{},{}], "points": [dict(p) for _ in range(3)], "final_ledger_sha256": "l"}
                        deps["fresh:"+key] = {"point": dict(p), "state": "l"}
            self.assertEqual(report(deps, Path(d))["fresh_points"], 28)
            deps["fresh:msmarco-postgres-upsert"]["point"]["vector_state_sha256"] = "stale"
            with self.assertRaisesRegex(AssertionError, "state or query"):
                report(deps, Path(d))


if __name__ == "__main__":
    unittest.main()
