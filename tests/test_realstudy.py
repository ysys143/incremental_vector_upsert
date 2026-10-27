"""Offline boundary tests only; synthetic tiny fixtures do not qualify real-data smoke."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vectorbench.backends.adapters import Postgres, assert_ann, validate_rows
from vectorbench.evidence.contracts import Config, GIB, ResourceGate, check_space, evidence_key, save, load
from vectorbench.workloads.events import Event, Live, Write, initial, replacement_history
from vectorbench.evaluation.metrics import exact, direct_exact, state_hash
from vectorbench.datasets.source import headers, ranges


class MetricTests(unittest.TestCase):
    def test_ip_is_not_l2_and_independent_oracles_agree(self):
        ids, x, q = np.array([9, 4]), np.array([[2., 0.], [1., 0.]], dtype=np.float32), np.array([[1., 0.]], dtype=np.float32)
        self.assertEqual(exact(ids, x, q, k=1, metric="ip").tolist(), [[9]])
        self.assertEqual(exact(ids, x, q, k=1, metric="l2").tolist(), [[4]])
        for metric in ["ip", "l2"]:
            np.testing.assert_array_equal(exact(ids, x, q, metric=metric), direct_exact(ids, x, q, metric=metric))

    def test_tie_boundary_and_block_size(self):
        ids = np.array([8, 3, 1, 6]); x = np.ones((4, 3), dtype=np.float32); q = np.ones((5, 3), dtype=np.float32)
        for block in [1, 3]:
            np.testing.assert_array_equal(exact(ids, x, q, k=2, block=block), np.tile([1, 3], (5, 1)))

    def test_changed_vector_same_id_invalidates_state_and_cache(self):
        a, b = np.array([[1., 0.]]), np.array([[2., 0.]])
        self.assertNotEqual(state_hash([0], a), state_hash([0], b))
        self.assertNotEqual(evidence_key("d", state_hash([0], a), "q", "ip"), evidence_key("d", state_hash([0], b), "q", "ip"))
        self.assertNotEqual(evidence_key("d", "s", "q", "ip"), evidence_key("d", "s", "q", "l2"))

    def test_invalid_vectors_rejected(self):
        with self.assertRaises(ValueError):
            exact([0], [[np.nan, 0]], [[1, 0]])
        with self.assertRaises(ValueError):
            exact([0, 0], [[1, 0], [2, 0]], [[1, 0]])
        self.assertEqual(exact([], np.empty((0, 2)), [[1, 0]]).shape, (1, 0))


class EventTests(unittest.TestCase):
    def test_one_and_five_share_exact_final_state(self):
        start = initial(range(100))
        for operation in ["delete", "churn", "upsert", "reinsert", "same_vector", "metadata"]:
            split = replacement_history(start, range(100, 600), operation=operation)
            combined = replacement_history(start, range(100, 600), operation=operation, combine=True)
            a, b = Live(start, 600), Live(start, 600)
            for event in split:
                a.apply(event)
            b.apply(combined[0])
            self.assertEqual(a.digest(), b.digest(), operation)

    def test_long_churn_constant_size_and_never_reused_vectors(self):
        start = initial(range(100)); live = Live(start, 600)
        seen = set(range(100))
        events = replacement_history(start, range(100, 600), rounds=50, operation="churn", repeated=True)
        for event in events:
            self.assertFalse(seen.intersection(w.row for w in event.writes))
            seen.update(w.row for w in event.writes)
            live.apply(event)
            self.assertEqual(len(live.current), 100)
        self.assertEqual(len(seen), 600)

    def test_hot_upsert_changes_same_ids_and_latest_versions(self):
        start = initial(range(100)); live = Live(start, 600)
        events = replacement_history(start, range(100, 600), rounds=50, operation="upsert", repeated=True, hot=True)
        for event in events:
            live.apply(event)
        self.assertEqual(set(live.current), set(range(100)))
        self.assertEqual(sum(w.version == 50 for w in live.records()), 10)
        self.assertEqual(sum(w.version == 0 for w in live.records()), 90)
        with self.assertRaises(ValueError):
            replacement_history(start, range(100, 600), operation="upsert", repeated=True, combine=True)

    def test_failed_event_does_not_partially_change_state(self):
        live = Live(initial(range(100)), 600); before = live.digest()
        with self.assertRaises(ValueError):
            live.apply(Event((0,), (Write(100, 700, 0),), "churn"))
        self.assertEqual(live.digest(), before)

    def test_stale_version_and_metadata_vector_mutation_rejected(self):
        live = Live(initial(range(100)), 600)
        with self.assertRaises(ValueError):
            live.apply(Event((), (Write(0, 100, 0),), "upsert"))
        with self.assertRaises(ValueError):
            live.apply(Event((), (Write(0, 100, 1),), "metadata"))

    def test_same_vector_control_is_fully_idempotent(self):
        start = initial(range(100)); live = Live(start, 600); before = live.digest()
        for event in replacement_history(start, range(100, 600), operation="same_vector", repeated=True, hot=True):
            live.apply(event)
        self.assertEqual(live.digest(), before)


class AdmissionTests(unittest.TestCase):
    def test_wrong_metric_seqscan_and_fallback_plan_rejected(self):
        plan = [{"Plan": {"Node Type": "Index Scan", "Index Name": "items_hnsw", "Order By": "embedding <#> query", "Actual Loops": 1}}]
        assert_ann(plan, "off", analyzed=True)
        for setting in ["on", "unknown"]:
            with self.assertRaises(AssertionError):
                assert_ann(plan, setting)
        wrong = json.loads(json.dumps(plan)); wrong[0]["Plan"]["Order By"] = "embedding <-> query"
        with self.assertRaises(AssertionError):
            assert_ann(wrong, "off")
        for kind in ["Seq Scan", "Bitmap Heap Scan", "Sort"]:
            wrong = json.loads(json.dumps(plan)); wrong[0]["Plan"]["Node Type"] = kind
            with self.assertRaises(AssertionError):
                assert_ann(wrong, "off")

    def test_upsert_uses_staging_copy_and_keeps_index(self):
        adapter = object.__new__(Postgres); adapter.table = "items"; calls = []; adapter.sql = calls.append
        adapter.upsert([4, 5], np.ones((2, 3), dtype=np.float32))
        self.assertTrue(calls[0].startswith("BEGIN; CREATE TEMP TABLE"))
        self.assertTrue(calls[1].startswith("COPY ivu_stage"))
        self.assertIn("ON CONFLICT (id) DO UPDATE", calls[2])
        self.assertTrue(calls[2].endswith("COMMIT;"))
        self.assertFalse(any("CREATE INDEX" in sql or "REINDEX" in sql for sql in calls))
        with self.assertRaises(ValueError):
            adapter.upsert([4, 4], np.ones((2, 3)))
        self.assertEqual(len(calls), 3)

    def test_disk_readings_do_not_block_execution(self):
        with patch("vectorbench.evidence.contracts.shutil.disk_usage") as usage:
            usage.return_value.free = 9 * GIB
            self.assertEqual(check_space(".")['disk_policy'], 'OBSERVE_ONLY')
            check_space(".", replace(Config(), free_floor_bytes=0))
            usage.return_value.free = 11 * GIB
            check_space(".")
            check_space(".", reserve=2 * GIB)

    def test_source_ranges_are_bounded_disjoint_and_deterministic(self):
        a = ranges(35000000, 100000, 32)
        self.assertEqual(a, ranges(35000000, 100000, 32))
        self.assertEqual(sum(n for _, n in a), 100000)
        self.assertTrue(all(x+n <= y for (x, n), (y, _) in zip(a, a[1:])))
        self.assertTrue(all(0 <= x < x+n <= 35000000 for x, n in a))
        status, h = headers('HTTP/1.1 206 Partial Content\r\nETag: "a"\r\nContent-Range: bytes 8-15/100\r\n\r\n')
        self.assertEqual((status, h["etag"]), (206, '"a"'))


if __name__ == "__main__":
    unittest.main()
