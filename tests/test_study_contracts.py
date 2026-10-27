import copy
import json
from pathlib import Path
import unittest

import numpy as np

from vectorbench.experiments.synthetic_insertion.contracts import case_specs, config, expected_counts
from vectorbench.experiments.synthetic_insertion.data import base_data, make_fixture, nearest, states_for, waves_for
from vectorbench.evidence.synthetic_telemetry import UnverifiedTelemetry, segments, verify_delta


class StudyContracts(unittest.TestCase):
    def test_complete_approved_path_count(self):
        c = config()
        counts = expected_counts(c)
        self.assertEqual(counts, {"paths": 132, "incremental_checkpoints": 504})
        specs = case_specs(c)
        self.assertEqual(len({s["id"] for s in specs}), 132)
        self.assertEqual({p: sum(s["stage"] == p for s in specs) for p in "ABCD"},
                         {"A": 24, "B": 72, "C": 24, "D": 12})

    def test_wave_partition_preserves_vector_order_and_final_set(self):
        c = {**config(True), "n": 100, "dim": 16}
        f = make_fixture(c, 11, "far", base_data(100, 16, 11))
        specs = [s for s in case_specs(c) if s["db"] == "postgres" and s["geometry"] == "far"]
        one, five = specs
        np.testing.assert_array_equal(np.concatenate(waves_for(one, c, f)), np.concatenate(waves_for(five, c, f)))
        np.testing.assert_array_equal(states_for(one, c, f)[-1], states_for(five, c, f)[-1])
        self.assertEqual(len(waves_for(one, c, f)), 1)
        self.assertEqual(len(waves_for(five, c, f)), 5)

    def test_partial_order_changes_only_sequence(self):
        c = {**config(True), "n": 100, "dim": 16}
        f = make_fixture(c, 11, "far", base_data(100, 16, 11))
        spec = next(s for s in case_specs(c) if s["geometry"] == "far" and s["waves"] == 5)
        a = np.concatenate(waves_for(spec, c, f))
        b = np.concatenate(waves_for({**spec, "order": "ordered"}, c, f))
        np.testing.assert_array_equal(np.sort(a), np.sort(b))
        self.assertFalse(np.array_equal(a, b))

    def test_fast_oracle_handles_tie_boundary_and_self_exclusion(self):
        x = np.array([[1, 0], [-1, 0], [0, 1], [0, -1]], dtype=np.float32)
        ids = np.array([4, 2, 1, 3])
        self.assertEqual(nearest(ids, x, np.array([[0, 0]], dtype=np.float32), 2).tolist(), [[1, 2]])
        self.assertNotIn(4, nearest(ids, x, x[:1], 2, exclude=[4])[0])


class TelemetryContract(unittest.TestCase):
    def setUp(self):
        self.root = Path("results/20261024T201222Z-telemetry-level4")
        if not self.root.exists():
            self.skipTest("requires recorded real Qdrant telemetry")
        self.before = json.loads((self.root / "before.json").read_text())
        self.after = json.loads((self.root / "after.json").read_text())
        self.plain = json.loads((self.root / "plain.json").read_text())

    def test_real_hnsw_delta(self):
        self.assertEqual(verify_delta(self.before, self.after, 2)["delta"]["unfiltered_hnsw"], 2)

    def test_real_exact_search_rejected(self):
        with self.assertRaises(UnverifiedTelemetry):
            verify_delta(self.after, self.plain, 1)

    def test_missing_outer_structure_is_not_zero(self):
        mutated = copy.deepcopy(self.after)
        del mutated["result"]["collections"]["collections"][0]["shards"][0]["local"]["segments"]
        with self.assertRaises(UnverifiedTelemetry):
            segments(mutated)

    def test_replaced_segment_is_rejected(self):
        mutated = copy.deepcopy(self.after)
        mutated["result"]["collections"]["collections"][0]["shards"][0]["local"]["segments"][1]["info"]["uuid"] = "replacement"
        with self.assertRaises(UnverifiedTelemetry):
            verify_delta(self.before, mutated, 2)

    def test_absence_of_hnsw_work_is_not_pass(self):
        with self.assertRaises(UnverifiedTelemetry):
            verify_delta(self.before, self.before, 2)


if __name__ == "__main__":
    unittest.main()
