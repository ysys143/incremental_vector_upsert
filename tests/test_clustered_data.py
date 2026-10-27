"""Clustered real-vector preparation: frozen paths, exact controls and G1 mutations."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import numpy as np

from vectorbench.datasets.data import Pool
from vectorbench.datasets.storage import load_state
from vectorbench.evidence.contracts import Config, sha
from vectorbench.experiments.real_vectors.clustered.data import prepare, validate_prepared


class ClusteredDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        rng = np.random.default_rng(405)
        vectors = rng.normal(size=(1000, 8)).astype(np.float32)
        queries = rng.normal(size=(800, 8)).astype(np.float32)
        # Five directional groups ensure all clusters are populated in small fixtures.
        for i in range(5):
            vectors[i::5, i] += 3
        cls.pool = Pool("msmarco", vectors, queries, np.arange(1000), np.arange(800),
                        "fixture-base-sha", "fixture-query-sha")
        cls.config = Config(n=100)
        cls.result = prepare(cls.pool, cls.root / "prepared", cls.config)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_prepared_paths_and_cohorts(self):
        pool, result, config = self.pool, self.result, self.config
        self.assertTrue(validate_prepared(result, pool, config))
        self.assertEqual(set(result["seeds"]), {"7", "11", "29", "47"})
        with np.load(result["query_rows"]) as q:
            self.assertEqual(len(q["calibration"]), 200)
            self.assertEqual(len(q["evaluation"]), 600)
        for seed in result["seeds"].values():
            self.assertEqual(set(seed["paths"]), {"M1", "M5", "C1", "C5"})
            self.assertEqual(len(seed["wave_sizes"]), 5)
            self.assertEqual(sum(seed["wave_sizes"]), 100)
            self.assertEqual(len({p["final_ledger_sha256"] for p in seed["paths"].values()}), 1)
            for path in seed["paths"].values():
                self.assertEqual(len(path["controls_by_query_set"]["calibration"]), len(path["controls"]))
                self.assertEqual(len(path["controls_by_query_set"]["evaluation"]), len(path["controls"]))
            expected_queries = 200 if seed["query_rows"] == "calibration" else 600
            with np.load(seed["cohort_codes"]) as cohorts:
                self.assertEqual(len(cohorts["origin"]), expected_queries)
            self.assertEqual(len(seed["cohort_labels"]), expected_queries)
            self.assertLessEqual(set(seed["cohort_labels"]), {"old", "boundary", "new"})
            self.assertEqual(load_state(seed["initial"], len(pool.vectors)).digest(),
                             seed["paths"]["M1"]["states"]["0"]["ledger_sha256"])

    def test_mutated_state_id_rejected(self):
        altered = deepcopy(self.result)
        point = altered["seeds"]["11"]["paths"]["M5"]["states"]["1"]
        with np.load(point["path"]) as original:
            records = original["records"].copy()
        records[0, 1] += 1
        path = self.root / "bad-state.npz"
        np.savez_compressed(path, records=records)
        point["path"], point["sha256"] = str(path), sha(path)
        with self.assertRaises(AssertionError):
            validate_prepared(altered, self.pool, self.config)

    def test_mutated_vector_byte_rejected(self):
        pool = self.pool
        changed = pool.vectors.copy()
        state = load_state(self.result["seeds"]["11"]["initial"], len(changed))
        changed[state.records()[0].row, 0] += np.float32(.125)
        altered = Pool(pool.dataset, changed, pool.queries, pool.original_rows,
                       pool.original_query_rows, pool.base_sha, pool.query_sha)
        with self.assertRaises(AssertionError):
            validate_prepared(self.result, altered, self.config)

    def test_mutated_query_order_rejected(self):
        altered = deepcopy(self.result)
        with np.load(self.result["query_rows"]) as original:
            cal, evaluation = original["calibration"].copy(), original["evaluation"].copy()
        evaluation[[0, 1]] = evaluation[[1, 0]]
        path = self.root / "bad-query.npz"
        np.savez_compressed(path, calibration=cal, evaluation=evaluation)
        altered["query_rows"] = str(path)
        with self.assertRaises(AssertionError):
            validate_prepared(altered, self.pool, self.config)

    def test_swapped_state_oracle_rejected(self):
        altered = deepcopy(self.result)
        controls = altered["seeds"]["11"]["paths"]["M5"]["controls_by_query_set"]["evaluation"]
        controls[1] = deepcopy(controls[2])
        altered["seeds"]["11"]["paths"]["M5"]["controls"][1] = controls[1]
        with self.assertRaises(AssertionError):
            validate_prepared(altered, self.pool, self.config)


if __name__ == "__main__":
    unittest.main()
