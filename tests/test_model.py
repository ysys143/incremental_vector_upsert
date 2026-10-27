import unittest

import numpy as np

from vectorbench.experiments.pilot.model import (Config, digest, direct_exact, exact, fixture, history,
                         packed_results, score, unpacked_results, validate_snapshot, validate_truth)


class MethodologyTests(unittest.TestCase):
    def test_short_and_empty_result_storage(self):
        rows = [[1, 2], [], [2]]
        packed = packed_results(rows)
        self.assertEqual(packed["returned"].shape, (3, 100))
        self.assertEqual(unpacked_results(packed), rows)
        quality = score([1, 2], np.array([[1, 2]] * 3), unpacked_results(packed))
        self.assertAlmostEqual(quality["recall100"], .5)
        self.assertAlmostEqual(quality["shortfall100"], 2 / 3)

    def test_oracle_matches_direct_distance_and_id_ties(self):
        ids = np.array([9, 2, 6])
        x = np.array([[1, 0], [-1, 0], [0, 3]], dtype=np.float32)
        queries = np.array([[0, 0], [0.5, 2]], dtype=np.float32)
        np.testing.assert_array_equal(exact(ids, x, queries, 3), direct_exact(ids, x, queries, 3))
        self.assertEqual(exact(ids, x, queries, 3)[0].tolist(), [2, 9, 6])

    def test_events_are_cumulative_and_repeatable(self):
        c = Config.smoke()
        for scenario, final_count in (("insert", 1200), ("delete", 800), ("churn", 1000)):
            previous = set(range(c.n))
            for ids, deleted, inserted in history(c, 11, scenario):
                self.assertEqual(set(ids), (previous - set(deleted)) | set(inserted))
                self.assertFalse(set(deleted) - previous)
                previous = set(ids)
            self.assertEqual(len(previous), final_count)
        a = list(history(c, 11, "churn"))
        b = list(history(c, 11, "churn"))
        for x, y in zip(a, b):
            np.testing.assert_array_equal(x[0], y[0])
            self.assertEqual(x[1:], y[1:])

    def test_deleted_id_mutation_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "non-live"):
            score([1, 2], np.array([[1, 2]]), [[1, 3]])

    def test_duplicate_mutation_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "duplicate"):
            score([1, 2], np.array([[1, 2]]), [[1, 1]])

    def test_missing_write_and_changed_vector_are_rejected(self):
        ids = np.arange(3)
        x = np.eye(3, dtype=np.float32)
        with self.assertRaises(AssertionError):
            validate_snapshot(ids, x, ids[:2], x[:2])
        changed = x.copy()
        changed[0, 0] = 2
        with self.assertRaises(AssertionError):
            validate_snapshot(ids, x, ids, changed)
        self.assertNotEqual(digest(ids, x), digest(ids, changed))

    def test_stale_oracle_mutation_is_rejected(self):
        ids = np.arange(3)
        x = np.array([[0, 0], [2, 0], [3, 0]], dtype=np.float32)
        queries = np.array([[0, 0]], dtype=np.float32)
        stale = exact(ids, x, queries, 2)
        changed = x.copy()
        changed[0] = [100, 100]
        with self.assertRaises(AssertionError):
            validate_truth(ids, changed, queries, stale)

    def test_fixture_holdout_and_oracle_agreement(self):
        c = Config.smoke()
        x, tuning, evaluation = fixture(c, 11)
        x2, t2, q2 = fixture(c, 11)
        np.testing.assert_array_equal(x, x2)
        np.testing.assert_array_equal(tuning, t2)
        np.testing.assert_array_equal(evaluation, q2)
        ids = np.arange(c.n)
        truth = exact(ids, x[:c.n], evaluation)
        validate_truth(ids, x[:c.n], evaluation, truth)
        self.assertEqual(score(ids, truth, truth.tolist())["recall100"], 1)


if __name__ == "__main__":
    unittest.main()
