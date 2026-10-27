"""Exercise transactional admission with real concurrent processes; no DB measurement."""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from vectorbench.evidence.contracts import GIB, ResourceGate
from vectorbench.evidence.registry import AdmissionConflict, Registry


def claim_and_acquire(path, index):
    registry = Registry(path)
    key, run = "cpu-" + str(index), "run-" + str(index)
    registry.claim(key, run, "frozen")
    try:
        registry.acquire(key, run, "cpu", GIB, 40 * GIB)
        return True
    except ResourceGate:
        return False


class RegistryTests(unittest.TestCase):
    def test_successor_inherits_source_budget_without_restarting_growth_allowance(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            r.inherit_baseline(40*GIB)
            r.inherit_baseline(40*GIB)
            with self.assertRaises(AdmissionConflict):
                r.inherit_baseline(33*GIB)
            r.claim("a", "a", "sha")
            r.acquire("a", "a", "db", 2*GIB, 33*GIB)

    def test_growth_baseline_cannot_reset_after_cleanup_or_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "admission.sqlite"
            r = Registry(path)
            r.claim("a", "a", "sha")
            r.acquire("a", "a", "cpu", GIB, 40*GIB)
            r.finish("a", "a", "worker-completed")
            r = Registry(path)
            r.claim("b", "b", "sha")
            r.acquire("b", "b", "db", 2*GIB, 33*GIB)
            self.assertEqual(r.baseline_free(), 40*GIB)
            self.assertEqual(len(r.snapshot()["leases"]), 1)

    def test_low_space_is_recorded_without_blocking(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            r.claim("a", "a", "sha")
            r.acquire("a", "a", "cpu", GIB, 5*GIB)
            self.assertEqual(r.baseline_free(), 5*GIB)

    def test_completed_worker_keeps_db_exclusion_until_blocking_check_finishes(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            r.claim("db", "db", "sha")
            r.claim("next", "next", "sha")
            r.acquire("db", "db", "db", GIB, 40 * GIB)
            r.finish("db", "db", "worker-completed", retain_lease=True)
            with self.assertRaises(ResourceGate):
                r.acquire("next", "next", "cpu", GIB, 40 * GIB)
            r.release_checked("db", "db")
            r.acquire("next", "next", "cpu", GIB, 40 * GIB)

    def test_original_phase_deadline_is_not_extended(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d)/"admission.sqlite")
            with patch("vectorbench.evidence.registry.time.time", return_value=100):
                self.assertEqual(r.deadline("A", 60), 160)
            with patch("vectorbench.evidence.registry.time.time", return_value=1000):
                self.assertEqual(r.deadline("A", 60), 160)
                self.assertEqual(r.deadline("B", 60), 1060)
    def test_parallel_admission_never_loses_cells_or_exceeds_two(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "admission.sqlite")
            Registry(path)
            with ProcessPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(claim_and_acquire, [path] * 4, range(4)))
            snapshot = Registry(path).snapshot()
            self.assertEqual(sum(results), 2)
            self.assertEqual(len(snapshot["attempts"]), 4)
            self.assertEqual(len(snapshot["leases"]), 2)

    def test_duplicate_finished_and_changed_scope_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            r.claim("a", "r1", "sha")
            with self.assertRaises(AdmissionConflict):
                r.claim("a", "r2", "sha")
            r.finish("a", "r1", "worker-completed", "receipt.json")
            with self.assertRaises(AdmissionConflict):
                r.claim("a", "r3", "sha")
            with self.assertRaises(AdmissionConflict):
                r.claim("a", "r4", "changed")

    def test_real_failure_needs_exact_retry_disposition(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            r.claim("a", "r1", "sha"); r.finish("a", "r1", "failed")
            with self.assertRaises(AdmissionConflict):
                r.claim("a", "r2", "sha")
            r.retry("a", "r1", "reconciled original outcome; selected retry")
            r.claim("a", "r3", "sha")
            self.assertEqual(len(r.snapshot()["attempts"]), 2)

    def test_db_slot_excludes_cpu_and_io_both_directions(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            for key in ["db1", "db2", "cpu", "io"]:
                r.claim(key, key, "sha")
            r.acquire("db1", "db1", "db", GIB, 40 * GIB)
            for key, kind in [("db2", "db"), ("cpu", "cpu"), ("io", "io")]:
                with self.assertRaises(ResourceGate):
                    r.acquire(key, key, kind, GIB, 40 * GIB)
            r.finish("db1", "db1", "worker-completed")
            r.acquire("cpu", "cpu", "cpu", GIB, 40 * GIB)
            with self.assertRaises(ResourceGate):
                r.acquire("db2", "db2", "db", GIB, 40 * GIB)

    def test_aggregate_disk_reservations_are_observational(self):
        with tempfile.TemporaryDirectory() as d:
            r = Registry(Path(d) / "admission.sqlite")
            for key in ["a", "b"]:
                r.claim(key, key, "sha")
            r.acquire("a", "a", "cpu", 2 * GIB, 13 * GIB)
            r.acquire("b", "b", "cpu", 2 * GIB, 13 * GIB)
            self.assertEqual(len(r.snapshot()['leases']), 2)


if __name__ == "__main__":
    unittest.main()
