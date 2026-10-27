from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import numpy as np

from vectorbench.workloads.workload import read_point, history
from vectorbench.workloads.events import initial
from vectorbench.evidence.synthetic_telemetry import UnverifiedTelemetry


class ReadRetryTests(unittest.TestCase):
    def test_cleanup_error_does_not_hide_first_workload_error(self):
        class Broken:
            def __init__(self, *args, **kwargs): pass
            def start(self): raise RuntimeError("first workload failure")
            def cleanup(self): return ["secondary cleanup failure"]
        with tempfile.TemporaryDirectory() as d:
            with patch("vectorbench.workloads.workload.Server", Broken), patch("vectorbench.workloads.workload.check_space"):
                with self.assertRaisesRegex(RuntimeError, "first workload failure"):
                    history("postgres", SimpleNamespace(vectors=np.zeros((100, 3))), initial(range(100)), [], [], [], {}, {}, {"archive_sha": "unused"}, Path(d))
            self.assertTrue((Path(d)/"cleanup-failure.json").exists())
    def test_only_index_change_gets_one_read_retry_and_original_evidence_remains(self):
        with tempfile.TemporaryDirectory() as d:
            owner = SimpleNamespace(token="owned")
            with patch("vectorbench.orchestration.isolation.docker", return_value="owned"), patch("vectorbench.workloads.workload.point", side_effect=[UnverifiedTelemetry("segment set changed during measured reads"), {"status": "PASS"}]) as point:
                result = read_point(owner, None, None, SimpleNamespace(vectors=[]), [], [], [], Path(d), None, {})
            self.assertEqual(point.call_count, 2)
            self.assertEqual(len(result["read_failures"]), 1)
            self.assertTrue((Path(d)/"read-1/unverified.json").exists())
            self.assertTrue((Path(d)/"guard-1/isolation-after.json").exists())
            self.assertTrue((Path(d)/"guard-2/isolation-after.json").exists())
            self.assertTrue(result["selected_read"].endswith("read-2"))

    def test_plain_fallback_or_missing_telemetry_never_retries(self):
        for message in ["plain/exact/non-HNSW fallback occurred", "incomplete segment telemetry; not zero counters"]:
            with tempfile.TemporaryDirectory() as d:
                with patch("vectorbench.orchestration.isolation.docker", return_value="owned"), patch("vectorbench.workloads.workload.point", side_effect=UnverifiedTelemetry(message)) as point:
                    with self.assertRaises(UnverifiedTelemetry):
                        read_point(SimpleNamespace(token="owned"), None, None, SimpleNamespace(vectors=[]), [], [], [], Path(d), None, {})
                self.assertEqual(point.call_count, 1)


if __name__ == "__main__":
    unittest.main()
