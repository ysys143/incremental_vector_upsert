"""Adapter/guard consumers, using explicit local fakes rather than claiming DB smoke."""
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vectorbench.backends.adapters import Postgres, Qdrant
from vectorbench.evidence.contracts import ResourceGate
from vectorbench.datasets.data import read_fbin
from vectorbench.backends.server import Server


class ConsumerTests(unittest.TestCase):
    def test_postgres_query_rejects_wrong_metric_before_search_execution(self):
        adapter = object.__new__(Postgres)
        adapter.table, adapter.metric, adapter.predicate = "items", "ip", None
        executed = []
        plan = [{"Plan": {"Node Type": "Index Scan", "Index Name": "items_hnsw", "Order By": "embedding <-> query"}}]

        def sql(statement):
            if statement.startswith("SHOW "):
                return "off\n" + json.dumps(plan)
            executed.append(statement)
            return "[9]"

        adapter.sql = sql
        with self.assertRaises(AssertionError):
            adapter.query(np.array([1., 0.], dtype=np.float32))
        self.assertEqual(executed, [])
        plan[0]["Plan"]["Order By"] = "embedding <#> query"
        self.assertEqual(adapter.query(np.array([1., 0.], dtype=np.float32)), [9])
        self.assertEqual(len(executed), 1)
        self.assertIn(" <#> ", executed[0])

    def test_qdrant_metric_and_actual_query_body(self):
        adapter = object.__new__(Qdrant)
        adapter.metric, adapter.batch, adapter.ef, adapter.predicate = "ip", 1000, 128, None
        calls = []

        def request(method, path, body=None):
            calls.append((method, path, body))
            if path.endswith("/exists"):
                return {"result": {"exists": False}}
            if path.endswith("/points/query"):
                return {"result": {"points": [{"id": 7}]}}
            return {"result": {"status": "completed"}}

        adapter.request = request
        adapter.wait = lambda: {"settled": True}
        adapter.setup([0, 1], np.ones((2, 768), dtype=np.float32), [16, 100])
        config = next(body for method, path, body in calls if method == "PUT" and path == "/collections/items")
        self.assertEqual(config["vectors"], {"size": 768, "distance": "Dot"})
        self.assertEqual(adapter.query(np.ones(768, dtype=np.float32)), [7])
        body = calls[-1][2]
        self.assertFalse(body["params"]["exact"])
        self.assertEqual(body["params"]["hnsw_ef"], 128)
        self.assertEqual(len(body["query"]), 768)

    def test_disk_block_occurs_before_any_docker_consumer(self):
        with tempfile.TemporaryDirectory() as d:
            server = Server("postgres", Path(d))
            with patch("vectorbench.backends.server.check_space", side_effect=ResourceGate("below original floor")), patch("vectorbench.backends.server.docker") as docker:
                with self.assertRaises(ResourceGate):
                    server.start()
                docker.assert_not_called()

    def test_fbin_is_readonly_and_malformed_file_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "fixture.fbin"
            path.write_bytes(struct.pack("<II", 2, 3) + np.arange(6, dtype="<f4").tobytes())
            array = read_fbin(path, dimension=3)
            self.assertEqual(array.shape, (2, 3))
            with self.assertRaises(ValueError):
                array[0, 0] = 42
            del array
            path.write_bytes(struct.pack("<II", 2, 3) + np.arange(5, dtype="<f4").tobytes())
            with self.assertRaises(ValueError):
                read_fbin(path, dimension=3)
            path.write_bytes(struct.pack("<II", 2, 3) + np.array([0, 1, 2, 3, 4, np.inf], dtype="<f4").tobytes())
            with self.assertRaises(ValueError):
                read_fbin(path, dimension=3)


if __name__ == "__main__":
    unittest.main()
