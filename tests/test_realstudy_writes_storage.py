from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vectorbench.evidence.contracts import ResourceGate
from vectorbench.workloads.events import Live, initial, replacement_history
from vectorbench.orchestration.isolation import measured
from vectorbench.datasets.storage import load_events, load_state, save_events, save_state
from vectorbench.workloads.writes import metadata_only


class StorageAndWritesTests(unittest.TestCase):
    def test_lossless_event_and_state_roundtrip(self):
        start = initial(range(100))
        events = replacement_history(start, range(100, 600), rounds=50, operation="upsert", repeated=True, hot=True)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            save_events(p / "events.npz", events)
            restored = load_events(p / "events.npz")
            self.assertEqual([e.json() for e in events], [e.json() for e in restored])
            live = Live(start, 600)
            for event in restored:
                live.apply(event)
            save_state(p / "final.npz", live)
            self.assertEqual(load_state(p / "final.npz", 600).digest(), live.digest())
            with self.assertRaises(FileExistsError):
                save_events(p / "events.npz", events)

    def test_postgres_metadata_only_never_assigns_embedding(self):
        class Adapter:
            name, table = "postgres", "items"
            def __init__(self): self.calls = []
            def sql(self, statement): self.calls.append(statement)
        a = Adapter()
        metadata_only(a, [1, 2], [[2, 10, 1], [3, 20, 2]])
        self.assertEqual(len(a.calls), 3)
        self.assertIn("COPY ivu_metadata", a.calls[1])
        self.assertTrue(a.calls[-1].startswith("UPDATE items"))
        self.assertNotIn("embedding", "\n".join(a.calls))

    def test_qdrant_metadata_only_uses_payload_batch_not_upsert(self):
        class Adapter:
            name, table = "qdrant", "items"
            def __init__(self): self.calls = []
            def request(self, method, url, body): self.calls.append((method, url, body))
        a = Adapter()
        metadata_only(a, [1, 2], [[2, 10, 1], [3, 20, 2]])
        self.assertEqual(len(a.calls), 1)
        method, url, body = a.calls[0]
        self.assertEqual((method, url), ("POST", "/collections/items/points/batch?wait=true"))
        self.assertEqual(len(body["operations"]), 2)
        self.assertTrue(all(set(op) == {"set_payload"} for op in body["operations"]))

    def test_other_containers_are_recorded_without_blocking_callback(self):
        class Owner: token = "owned"
        with tempfile.TemporaryDirectory() as d:
            calls = []
            with patch("vectorbench.orchestration.isolation.docker", return_value="foreign"):
                measured(Owner(), Path(d), lambda: calls.append("executed"))
            self.assertEqual(calls, ["executed"])
            self.assertEqual(json.loads((Path(d)/"isolation-before.json").read_text())["foreign"], ["foreign"])
            with patch("vectorbench.orchestration.isolation.docker", side_effect=["owned", "owned\nforeign"]):
                measured(Owner(), Path(d), lambda: calls.append("executed"))
            self.assertEqual(calls, ["executed", "executed"])
            self.assertEqual(json.loads((Path(d)/"isolation-after.json").read_text())["foreign"], ["foreign"])
            self.assertFalse((Path(d)/"isolation-unverified.json").exists())


if __name__ == "__main__":
    unittest.main()
