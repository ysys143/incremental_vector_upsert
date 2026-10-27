"""Check the actual history consumer's observation policy; no DB-physics claim."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vectorbench.backends.pilot import Qdrant
from vectorbench.experiments.pilot.model import digest
from vectorbench.experiments.synthetic_insertion.contracts import case_specs, config
from vectorbench.experiments.synthetic_insertion.data import base_data, make_fixture, states_for, waves_for
from vectorbench.experiments.synthetic_insertion.worker import incremental


class ObservationPolicy(unittest.TestCase):
    def exercise(self, policy):
        c = {**config(), "n": 100, "dim": 16, "queries_per_cohort": 2}
        f = make_fixture(c, 11, "iid", base_data(100, 16, 11))
        spec = next(s for s in case_specs(c) if s["db"] == "qdrant" and s["seed"] == 11
                    and s["geometry"] == "iid" and s["ratio"] == 100 and s["waves"] == 5 and s["policy"] == policy)
        spec = {**spec, "fixture": "test", "base": "test"}
        spec["states"] = [digest(ids, f["pool"][ids]) for ids in states_for(spec, c, f)]
        spec["wave_hashes"] = [digest(w, f["pool"][w]) for w in waves_for(spec, c, f)]
        events, stored = [], {i: f["pool"][i] for i in range(c["n"])}
        a = Qdrant.__new__(Qdrant)
        a.table = "items"

        def request(method, path, body=None):
            events.append("write")
            for i, v in zip(body["batch"]["ids"], body["batch"]["vectors"]):
                stored[i] = v
            return {"result": {"status": "completed"}}

        def state():
            events.append("metadata")
            return {}

        def snapshot():
            events.append("readback")
            ids = sorted(stored)
            return ids, [stored[i] for i in ids]

        def point(adapter, fixture, metadata, h, root, out):
            events.append("measure")
            ids = np.array(sorted(stored))
            self.assertEqual(h, digest(ids, np.asarray([stored[i] for i in ids], dtype=np.float32)))
            return {"data_hash": h}

        a.request, a.state, a.snapshot = request, state, snapshot
        fake_server = type("Server", (), {"resources": lambda self: {}})()
        base = {"result": {"snapshot": {"archive": "unused", "archive_sha": "unused", "files_hash": "unused"},
                           "chosen": {"ef": 128}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("vectorbench.experiments.synthetic_insertion.worker.fixture", return_value=(f, {})), \
                 patch("vectorbench.experiments.synthetic_insertion.worker.acknowledged", return_value=base), \
                 patch("vectorbench.experiments.synthetic_insertion.worker.owner_run", side_effect=lambda db, out, action, archive: action(fake_server, a)), \
                 patch("vectorbench.experiments.synthetic_insertion.worker.measure_point", side_effect=point), \
                 patch("vectorbench.experiments.synthetic_insertion.worker.space_guard"):
                result = incremental(root, spec, root / "out", {"config": c})
        self.assertEqual(len(result["writes"]), 5)
        self.assertTrue(all(len(w["requests"]) == 1 for w in result["writes"]))
        return events

    def test_continuous_has_no_intermediate_db_observation(self):
        events = self.exercise("continuous")
        first, last = events.index("write"), len(events) - 1 - events[::-1].index("write")
        self.assertEqual(events[first:last + 1], ["write"] * 5)
        self.assertEqual(events.count("measure"), 2)

    def test_settled_measures_each_wave(self):
        events = self.exercise("settled")
        self.assertEqual(events.count("measure"), 6)


if __name__ == "__main__":
    unittest.main()
