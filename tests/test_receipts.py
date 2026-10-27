"""Fault injection against real smoke artifacts (set PILOT_SMOKE_ROOT)."""
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from vectorbench.experiments.pilot.report import validate_run


@unittest.skipUnless(os.environ.get("PILOT_SMOKE_ROOT"), "requires a real smoke root")
class ReceiptBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "copy"
        shutil.copytree(os.environ["PILOT_SMOKE_ROOT"], self.root)

    def test_real_consumer_accepts_complete_receipt(self):
        self.assertEqual(validate_run(self.root), 18)

    def test_missing_checkpoint_rejected(self):
        next(self.root.glob("seed-*/*/*/checkpoint-1.json")).unlink()
        with self.assertRaises(FileNotFoundError):
            validate_run(self.root)

    def test_false_summary_rejected(self):
        path = next(self.root.glob("seed-*/*/*/checkpoint-1.json"))
        receipt = json.loads(path.read_text())
        receipt["recall100"] = .12345
        path.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(AssertionError, "raw results"):
            validate_run(self.root)

    def test_raw_corruption_rejected(self):
        path = next(self.root.glob("seed-*/*/*/checkpoint-1.npz"))
        path.write_bytes(path.read_bytes() + b"corruption")
        with self.assertRaisesRegex(AssertionError, "hash mismatch"):
            validate_run(self.root)

    def test_changed_index_identity_rejected(self):
        path = self.root / "seed-11/postgres/insert/checkpoint-1.json"
        receipt = json.loads(path.read_text())
        receipt["state"]["index_oid"] = str(int(receipt["state"]["index_oid"]) + 1)
        path.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(AssertionError, "identity changed"):
            validate_run(self.root)


if __name__ == "__main__":
    unittest.main()
