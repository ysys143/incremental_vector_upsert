"""Focused successor adoption and resource-origin tests; no native DB execution."""
from pathlib import Path
from tempfile import TemporaryDirectory
import json
from types import SimpleNamespace
import unittest

from vectorbench.evidence.contracts import load, save, sha
from vectorbench.evidence.registry import Registry
from vectorbench.experiments.real_vectors.clustered.adopt import (
    original_timing, qualified_evidence_checks, verify_exception)
from vectorbench.experiments.real_vectors.clustered.diagnostics import resources


class ClusteredAdoptionTests(unittest.TestCase):
    def test_multi_asset_run_selects_own_evidence_and_rejects_wrong_asset(self):
        def check(role, passed=True):
            return SimpleNamespace(asset_key=SimpleNamespace(path=(role,)),
                                   check_name="evidence", partition="msmarco-postgres", passed=passed,
                                   target_materialization_data=SimpleNamespace(run_id="shared-run"))
        own = check("smoke_initial")
        neighbor = check("smoke_fresh")
        spec = {"id": "msmarco-postgres"}
        self.assertIs(qualified_evidence_checks([neighbor, own],
                                                "smoke_initial:msmarco-postgres", spec, "shared-run"), own)
        with self.assertRaisesRegex(AssertionError, "target asset evidence check"):
            qualified_evidence_checks([neighbor], "smoke_initial:msmarco-postgres", spec, "shared-run")
        with self.assertRaisesRegex(AssertionError, "failed blocking checks"):
            qualified_evidence_checks([own, check("smoke_fresh", False)],
                                      "smoke_initial:msmarco-postgres", spec, "shared-run")
        with self.assertRaisesRegex(AssertionError, "different materialization run"):
            qualified_evidence_checks([own, neighbor], "smoke_initial:msmarco-postgres", spec, "foreign-run")

    def test_original_native_timing_keeps_lease_cost_basis(self):
        value = original_timing({"started": 10, "lease_acquired": 13, "finished": 24,
                                 "worker_peak_rss_bytes": 99}, 25, {"ended": 24.5})
        self.assertEqual(value["elapsed_s"], 11)
        self.assertEqual(value["wall_elapsed_s"], 14)
        self.assertEqual(value["lease_acquired"], 13)
        self.assertEqual(value["finished"], 24)

    def test_forecast_exception_must_target_failed_block(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            old = root / "prior"
            old.mkdir()
            auth = old / "authorization.json"
            save(auth, {"t0": 100, "deadline": 43300})
            prior_manifest = old / "manifest.json"
            save(prior_manifest, {"authorization": {"path": str(auth)}})
            failed = old / "cells" / "cost_gate__all" / "old-run" / "cost-gate.json"
            save(failed, {"status": "BLOCK"})
            registry = Registry(old / "admission.sqlite")
            with registry.connection() as connection:
                connection.execute("INSERT INTO attempts(node,run_id,spec_hash,started,ended,outcome) VALUES (?,?,?,?,?,?)",
                                   ("cost_gate:all", "old-run", "hash", 101, 110, "failed"))
            approval = root / "forecast-exception.json"
            record = {"action": "PROCEED_DESPITE_FORECAST_BLOCK", "user_text": "그냥 진행해.",
                      "recorded_at": 120, "original_t0": 100, "original_deadline": 43300,
                      "predecessor_root": str(old), "predecessor_manifest_sha256": sha(prior_manifest),
                      "failed_cost_gate": {"path": str(failed), "sha256": sha(failed)}}
            save(approval, record)
            successor = {"forecast_exception": {"path": str(approval), "sha256": sha(approval)},
                         "execution_window_end": 43300}
            self.assertEqual(verify_exception(successor, old)["failed_cost_gate_sha256"], sha(failed))
            save(failed, {"status": "PASS"})
            record["failed_cost_gate"]["sha256"] = sha(failed)
            save(approval, record)
            successor["forecast_exception"]["sha256"] = sha(approval)
            with self.assertRaisesRegex(AssertionError, "failed BLOCK forecast"):
                verify_exception(successor, old)

    def test_adopted_resources_use_original_receipt_and_samples(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            old = root / "prior"
            current = root / "current"
            original = old / "cells" / "smoke_initial__msmarco-postgres" / "old-run"
            adopted = current / "cells" / "smoke_initial__msmarco-postgres" / "new-run"
            original.mkdir(parents=True)
            adopted.mkdir(parents=True)
            old_receipt = original / "receipt.json"
            save(old_receipt, {"status": "PASS", "node": "smoke_initial:msmarco-postgres",
                               "run_id": "old-run", "resource": "db", "started": 10,
                               "finished": 20, "worker_peak_rss_bytes": 900})
            (original / "resource-samples.jsonl").write_text(json.dumps({
                "summed_process_rss_bytes": 600, "database_disk_bytes": 700,
                "host_free_bytes": 800}) + "\n")
            save(adopted / "receipt.json", {"status": "PASS", "node": "smoke_initial:msmarco-postgres",
                                                 "run_id": "new-run", "resource": "db", "started": 30,
                                                 "finished": 31, "worker_peak_rss_bytes": 10})
            timing = {"elapsed_s": 9}
            save(adopted / "adoption-proof.json", {"result": {"original_native_timing": timing},
                     "reference": {"root": str(old), "node": "smoke_initial:msmarco-postgres",
                                   "run_id": "old-run", "receipt_sha256": sha(old_receipt),
                                   "original_native_timing": timing}})
            out = root / "out"
            out.mkdir()
            result = resources(current, out)
            self.assertEqual(result["rows"][0]["origin"]["kind"], "adopted")
            self.assertEqual(result["rows"][0]["worker_peak_rss_bytes"], 900)
            self.assertEqual(result["rows"][0]["sampled_database_summed_rss_peak_bytes"], 600)
            self.assertEqual(result["rows"][0]["sampled_database_disk_peak_bytes"], 700)
            self.assertEqual(result["rows"][0]["started_at"], 10)


if __name__ == "__main__":
    unittest.main()
