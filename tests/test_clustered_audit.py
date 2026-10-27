"""Mutations against the independent G5 inventory validator; no DB or Dagster run."""
from pathlib import Path
import unittest

from vectorbench.evidence.contracts import load
from vectorbench.experiments.real_vectors.clustered.campaign import catalogue
from scripts.audit_clustered_completion import EXPECTED, audit_inventory, assert_db_exclusive


MATRIX = load(Path(__file__).resolve().parents[1] / "docs" / "real-vectors" / "clustered-insertion-matrix.json")


def point(db, count):
    ann = ({"status": "PASS", "seqscan": "off", "verified_queries": count}
           if db == "postgres" else
           {"status": "PASS", "query_count": count, "delta": {"unfiltered_hnsw": count,
                                                                  "unfiltered_plain": 0, "unfiltered_exact": 0}})
    return {"status": "PASS", "metric": "ip", "queries": count, "ann": ann}


def fixture():
    nodes = catalogue(MATRIX)
    refs = {dataset: {"dataset": dataset, "base_sha256": "fixture"} for dataset in MATRIX["datasets"]}
    manifest = {"mode": "real-clustered-insertion", "nodes": nodes,
                "matrix": MATRIX, "expected": MATRIX["expected"], "source_refs": refs}
    receipts = {}
    for node, spec in nodes.items():
        action, role = spec["action"], spec["role"]
        db = spec.get("db")
        if action == "prepare":
            value = {"dataset": spec["dataset"], "source": refs[spec["dataset"]],
                     "seeds": {str(s): {} for s in (7, 11, 29, 47)}}
        elif action == "candidate":
            value = {"status": "PASS", "points": [{"state": state, "ef": ef, "point": point(db, 200)}
                                                   for state in ("C5:0", "C5:1", "C5:3", "C5:5", "fresh")
                                                   for ef in (100, 128, 256, 512, 1000)]}
        elif action == "select":
            value = {"status": "PASS", "candidates": [{} for _ in range(10)], "ef": 256}
        elif action == "initial":
            value = {"status": "PASS", "archive": {"archive_sha": "fixture"},
                     "before_restart": point(db, 8), "after_restart": point(db, 8)}
        elif action == "history":
            steps = list(range(2 if spec["path"].endswith("1") else 6))
            final_count = 600 if role.startswith("history_slot") else 200
            value = {"status": "PASS", "steps": steps,
                     "writes": [{"status": "ACKNOWLEDGED", "written": 10000 // (len(steps)-1)}
                                for _ in steps[1:]],
                     "points": [point(db, final_count if index == len(steps)-1 else 200)
                                for index in steps],
                     "ef_points": [{"ef": ef, "point": point(db, 600)} for ef in (128, 512)]
                                  if role.startswith("history_slot") else []}
        elif action == "fresh":
            state = spec["state"]
            count = 200 if ":step" in state or role == "smoke_fresh" else 600
            value = {"status": "PASS", "state": state, "point": point(db, count),
                     "ef_points": [{"ef": ef, "point": point(db, 600)} for ef in (128, 512)]
                                  if role == "fresh" and state == "final" else []}
        elif action == "path_guard":
            value = {"status": "PASS", "positive": point(db, 4),
                     "negative_controls": [{"name": str(i), "status": "PASS", "rejected": "AssertionError"}
                                           for i in range(3)]}
        elif action == "cost_gate":
            value = {"status": "PASS", "deadline": 1793113013}
        elif action == "report":
            value = {"status": "PASS", "counts": EXPECTED}
        else:
            value = {"status": "PASS"}
        receipts[node] = {"result": value}
    manifest["execution_window_end"] = 1793113013
    return manifest, receipts


class ClusteredAuditTests(unittest.TestCase):
    def test_complete_inventory(self):
        manifest, receipts = fixture()
        self.assertEqual(audit_inventory(manifest, receipts, files=False), EXPECTED)

    def test_missing_measurement_quantity_rejected(self):
        manifest, receipts = fixture()
        node = next(key for key, spec in manifest["nodes"].items() if spec["role"] == "history_slot0")
        receipts[node]["result"]["ef_points"].pop()
        with self.assertRaisesRegex(AssertionError, "additional ef pair"):
            audit_inventory(manifest, receipts, files=False)

    def test_missing_negative_path_control_rejected(self):
        manifest, receipts = fixture()
        receipts["path_guard:postgres"]["result"]["negative_controls"].pop()
        with self.assertRaisesRegex(AssertionError, "negative path controls"):
            audit_inventory(manifest, receipts, files=False)

    def test_non_ann_point_rejected(self):
        manifest, receipts = fixture()
        node = next(key for key, spec in manifest["nodes"].items() if spec["role"] == "history_slot0")
        receipts[node]["result"]["points"][-1]["ann"]["status"] = "UNVERIFIED"
        with self.assertRaisesRegex(AssertionError, "unqualified IP ANN"):
            audit_inventory(manifest, receipts, files=False)

    def test_report_count_disagreement_rejected(self):
        manifest, receipts = fixture()
        receipts["report:all"]["result"]["counts"] = {**EXPECTED, "evaluation_queries": 211000}
        with self.assertRaisesRegex(AssertionError, "report completion counts"):
            audit_inventory(manifest, receipts, files=False)

    def test_forecast_exception_must_keep_block_label(self):
        manifest, receipts = fixture()
        manifest["forecast_exception"] = {"path": "fixture", "sha256": "fixture"}
        gate = receipts["cost_gate:all"]["result"]
        gate.update(forecast_status="BLOCK", admission_status="USER_AUTHORIZED_FORECAST_EXCEPTION",
                    forecast_exception=manifest["forecast_exception"])
        self.assertEqual(audit_inventory(manifest, receipts, files=False), EXPECTED)
        gate["forecast_status"] = "PASS"
        with self.assertRaisesRegex(AssertionError, "BLOCK forecast exception"):
            audit_inventory(manifest, receipts, files=False)

    def test_adopted_predecessor_db_overlap_rejected(self):
        with self.assertRaisesRegex(AssertionError, "adopted predecessor DB interval overlapped"):
            assert_db_exclusive([(10, 30, "db", "old-db"), (20, 25, "cpu", "old-cpu")],
                                "adopted predecessor")
        assert_db_exclusive([(10, 20, "db", "old-db"), (20, 25, "cpu", "old-cpu")],
                            "adopted predecessor")


if __name__ == "__main__":
    unittest.main()
