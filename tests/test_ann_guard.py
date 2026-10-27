import copy
import unittest

from vectorbench.backends.pilot import assert_postgres_ann


def plan():
    return [{"Plan": {"Node Type": "Limit", "Plans": [{"Node Type": "Index Scan",
        "Index Name": "items_hnsw", "Order By": "(embedding <-> query)", "Actual Loops": 1}]}}]


class AnnGuardTests(unittest.TestCase):
    def test_executed_hnsw_is_accepted(self):
        assert_postgres_ann(plan(), "off", "items_hnsw", analyzed=True)

    def test_seqscan_on_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "enable_seqscan"):
            assert_postgres_ann(plan(), "on", "items_hnsw")

    def test_seqscan_with_index_name_in_comment_is_rejected(self):
        p = [{"Plan": {"Node Type": "Seq Scan", "Comment": "items_hnsw"}}]
        with self.assertRaisesRegex(AssertionError, "forbidden"):
            assert_postgres_ann(p, "off", "items_hnsw")

    def test_nested_seqscan_is_rejected_even_alongside_hnsw(self):
        p = copy.deepcopy(plan())
        p[0]["Plan"]["Plans"].append({"Node Type": "Seq Scan"})
        with self.assertRaisesRegex(AssertionError, "forbidden"):
            assert_postgres_ann(p, "off", "items_hnsw")

    def test_other_index_and_unexecuted_index_are_rejected(self):
        for key, value in (("Index Name", "items_pkey"), ("Actual Loops", 0)):
            p = copy.deepcopy(plan())
            p[0]["Plan"]["Plans"][0][key] = value
            with self.assertRaises(AssertionError):
                assert_postgres_ann(p, "off", "items_hnsw", analyzed=True)

    def test_missing_index_is_rejected(self):
        with self.assertRaises(AssertionError):
            assert_postgres_ann([{"Plan": {"Node Type": "Limit"}}], "off", "items_hnsw")


if __name__ == "__main__":
    unittest.main()
