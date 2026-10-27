"""Small raw-receipt analysis checks, explicitly not scientific/native evidence."""
from pathlib import Path
import tempfile
import unittest
import numpy as np
from vectorbench.evidence.contracts import save,sha
from vectorbench.reporting.baseline_report import paired_interval,query_scores


class AnalysisTests(unittest.TestCase):
    def test_paired_bootstrap_preserves_pairing_and_reproducibility(self):
        a=paired_interval([-.1,-.1,-.1],"fixed",draws=100)
        b=paired_interval([-.1,-.1,-.1],"fixed",draws=100)
        self.assertEqual(a,b)
        self.assertAlmostEqual(a["mean_pp"],-10)
        self.assertAlmostEqual(a["lower_pp"],-10)
        self.assertAlmostEqual(a["upper_pp"],-10)
        with self.assertRaises(ValueError): paired_interval([np.nan],"bad")

    def test_raw_short_result_and_changed_evidence_are_not_full_recall(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d); oracle=p/"oracle.npz"
            np.savez_compressed(oracle,truth=np.array([[4,5,6],[7,8,9]],dtype=np.int32))
            np.savez_compressed(p/"raw.npz",returned=np.array([[4,5,-1],[7,8,9]],dtype=np.int32),returned_lengths=np.array([2,3]))
            point={"selected_read":str(p),"oracle":{"path":str(oracle),"sha256":sha(oracle)},
                   "raw_sha256":sha(p/"raw.npz"),"recall10":5/6,"recall100":5/6}
            np.testing.assert_allclose(query_scores(point,100),[2/3,1])
            point["recall100"]=1
            with self.assertRaisesRegex(AssertionError,"summary"): query_scores(point,100)
            point["recall100"]=5/6
            (p/"raw.npz").write_bytes(b'changed')
            with self.assertRaisesRegex(AssertionError,"changed"): query_scores(point,100)


if __name__=='__main__': unittest.main()
