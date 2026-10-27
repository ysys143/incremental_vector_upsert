import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np

from vectorbench.evidence.contracts import save, sha
from vectorbench.workloads.events import Live, initial
from vectorbench.evaluation.metrics import exact, state_hash
from vectorbench.workloads.workload import oracle_for


class OracleIdentityTests(unittest.TestCase):
    def test_query_change_outside_spot_checks_rejected(self):
        rng = np.random.default_rng(1)
        x = rng.normal(size=(10, 4)).astype(np.float32)
        q = rng.normal(size=(12, 4)).astype(np.float32)
        pool = SimpleNamespace(vectors=x, queries=q, base_sha="base", query_sha="query-file")
        live = Live(initial(range(10)), 10)
        query_rows = np.arange(12)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "oracle.npz"
            np.savez_compressed(p, ids=np.arange(10), rows=np.arange(10), query_rows=query_rows,
                                truth=exact(np.arange(10), x, q))
            save(p.with_suffix(".npz.receipt.json"), {"source_sha256": "base", "source_queries_sha256": "query-file",
                "metric": "ip", "vector_state_sha256": state_hash(np.arange(10), x),
                "query_sha256": hashlib.sha256(q.astype("<f4").tobytes()).hexdigest()})
            ref = {"path": str(p), "sha256": sha(p)}
            self.assertEqual(oracle_for(ref, pool, live, query_rows).shape, (12, 10))
            pool.queries = q.copy();pool.queries[11] *= -1
            with self.assertRaises(AssertionError):
                oracle_for(ref, pool, live, query_rows)


if __name__ == "__main__":
    unittest.main()
