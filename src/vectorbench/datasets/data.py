"""Read-only, provenance-bound public vector pools and version-aware exact states."""
from dataclasses import dataclass
import hashlib
from pathlib import Path
import struct

import numpy as np

from vectorbench.evidence.contracts import Config, check_space, evidence_key, load, save, sha
from vectorbench.evaluation.metrics import direct_exact, exact, state_hash
from vectorbench.datasets.source import DATASETS


def read_fbin(path, dimension=768):
    path = Path(path)
    with path.open("rb") as stream:
        raw = stream.read(8)
    if len(raw) != 8:
        raise ValueError("truncated fbin header")
    rows, d = struct.unpack("<II", raw)
    if d != dimension or rows < 1 or path.stat().st_size != 8 + rows * d * 4:
        raise ValueError("fbin dimension/count/file size mismatch")
    result = np.memmap(path, dtype="<f4", mode="r", offset=8, shape=(rows, d))
    for start in range(0, rows, 4096):
        if not np.isfinite(result[start:start+4096]).all():
            raise ValueError("public vector file contains non-finite values")
    return result


def verified_file(path, dataset, role):
    path = Path(path)
    receipt = load(path.with_suffix(path.suffix + ".receipt.json"))
    expected_url = DATASETS[dataset].base if role == "base" else DATASETS[dataset].queries
    if receipt["status"] != "PASS" or receipt["source"]["url"] != expected_url:
        raise AssertionError("input is not a completed declared public-vector source")
    if receipt["source"]["dtype"] != "<f4" or receipt["source"]["metric"] != "ip" or not receipt["source"]["etag"]:
        raise AssertionError("input precision/metric/source identity mismatch")
    if sha(path) != receipt["sha256"]:
        raise AssertionError("public vector bytes differ from the acknowledged source")
    array = read_fbin(path, DATASETS[dataset].dimension)
    if len(array) != receipt["rows"]:
        raise AssertionError("sample count differs from its receipt")
    original_rows, cursor, last_end = [], 0, -1
    for block in receipt["blocks"]:
        source_start, count = block["source_row"], block["rows"]
        if source_start < last_end or source_start + count > receipt["source"]["rows"]:
            raise AssertionError("overlapping/out-of-range source blocks")
        digest = hashlib.sha256(array[cursor:cursor+count].tobytes()).hexdigest()
        if digest != block["sha256"]:
            raise AssertionError("sampled block differs from downloaded source bytes")
        original_rows.extend(range(source_start, source_start+count))
        cursor += count; last_end = source_start + count
    if cursor != len(array):
        raise AssertionError("source block ledger does not cover the vector file")
    return array, np.asarray(original_rows, dtype=np.int64), receipt


@dataclass(frozen=True)
class Pool:
    dataset: str
    vectors: np.ndarray
    queries: np.ndarray
    original_rows: np.ndarray
    original_query_rows: np.ndarray
    base_sha: str
    query_sha: str

    @classmethod
    def open(cls, dataset, base, queries):
        if dataset not in DATASETS:
            raise ValueError("undeclared dataset")
        x, rows, a = verified_file(base, dataset, "base")
        q, query_rows, b = verified_file(queries, dataset, "queries")
        return cls(dataset, x, q, rows, query_rows, a["sha256"], b["sha256"])

    def split(self, seed, config=None):
        c = config or Config()
        c.validate()
        if len(self.vectors) < c.n + int(c.n*c.update_fraction)*c.long_rounds:
            raise ValueError("real pool cannot supply the full long history")
        if len(self.queries) < c.calibration_queries + c.query_rows:
            raise ValueError("not enough disjoint calibration/evaluation queries")
        order = np.random.default_rng(seed).permutation(len(self.vectors))
        # Query split is fixed independently of the evaluation data/event seed.
        qorder = np.random.default_rng(20261025).permutation(len(self.queries))
        return {"initial_rows": order[:c.n], "replacement_rows": order[c.n:],
                "calibration_query_rows": qorder[:c.calibration_queries],
                "evaluation_query_candidates": qorder[c.calibration_queries:]}


def oracle(pool, live, query_rows, out, config=None):
    """Write a declared state once; graph-level deduplication owns any reuse."""
    c = config or Config()
    out = Path(out)
    if out.exists() or out.with_suffix(out.suffix + ".receipt.json").exists():
        raise FileExistsError("oracle evidence already exists; verify its native producer instead of overwriting")
    records = live.records()
    ids = np.asarray([w.id for w in records], dtype=np.int64)
    rows = np.asarray([w.row for w in records], dtype=np.int64)
    vectors = pool.vectors[rows]
    queries = np.asarray(pool.queries[query_rows], dtype=np.float32)
    vector_state = state_hash(ids, vectors)
    query_state = hashlib.sha256(queries.astype("<f4").tobytes()).hexdigest()
    identity = evidence_key(pool.base_sha, vector_state, query_state, "ip")
    check_space(out.parent, c, reserve=len(queries)*min(100, len(ids))*8 + 65536)
    truth = exact(ids, vectors, queries, metric="ip")
    np.testing.assert_array_equal(truth[:8], direct_exact(ids, vectors, queries[:8], metric="ip"))
    out.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(out, truth=truth.astype(np.int32), ids=ids.astype(np.int32), rows=rows.astype(np.int32),
                        query_rows=np.asarray(query_rows, dtype=np.int32))
    receipt = {"status": "PASS", "dataset": pool.dataset, "data_kind": "real-precomputed", "metric": "ip",
               "key": identity, "source_sha256": pool.base_sha, "source_queries_sha256": pool.query_sha,
               "vector_state_sha256": vector_state, "ledger_sha256": live.digest(), "query_sha256": query_state,
               "count": len(ids), "query_count": len(queries), "sha256": sha(out)}
    save(out.with_suffix(out.suffix + ".receipt.json"), receipt)
    return receipt
