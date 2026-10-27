"""Deterministic inputs, event ledger, and independent exact quality checks."""
import hashlib
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Config:
    n: int = 10000
    dim: int = 128
    tune_queries: int = 200
    eval_queries: int = 800
    rounds: int = 5
    seeds: tuple = (11, 29, 47)

    @classmethod
    def smoke(cls):
        return cls(1000, 32, 20, 40, 2, (11,))


def fixture(config, seed):
    rng = np.random.default_rng(seed)
    centers = rng.normal(0, 0.35, (16, config.dim))

    def sample(count):
        labels = rng.integers(0, len(centers), count)
        return (centers[labels] + rng.normal(0, 1, (count, config.dim))).astype(np.float32)

    pool = sample(config.n + config.rounds * (config.n // 10))
    queries = sample(config.tune_queries + config.eval_queries)
    return pool, queries[:config.tune_queries], queries[config.tune_queries:]


def history(config, seed, scenario):
    if scenario not in {"insert", "delete", "churn"}:
        raise ValueError(scenario)
    rng = np.random.default_rng(seed + 10000)
    alive = np.arange(config.n, dtype=np.int64)
    chunk = config.n // 10
    yield alive.copy(), [], []
    for step in range(config.rounds):
        deleted = (rng.choice(alive, chunk, replace=False)
                   if scenario != "insert" else np.array([], dtype=np.int64))
        inserted = (np.arange(config.n + step * chunk, config.n + (step + 1) * chunk)
                    if scenario != "delete" else np.array([], dtype=np.int64))
        alive = np.sort(np.concatenate([alive[~np.isin(alive, deleted)], inserted]))
        yield alive.copy(), deleted.tolist(), inserted.tolist()


def digest(ids, vectors):
    h = hashlib.sha256()
    h.update(np.asarray(ids, dtype="<i8").tobytes())
    h.update(np.asarray(vectors, dtype="<f4").tobytes())
    return h.hexdigest()


def exact(ids, vectors, queries, k=100):
    """Chunked float64 distances; stable ID ascending tie-break, no distance matrix saved."""
    ids = np.asarray(ids, dtype=np.int64)
    order = np.argsort(ids)
    ids, x = ids[order], np.asarray(vectors[order], dtype=np.float64)
    norms = np.einsum("ij,ij->i", x, x)
    rows = []
    for start in range(0, len(queries), 64):
        q = np.asarray(queries[start:start + 64], dtype=np.float64)
        distances = np.einsum("ij,ij->i", q, q)[:, None] + norms[None, :] - 2 * q @ x.T
        for row in distances:
            rows.append(ids[np.argsort(row, kind="stable")[:min(k, len(ids))]])
    return np.asarray(rows, dtype=np.int64)


def direct_exact(ids, vectors, queries, k=100):
    rows = []
    x = np.asarray(vectors, dtype=np.float64)
    for q in queries:
        d = np.sum((x - q.astype(np.float64)) ** 2, axis=1)
        rows.append(np.asarray(ids)[np.lexsort((ids, d))[:min(k, len(ids))]])
    return np.asarray(rows, dtype=np.int64)


def validate_truth(ids, vectors, queries, truth):
    if not np.isin(truth, ids).all():
        raise AssertionError("oracle contains non-live IDs")
    reference = direct_exact(ids, vectors, queries[:8], truth.shape[1])
    np.testing.assert_array_equal(truth[:8], reference)


def validate_snapshot(ids, vectors, actual_ids, actual_vectors):
    order = np.argsort(actual_ids)
    np.testing.assert_array_equal(ids, np.asarray(actual_ids)[order])
    np.testing.assert_array_equal(vectors, np.asarray(actual_vectors, dtype=np.float32)[order])


def score(ids, truth, returned):
    if len(truth) != len(returned):
        raise AssertionError("query result count mismatch")
    alive = set(map(int, ids))
    for row in returned:
        if len(row) != len(set(row)) or not set(row).issubset(alive):
            raise AssertionError("duplicate or non-live result ID")
    result = {}
    for k in (10, 100):
        recalls, short = [], 0
        for expected, got in zip(truth, returned):
            target = set(map(int, expected[:k]))
            recalls.append(len(target.intersection(got[:k])) / len(target) if target else 1.0)
            short += len(got) < min(k, len(ids))
        result[f"recall{k}"] = float(np.mean(recalls))
        result[f"shortfall{k}"] = short / len(returned)
    return result


def packed_results(rows, k=100):
    """Keep short/empty responses without pickle or treating padding as returned IDs."""
    lengths = np.array([len(row) for row in rows], dtype=np.int64)
    if np.any(lengths > k):
        raise AssertionError("server returned more than requested k")
    packed = np.full((len(rows), k), -1, dtype=np.int64)
    for index, row in enumerate(rows):
        packed[index, :len(row)] = row
    return {"returned": packed, "returned_lengths": lengths}


def unpacked_results(raw):
    return [row[:int(length)].tolist() for row, length in
            zip(raw["returned"], raw["returned_lengths"])]
