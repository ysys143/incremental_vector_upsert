"""Chunked float64 exact ranking over original float32 real embeddings."""
import hashlib
import numpy as np


def checked(ids, vectors, queries):
    ids = np.asarray(ids, dtype=np.int64)
    x, q = np.asarray(vectors, dtype=np.float32), np.asarray(queries, dtype=np.float32)
    if ids.ndim != 1 or x.ndim != 2 or q.ndim != 2 or len(ids) != len(x) or x.shape[1] != q.shape[1]:
        raise ValueError("invalid ID/vector/query dimensions")
    if len(set(map(int, ids))) != len(ids) or np.any(ids < 0):
        raise ValueError("IDs must be unique and nonnegative")
    if not np.isfinite(x).all() or not np.isfinite(q).all():
        raise ValueError("non-finite vector or query")
    return ids, x, q


def exact(ids, vectors, queries, k=100, metric="ip", exclude=None, block=64):
    """Lower cost is better: -dot for IP, squared distance for L2; tie by ID."""
    ids, vectors, queries = checked(ids, vectors, queries)
    if metric not in {"ip", "l2"} or k < 1 or block < 1:
        raise ValueError("unsupported metric or invalid k/block")
    if exclude is not None and len(exclude) != len(queries):
        raise ValueError("self-exclusion must match the query rows")
    if exclude is not None and any(np.count_nonzero(ids == own) != 1 for own in exclude):
        raise ValueError("self-exclusion IDs must occur exactly once")
    take = min(k, max(0, len(ids) - (exclude is not None)))
    if take == 0:
        return np.empty((len(queries), 0), dtype=np.int64)
    x = vectors.astype(np.float64)
    norms = np.einsum("ij,ij->i", x, x) if metric == "l2" else None
    output = []
    for start in range(0, len(queries), block):
        q = queries[start:start + block].astype(np.float64)
        costs = -q @ x.T
        if metric == "l2":
            costs = np.maximum(0, norms[None, :] + np.einsum("ij,ij->i", q, q)[:, None] + 2 * costs)
        if exclude is not None:
            for offset, own in enumerate(exclude[start:start + len(q)]):
                costs[offset, ids == own] = np.inf
        for row in costs:
            threshold = np.partition(row, take - 1)[take - 1]
            candidates = np.flatnonzero(row <= threshold)
            output.append(ids[candidates[np.lexsort((ids[candidates], row[candidates]))[:take]]])
    return np.asarray(output, dtype=np.int64).reshape(len(queries), take)


def direct_exact(ids, vectors, queries, k=100, metric="ip"):
    """Independent scalar-query oracle for checks, using direct L2 differences."""
    ids, x, queries = checked(ids, vectors, queries)
    x = x.astype(np.float64)
    output = []
    if metric not in {"ip", "l2"}:
        raise ValueError(metric)
    for q in queries.astype(np.float64):
        costs = -np.sum(x * q, axis=1) if metric == "ip" else np.sum((x - q) ** 2, axis=1)
        output.append(ids[np.lexsort((ids, costs))[:min(k, len(ids))]])
    return np.asarray(output, dtype=np.int64).reshape(len(queries), min(k, len(ids)))


def state_hash(ids, vectors, attributes=None):
    """Same IDs with new vector bytes are a different state; input order is irrelevant."""
    ids, x, _ = checked(ids, vectors, np.empty((0, np.asarray(vectors).shape[1]), dtype=np.float32))
    order = np.argsort(ids)
    h = hashlib.sha256()
    h.update(ids[order].astype("<i8").tobytes())
    h.update(x[order].astype("<f4").tobytes())
    if attributes is not None:
        a = np.asarray(attributes)
        if a.shape[0] != len(ids) or a.dtype.kind not in "iu":
            raise ValueError("integer metadata rows must match current IDs")
        h.update(a[order].astype("<i8").tobytes())
    return h.hexdigest()
