"""Geometry-controlled fixtures, cohort queries, and chunked independent oracles."""
import math
import random

import numpy as np

from vectorbench.experiments.pilot.model import digest, direct_exact
from vectorbench.experiments.synthetic_insertion.contracts import GEOMETRIES


def nearest(ids, vectors, queries, k=100, exclude=None, distances=False):
    ids = np.asarray(ids, dtype=np.int64)
    x = np.asarray(vectors, dtype=np.float64)
    norms = np.einsum("ij,ij->i", x, x)
    results, radii = [], []
    for start in range(0, len(queries), 64):
        q = np.asarray(queries[start:start + 64], dtype=np.float64)
        d = np.maximum(0, np.einsum("ij,ij->i", q, q)[:, None] + norms - 2 * q @ x.T)
        if exclude is not None:
            for offset, own in enumerate(exclude[start:start + len(q)]):
                d[offset, ids == own] = np.inf
        for row in d:
            take = min(k, len(ids) - (1 if exclude is not None else 0))
            threshold = np.partition(row, take - 1)[take - 1]
            candidates = np.flatnonzero(row <= threshold)
            chosen = candidates[np.lexsort((ids[candidates], row[candidates]))[:take]]
            results.append(ids[chosen])
            radii.append(np.sqrt(row[chosen]))
    result = np.asarray(results, dtype=np.int64)
    return (result, np.asarray(radii)) if distances else result


def base_data(n, dim, seed):
    rng = np.random.default_rng(seed)
    rotation, _ = np.linalg.qr(rng.normal(size=(dim, dim)))
    a = 2 * math.sqrt(dim)
    labels = np.arange(n) % 2
    pre = rng.normal(size=(n, dim))
    pre[:, 0] += np.where(labels == 0, -a, a)
    base = (pre @ rotation).astype(np.float32)
    _, radii = nearest(np.arange(n), base, base, 20, exclude=np.arange(n), distances=True)
    return base, rotation, a, float(np.median(radii[:, -1]))


def make_fixture(c, seed, geometry, base_info):
    n, dim = c["n"], c["dim"]
    base, rotation, a, r20 = base_info
    if geometry not in GEOMETRIES:
        raise ValueError(geometry)
    rng = np.random.default_rng(seed + 1000)

    def sample(count, new=False):
        labels = np.arange(count) % 2
        pre = rng.normal(size=(count, dim))
        pre[:, 0] += np.where(labels == 0, -a, a)
        if new and geometry in {"near", "far"}:
            pre[:, 1] += r20 * (.25 if geometry == "near" else 3)
        if new and geometry == "bridge":
            pre[:, 0] = rng.uniform(-a, a, count) + rng.normal(size=count)
        return (pre @ rotation).astype(np.float32), labels, pre[:, 0]

    novel, labels, projection = sample(n, True)
    qcount = c["queries_per_cohort"]
    old_queries, _, _ = sample(qcount)
    new_queries, _, _ = sample(qcount, True)
    boundary = []
    candidates_seen = 0
    while len(boundary) < qcount and candidates_seen < 20000:
        count = min(512, 20000 - candidates_seen)
        old, _, _ = sample(count)
        new, _, _ = sample(count, True)
        weight = rng.uniform(.25, .75, (count, 1))
        candidates = (old * weight + new * (1 - weight)).astype(np.float32)
        _, d0 = nearest(np.arange(n), base, candidates, 1, distances=True)
        _, d1 = nearest(np.arange(n), novel, candidates, 1, distances=True)
        ratios = d0[:, 0] / np.maximum(d1[:, 0], 1e-15)
        boundary.extend(candidates[(ratios >= .8) & (ratios <= 1.25)])
        candidates_seen += count
    if len(boundary) < qcount:
        raise AssertionError("boundary fixture failed its declared candidate cap")
    queries = np.concatenate([old_queries, new_queries, np.asarray(boundary[:qcount])])
    order = list(range(n, 2 * n))
    random.Random(seed + 9000).shuffle(order)
    # Reorder exactly the same selected set, not a different set of novel vectors.
    return {"pool": np.concatenate([base, novel]), "queries": queries,
            "cohort": np.repeat(np.arange(3), qcount), "labels": labels,
            "projection": projection, "random_order": np.asarray(order, dtype=np.int64),
            "r20": np.asarray(r20), "boundary_candidates": np.asarray(candidates_seen)}


def waves_for(spec, c, fixture):
    count = c["n"] * spec["ratio"] // 100
    ids = fixture["random_order"][:count].tolist()
    if spec["order"] == "ordered":
        if spec["geometry"] == "far":
            ids.sort(key=lambda i: int(fixture["labels"][i - c["n"]]))
        else:
            ids.sort(key=lambda i: float(fixture["projection"][i - c["n"]]))
    return [np.asarray(p, dtype=np.int64) for p in np.array_split(ids, spec["waves"])]


def states_for(spec, c, fixture):
    alive = list(range(c["n"]))
    states = [np.asarray(alive, dtype=np.int64)]
    for wave in waves_for(spec, c, fixture):
        alive.extend(wave.tolist())
        states.append(np.asarray(sorted(alive), dtype=np.int64))
    return states


def graph_diagnostics(pool, ids, n, seed):
    vectors = pool[ids]
    neighbors, radii = nearest(ids, vectors, vectors, 20, exclude=ids, distances=True)
    np.testing.assert_array_equal(neighbors[:4], direct_exact(ids, vectors,
        vectors[:4], 21)[:, 1:])
    index = {int(v): i for i, v in enumerate(ids)}
    parent = list(range(len(ids)))
    size = [1] * len(ids)

    def find(v):
        while parent[v] != v:
            parent[v] = parent[parent[v]]
            v = parent[v]
        return v

    edges = set()
    cross = 0
    for a, row in zip(ids, neighbors):
        cross += sum((int(a) < n) != (int(b) < n) for b in row)
        ia = index[int(a)]
        for b in row:
            ib = index[int(b)]
            edges.add((min(ia, ib), max(ia, ib)))
            ra, rb = find(ia), find(ib)
            if ra != rb:
                if size[ra] < size[rb]:
                    ra, rb = rb, ra
                parent[rb] = ra
                size[ra] += size[rb]
    components = [size[i] for i in range(len(ids)) if parent[i] == i]
    degrees = np.zeros(len(ids), dtype=np.int64)
    cut = 0
    for a, b in edges:
        degrees[a] += 1
        degrees[b] += 1
        cut += (ids[a] < n) != (ids[b] < n)
    volume_old, volume_new = int(degrees[ids < n].sum()), int(degrees[ids >= n].sum())
    lids = -1 / np.mean(np.log(np.maximum(radii[:, :-1], 1e-15) / radii[:, -1:]), axis=1)
    result = {"count": len(ids), "components": len(components),
              "largest_component_fraction": max(components) / len(ids),
              "cross_neighbor_fraction": cross / neighbors.size,
              "conductance_old_new_cut": cut / min(volume_old, volume_new) if volume_new else None,
              "r20_quantiles": np.percentile(radii[:, -1], [10, 50, 90]).tolist(),
              "lid_quantiles": np.percentile(lids, [10, 50, 90]).tolist(),
              "definition": "symmetrized exact 20-NN data graph, not a product ANN graph"}
    if np.any(ids >= n):
        rng = np.random.default_rng(seed + 991)
        old = pool[rng.choice(np.arange(n), min(1024, n), replace=False)].astype(np.float64)
        newids = ids[ids >= n]
        new = pool[rng.choice(newids, min(1024, len(newids)), replace=False)].astype(np.float64)
        norm0 = np.sum(old**2, axis=1)
        d00 = np.maximum(0, norm0[:, None] + norm0 - 2 * old @ old.T)
        width = float(np.median(d00[np.triu_indices(len(old), 1)]))
        norm1 = np.sum(new**2, axis=1)
        d11 = np.maximum(0, norm1[:, None] + norm1 - 2 * new @ new.T)
        d01 = np.maximum(0, norm0[:, None] + norm1 - 2 * old @ new.T)
        k00, k11, k01 = np.exp(-d00 / (2 * width)), np.exp(-d11 / (2 * width)), np.exp(-d01 / (2 * width))
        result["mmd2_unbiased"] = float((k00.sum() - len(old)) / (len(old) * (len(old) - 1))
            + (k11.sum() - len(new)) / (len(new) * (len(new) - 1)) - 2 * k01.mean())
        result["mmd_bandwidth_squared"] = width
        _, new_to_old = nearest(np.arange(n), pool[:n], pool[newids], 1, distances=True)
        result["new_to_old_nn_median"] = float(np.median(new_to_old))
    return result


def validate_geometry(geometry, initial, final):
    if geometry == "iid" and not .35 <= final["cross_neighbor_fraction"] <= .65:
        raise AssertionError("IID fixture did not realize mixing")
    if geometry == "far" and final["cross_neighbor_fraction"] >= .01:
        raise AssertionError("far fixture did not realize separation")
    if geometry == "near" and final["cross_neighbor_fraction"] <= .02:
        raise AssertionError("near fixture did not realize overlap")
    if geometry == "bridge" and final["largest_component_fraction"] <= initial["largest_component_fraction"] + .1:
        raise AssertionError("bridge fixture did not realize added connectivity")
