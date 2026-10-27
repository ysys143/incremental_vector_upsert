"""Deterministic spherical clustering of a copy of the incoming real vectors."""
import numpy as np


def cluster(x, source_rows, seed, k=5, restarts=3, max_iter=50, tol=1e-4):
    x = np.asarray(x)
    source_rows = np.asarray(source_rows, dtype=np.int64)
    if x.ndim != 2 or len(x) < k or len(source_rows) != len(x) or len(np.unique(source_rows)) != len(x):
        raise ValueError("invalid clustering input or source rows")
    if x.dtype != np.float32 or not np.isfinite(x).all():
        raise ValueError("clustering requires finite original float32 vectors")
    norms = np.linalg.norm(x.astype(np.float64), axis=1)
    if np.any(norms == 0) or not np.isfinite(norms).all():
        raise ValueError("zero or nonfinite original norm")
    unit = x.astype(np.float64, copy=True) / norms[:, None]
    best = None
    details = []
    for restart in range(restarts):
        rng = np.random.default_rng(np.random.SeedSequence([int(seed), 193, restart]))
        chosen = [int(rng.integers(len(unit)))]
        for _ in range(1, k):
            similarity = np.max(unit @ unit[chosen].T, axis=1)
            weight = np.maximum(0.0, 1.0 - similarity)
            weight[chosen] = 0
            if weight.sum() <= 0:
                raise ValueError("fewer than five effective centroids")
            chosen.append(int(rng.choice(len(unit), p=weight / weight.sum())))
        centers = unit[chosen].copy()
        previous = None
        for iteration in range(1, max_iter + 1):
            score = unit @ centers.T
            labels = np.argmax(score, axis=1)  # numpy chooses the smaller cluster ID on ties
            sizes = np.bincount(labels, minlength=k)
            if np.any(sizes == 0):
                raise ValueError("empty spherical cluster")
            sums = np.stack([unit[labels == i].sum(axis=0) for i in range(k)])
            lengths = np.linalg.norm(sums, axis=1)
            if np.any(lengths == 0):
                raise ValueError("zero spherical centroid")
            centers = sums / lengths[:, None]
            objective = float(np.sum(np.sum(unit * centers[labels], axis=1)))
            if previous is not None and abs(objective - previous) / max(abs(previous), 1.0) <= tol:
                break
            previous = objective
        # Reassign once after the final center update so labels and centers agree.
        labels = np.argmax(unit @ centers.T, axis=1)
        sizes = np.bincount(labels, minlength=k)
        if np.any(sizes == 0):
            raise ValueError("empty final spherical cluster")
        objective = float(np.sum(np.max(unit @ centers.T, axis=1)))
        details.append({"restart": restart, "iterations": iteration, "objective": objective})
        if best is None or objective > best[0]:
            best = (objective, labels.copy(), centers.copy(), restart)
    _, labels, centers, winning = best
    canonical = sorted(range(k), key=lambda i: int(np.min(source_rows[labels == i])))
    remap = {old: new for new, old in enumerate(canonical)}
    labels = np.asarray([remap[int(v)] for v in labels], dtype=np.int32)
    centers = centers[canonical]
    size = np.bincount(labels, minlength=k)
    if np.any(size == 0) or len(np.unique(centers, axis=0)) != k:
        raise ValueError("empty or duplicate centroid")
    order_rng = np.random.default_rng(np.random.SeedSequence([int(seed), 771]))
    cluster_order = order_rng.permutation(k).astype(np.int32)
    pieces = []
    for cid in cluster_order:
        indices = np.flatnonzero(labels == cid)
        pieces.append(order_rng.permutation(indices))
    clustered_order = np.concatenate(pieces).astype(np.int32)
    random_order = np.random.default_rng(np.random.SeedSequence([int(seed), 772])).permutation(len(x)).astype(np.int32)
    wave_sizes = [int(size[cid]) for cid in cluster_order]
    return {"labels": labels, "centers": centers, "cluster_order": cluster_order,
            "clustered_order": clustered_order, "random_order": random_order,
            "wave_sizes": wave_sizes, "unit": unit,
            "diagnostics": {"algorithm": "spherical-kmeans", "k": k, "restarts": restarts,
                            "max_iter": max_iter, "relative_tolerance": tol, "winning_restart": winning,
                            "objectives": details, "sizes_canonical": size.tolist(),
                            "cluster_order": cluster_order.tolist(), "wave_sizes": wave_sizes,
                            "centroid_cosine": (centers @ centers.T).tolist(),
                            "within_cosine_mean": [float(np.mean(np.sum(unit[labels == i] * centers[i], axis=1))) for i in range(k)],
                            "norm_min": float(norms.min()), "norm_median": float(np.median(norms)),
                            "norm_max": float(norms.max())}}
