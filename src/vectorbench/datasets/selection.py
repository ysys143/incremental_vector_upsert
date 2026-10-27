"""Content-based disjoint public-vector selection; no new embeddings or transformations."""
import hashlib
import numpy as np


def unique_rows(array):
    identities, first, duplicate_rows = [], {}, 0
    for row, vector in enumerate(array):
        key = hashlib.sha256(np.asarray(vector, dtype="<f4").tobytes()).digest()
        if key in first:
            duplicate_rows += 1
        else:
            first[key] = row
            identities.append(row)
    return np.asarray(identities, dtype=np.int64), first, duplicate_rows


def entry_split(pool, n, replacement_count, calibration_queries, evaluation_queries):
    documents, document_hashes, duplicate_documents = unique_rows(pool.vectors)
    queries, query_hashes, duplicate_queries = unique_rows(pool.queries)
    overlap = set(document_hashes) & set(query_hashes)
    excluded_query_rows = {query_hashes[key] for key in overlap}
    eligible = np.asarray([row for row in queries if int(row) not in excluded_query_rows], dtype=np.int64)
    required_documents = 2*n + replacement_count
    if len(documents) < required_documents:
        raise AssertionError("insufficient unique real documents for disjoint calibration/evaluation histories")
    if len(eligible) < calibration_queries + evaluation_queries:
        raise AssertionError("insufficient unique document-disjoint real queries")
    rows = np.random.default_rng(11).permutation(documents)
    query_rows = np.random.default_rng(20261025).permutation(eligible)
    available = rows[n+replacement_count:]
    calibration_rows = np.random.default_rng(7).choice(available, n, replace=False)
    return {"initial": rows[:n], "replacement": rows[n:n+replacement_count],
            "calibration_documents": calibration_rows, "calibration_queries": query_rows[:calibration_queries],
            "evaluation_queries": query_rows[calibration_queries:calibration_queries+evaluation_queries],
            "diagnostics": {"duplicate_document_rows": duplicate_documents, "duplicate_query_rows": duplicate_queries,
                            "query_values_present_in_document_pool": len(overlap),
                            "eligible_unique_documents": len(documents), "eligible_unique_queries": len(eligible),
                            "method": "SHA256 of original little-endian float32 bytes; no normalization",
                            "scope": "exact byte duplicates only; near-duplicate or semantic leakage not established"}}
