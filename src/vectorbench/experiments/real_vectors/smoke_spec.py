"""Dependency-free entry parameters shared by native orchestration and NumPy workers."""
from dataclasses import replace
from vectorbench.evidence.contracts import Config


OPERATIONS = ("insert", "delete", "churn", "upsert", "reinsert", "same_vector", "metadata")


def config(large_requests=False):
    # Two rounds qualify mechanisms only. Main five/fifty-round histories are unchanged.
    return replace(Config(), n=10000 if large_requests else 1000, query_rows=60, seeds=(11,))
