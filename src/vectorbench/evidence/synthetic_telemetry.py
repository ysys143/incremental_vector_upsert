"""Version-bound Qdrant search-mode verification from complete segment telemetry."""
import hashlib

from vectorbench.experiments.synthetic_insertion.contracts import canonical

COUNTERS = ("unfiltered_hnsw", "unfiltered_plain", "unfiltered_exact", "filtered_plain",
            "filtered_exact", "filtered_small_cardinality", "filtered_large_cardinality",
            "unfiltered_sparse", "filtered_sparse")


class UnverifiedTelemetry(AssertionError):
    pass


def segments(telemetry, collection="items", require_ann=True):
    try:
        result = telemetry["result"]
        if result["app"]["version"] != "1.17.1":
            raise UnverifiedTelemetry("counter serialization is only validated for pinned Qdrant 1.17.1")
        matches = [c for c in result["collections"]["collections"] if c["id"] == collection]
        if len(matches) != 1:
            raise UnverifiedTelemetry("collection telemetry missing or ambiguous")
        records = {}
        for shard in matches[0]["shards"]:
            for s in shard["local"]["segments"]:
                info = s["info"]
                uid = info["uuid"]
                if uid in records:
                    raise UnverifiedTelemetry("duplicate segment UUID")
                indexes = s["vector_index_searches"]
                if len(indexes) != 1 or indexes[0]["index_name"] != "":
                    raise UnverifiedTelemetry("expected exactly one named dense index telemetry object")
                index = s["config"]["vector_data"][""]["index"]
                count, indexed = info["num_vectors"], info["num_indexed_vectors"]
                if require_ann and count and (index["type"] != "hnsw" or indexed != count):
                    raise UnverifiedTelemetry("nonempty segment is not fully HNSW indexed")
                stable = {"uuid": uid, "count": count, "indexed": indexed,
                          "deleted": info["num_deleted_vectors"], "index": index}
                counters = {}
                for name in COUNTERS:
                    # v1.17.1's skip_serializing_if=OperationDurationStatistics::is_empty
                    # omits zero-operation members. Complete outer objects remain mandatory.
                    stats = indexes[0].get(name)
                    if stats is None:
                        counters[name] = {"count": 0, "fail_count": 0, "zero_omitted": True}
                    else:
                        counters[name] = {"count": stats["count"], "fail_count": stats.get("fail_count", 0),
                                          "zero_omitted": False}
                records[uid] = {"stable": stable, "counters": counters}
        if not records or not any(s["stable"]["count"] for s in records.values()):
            raise UnverifiedTelemetry("nonempty segment telemetry missing")
        return records
    except (KeyError, TypeError, IndexError) as exc:
        raise UnverifiedTelemetry("incomplete segment telemetry; not zero counters") from exc


def verify_delta(before, after, query_count, collection="items"):
    a, b = segments(before, collection), segments(after, collection)
    if set(a) != set(b):
        raise UnverifiedTelemetry("segment set changed during measured reads")
    delta = {name: 0 for name in COUNTERS}
    for uid in a:
        if a[uid]["stable"] != b[uid]["stable"]:
            raise UnverifiedTelemetry("segment/index changed during measured reads")
        for name in COUNTERS:
            ca, cb = a[uid]["counters"][name], b[uid]["counters"][name]
            change = cb["count"] - ca["count"]
            if change < 0 or cb["fail_count"] != ca["fail_count"]:
                raise UnverifiedTelemetry("counter reset or failed search")
            if a[uid]["stable"]["count"]:
                delta[name] += change
    if delta["unfiltered_hnsw"] < query_count:
        raise UnverifiedTelemetry("HNSW operation delta is below measured query count")
    if any(value for name, value in delta.items() if name != "unfiltered_hnsw"):
        raise UnverifiedTelemetry("plain/exact/non-HNSW fallback occurred")
    return {"status": "PASS", "query_count": query_count, "delta": delta,
            "segments": len(a), "fingerprint": hashlib.sha256(canonical(
                {uid: v["stable"] for uid, v in a.items()}).encode()).hexdigest(),
            "schema": "qdrant-v1.17.1-empty-omitted; details_level=4"}
