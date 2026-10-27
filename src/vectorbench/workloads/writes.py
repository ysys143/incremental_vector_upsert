"""One logical wave, including true payload-only updates rather than vector rewrites."""
import json
import time

import numpy as np

from vectorbench.backends.adapters import validate_rows
from vectorbench.evidence.contracts import Config, save


def metadata_only(adapter, ids, attributes):
    ids = list(map(int, ids)); a = np.asarray(attributes, dtype=np.int64)
    if len(ids) != len(set(ids)) or a.shape != (len(ids), 3):
        raise ValueError("metadata-only batch has invalid IDs or fields")
    if adapter.name == "postgres":
        rows = "\n".join(f"{i}\t{m[0]}\t{m[1]}\t{m[2]}" for i, m in zip(ids, a))
        adapter.sql("BEGIN; CREATE TEMP TABLE ivu_metadata (id bigint PRIMARY KEY,category bigint,ordinal bigint,version bigint) ON COMMIT DROP;")
        try:
            adapter.sql("COPY ivu_metadata (id,category,ordinal,version) FROM STDIN;\n" + rows + "\n\\.")
            adapter.sql(f"UPDATE {adapter.table} AS target SET category=s.category,ordinal=s.ordinal,version=s.version FROM ivu_metadata s WHERE target.id=s.id; COMMIT;")
        except BaseException:
            if adapter.process.poll() is None:
                try:
                    adapter.sql("ROLLBACK;")
                except Exception:
                    pass
            raise
    else:
        operations = [{"set_payload": {"payload": {"category": int(m[0]), "ordinal": int(m[1]), "version": int(m[2])},
                                        "points": [i]}} for i, m in zip(ids, a)]
        adapter.request("POST", f"/collections/{adapter.table}/points/batch?wait=true", {"operations": operations})


def wave(adapter, event, pool, out, number, config=None):
    """Intent first, no retries, exact wave size. A churn wave has delete then insert."""
    c = config or Config()
    ids = [w.id for w in event.writes]
    rows = [w.row for w in event.writes]
    attributes = np.asarray([[w.category, w.ordinal, w.version] for w in event.writes], dtype=np.int64).reshape(-1, 3)
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate ID in a batch cannot represent repeated same-ID updates")
    intent = {"status": "INTENT", "operation": event.operation, "deleted": len(event.deletes),
              "written": len(ids), "requests": [], "started_at": time.time()}
    path = out / f"write-{number}.json"
    save(path, intent)
    original = adapter.sql if adapter.name == "postgres" else adapter.request

    def sql(statement):
        record = None
        if statement.startswith("COPY "):
            record = {"kind": "COPY", "bytes": len(statement.encode()), "rows": len(statement.splitlines())-2}
        elif statement.startswith("DELETE "):
            record = {"kind": "DELETE", "bytes": len(statement.encode())}
        if record is not None:
            record.update(status="INTENT", started_at=time.time())
            intent["requests"].append(record)
            save(path, intent)
        result = original(statement)
        if record is not None:
            record.update(status="ACKNOWLEDGED", finished_at=time.time(),
                          scope="statement; stage COPY is not transaction commit")
            save(path, intent)
        if "COMMIT;" in statement:
            intent.setdefault("commits", []).append({"status":"ACKNOWLEDGED", "finished_at":time.time()})
            save(path, intent)
        return result

    def request(method, url, body=None):
        if method in {"PUT", "POST"} and "/points" in url:
            encoded = json.dumps(body, separators=(",", ":")).encode()
            if len(encoded) > c.max_payload_bytes:
                raise ValueError("actual wave exceeds its frozen request-size limit")
            record = {"kind": method, "url": url, "bytes": len(encoded), "status":"INTENT", "started_at":time.time(),
                      "rows": len(body.get("batch", {}).get("ids", body.get("points", body.get("operations", []))))}
            intent["requests"].append(record)
            save(path, intent)
            sender = getattr(adapter, "request_encoded", None)
            result = sender(method, url, encoded) if sender is not None else original(method, url, body)
            record.update(status="ACKNOWLEDGED", finished_at=time.time())
            save(path, intent)
            return result
        return original(method, url, body)

    if adapter.name == "postgres":
        adapter.sql = sql
    else:
        adapter.request = request
    started = time.perf_counter()
    try:
        if event.deletes:
            adapter.delete(list(event.deletes))
        if ids:
            adapter.batch = len(ids)
            if event.operation == "metadata":
                metadata_only(adapter, ids, attributes)
            elif event.operation in {"upsert", "same_vector"}:
                adapter.upsert(ids, pool[rows], attributes)
            else:
                adapter.insert(ids, pool[rows], attributes)
        expected = bool(event.deletes) + bool(ids)
        if len(intent["requests"]) != expected:
            raise AssertionError("wave was silently chunked or a declared write was missing")
        intent.update(status="NO_WRITE_CONTROL" if event.operation == "read_only" else "ACKNOWLEDGED",
                      acknowledgement_s=time.perf_counter()-started, finished_at=time.time())
        save(path, intent)
    except BaseException as exc:
        intent.update(status="OUTCOME_REQUIRES_RECONCILIATION", error=repr(exc), elapsed_s=time.perf_counter()-started)
        save(path, intent)
        raise
    finally:
        if adapter.name == "postgres":
            adapter.sql = original
        else:
            adapter.request = original
    return intent
