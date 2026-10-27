"""Compact immutable event/state references; never copy embeddings per checkpoint."""
from pathlib import Path
import numpy as np

from vectorbench.evidence.contracts import save, sha
from vectorbench.workloads.events import Event, Live, Write


def int32(values):
    array = np.asarray(values, dtype=np.int64)
    if array.size and (array.min() < np.iinfo(np.int32).min or array.max() > np.iinfo(np.int32).max):
        raise ValueError("lossless event encoding exceeds int32")
    return array.astype(np.int32)


def save_state(path, live):
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    records = live.records()
    rows = int32([[w.id, w.row, w.version, w.category, w.ordinal] for w in records]).reshape(-1, 5)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, records=rows)
    result = {"sha256": sha(path), "ledger_sha256": live.digest(), "count": len(records)}
    save(path.with_suffix(path.suffix + ".receipt.json"), result)
    return result


def load_state(path, pool_size):
    with np.load(path) as data:
        rows = data["records"]
    if rows.ndim != 2 or rows.shape[1] != 5 or rows.dtype.kind not in "iu":
        raise ValueError("malformed state record shape")
    records = [Write(*map(int, row)) for row in rows]
    if any(w.id < 0 or w.version < 0 for w in records):
        raise ValueError("negative current ID/version")
    return Live(records, pool_size)


def save_events(path, events):
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    deletes, writes, delete_offsets, write_offsets = [], [], [0], [0]
    kinds = []
    for event in events:
        deletes.extend(event.deletes)
        writes.extend([w.id, w.row, w.version, w.category, w.ordinal] for w in event.writes)
        delete_offsets.append(len(deletes)); write_offsets.append(len(writes)); kinds.append(event.operation)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, deletes=int32(deletes), writes=int32(writes).reshape(-1, 5),
                        delete_offsets=int32(delete_offsets), write_offsets=int32(write_offsets), kinds=np.array(kinds))
    result = {"sha256": sha(path), "events": len(events), "delete_rows": len(deletes), "write_rows": len(writes)}
    save(path.with_suffix(path.suffix + ".receipt.json"), result)
    return result


def load_events(path):
    with np.load(path) as data:
        deleted, written = data["deletes"], data["writes"]
        d, w, kinds = data["delete_offsets"], data["write_offsets"], data["kinds"]
    if len(d) != len(kinds)+1 or len(w) != len(kinds)+1 or d[0] != 0 or w[0] != 0:
        raise ValueError("malformed event offsets")
    if np.any(np.diff(d) < 0) or np.any(np.diff(w) < 0) or d[-1] != len(deleted) or w[-1] != len(written):
        raise ValueError("event offsets do not cover the original rows")
    if written.ndim != 2 or written.shape[1] != 5:
        raise ValueError("malformed write record shape")
    return [Event(tuple(map(int, deleted[d[i]:d[i+1]])),
                  tuple(Write(*map(int, row)) for row in written[w[i]:w[i+1]]), str(kinds[i]))
            for i in range(len(kinds))]
