"""Versioned ID-to-source-row histories; vector bytes always remain in the public pool."""
from dataclasses import asdict, dataclass
import hashlib
import random

from vectorbench.evidence.contracts import canonical


@dataclass(frozen=True)
class Write:
    id: int
    row: int
    version: int
    category: int = 0
    ordinal: int = 0


@dataclass(frozen=True)
class Event:
    deletes: tuple = ()
    writes: tuple = ()
    operation: str = "insert"

    def json(self):
        return {"operation": self.operation, "deletes": list(self.deletes),
                "writes": [asdict(w) for w in self.writes]}


class Live:
    def __init__(self, initial, pool_size):
        self.pool_size = pool_size
        self.current = {w.id: w for w in initial}
        if len(self.current) != len(initial):
            raise ValueError("duplicate initial ID")
        if any(w.row < 0 or w.row >= pool_size for w in initial):
            raise ValueError("initial source row out of range")

    def apply(self, event):
        if event.operation not in {"insert", "delete", "churn", "reinsert", "upsert", "same_vector", "metadata", "read_only"}:
            raise ValueError("unknown update operation")
        removed, written = list(event.deletes), [w.id for w in event.writes]
        if event.operation == "read_only" and (removed or written):
            raise ValueError("read-only stability control cannot mutate any row")
        if len(set(removed)) != len(removed) or len(set(written)) != len(written):
            raise ValueError("duplicate IDs in one logical operation")
        if set(removed).intersection(written):
            raise ValueError("delete/new-ID churn and same-ID upsert are separate operations")
        if event.operation in {"upsert", "same_vector", "metadata"} and removed:
            raise ValueError("same-ID update cannot contain explicit deletion")
        if event.operation == "delete" and written or event.operation == "insert" and removed:
            raise ValueError("operation payload contradicts its declared semantics")
        result = dict(self.current)
        for identity in removed:
            if identity not in result:
                raise ValueError("deleting a non-live ID")
            del result[identity]
        for w in event.writes:
            if w.row < 0 or w.row >= self.pool_size:
                raise ValueError("replacement source row out of range")
            old = self.current.get(w.id)
            updating = event.operation in {"upsert", "same_vector", "metadata"}
            if updating and old is None:
                raise ValueError("same-ID update targeted a missing ID")
            if not updating and old is not None:
                raise ValueError("new-ID insert targeted an existing ID")
            expected_version = old.version if event.operation == "same_vector" and old is not None else old.version + 1 if old is not None else 0
            if w.version != expected_version:
                raise ValueError("incorrect current version")
            if event.operation in {"same_vector", "metadata"} and w.row != old.row:
                raise ValueError("non-vector control changed its embedding source row")
            result[w.id] = w
        if event.operation == "reinsert" and sorted(self.current[i].row for i in removed) != sorted(w.row for w in event.writes):
            raise ValueError("same-vector new-ID control changed its vector multiset")
        self.current = result  # Only publish after all checks; failed events do not partially apply.
        return self

    def records(self):
        return [self.current[k] for k in sorted(self.current)]

    def digest(self):
        return hashlib.sha256(canonical([asdict(w) for w in self.records()]).encode()).hexdigest()


def initial(rows, categories=None):
    return tuple(Write(i, int(row), 0, int(categories[row]) if categories is not None else 0, int(row))
                 for i, row in enumerate(rows))


def replacement_history(start, replacements, rounds=5, chunk=None, operation="churn",
                        seed=11, repeated=False, hot=False, combine=False, categories=None,
                        target_slots_by_wave=None):
    """Uniform live slots or repeated hot slots; distinct source rows are consumed in order."""
    n = len(start)
    chunk = chunk or n // 10
    if operation not in {"delete", "churn", "reinsert", "upsert", "same_vector", "metadata"}:
        raise ValueError("unsupported replacement-history operation")
    if chunk < 1 or chunk > n or rounds < 1 or (not repeated and rounds * chunk > n):
        raise ValueError("invalid distinct-target update budget")
    if hot and not repeated:
        raise ValueError("hot repeated slots require repeated=True")
    if combine and repeated:
        raise ValueError("cannot disguise repeated same-ID updates as one duplicate-ID batch")
    if operation == "delete" and repeated:
        raise ValueError("delete-only history cannot repeatedly delete the same slots")
    required = chunk * rounds if operation in {"churn", "upsert"} else 0
    replacements = list(map(int, replacements))
    if len(replacements) < required or len(set(replacements[:required])) != required:
        raise ValueError("insufficient never-reused source rows")
    if set(replacements[:required]).intersection(w.row for w in start):
        raise ValueError("replacement pool overlaps initial source rows")
    pool_size = max([w.row for w in start] + replacements + [0]) + 1
    live = Live(start, pool_size)
    rng = random.Random(seed)
    slots = [w.id for w in start]
    ordered_slots = rng.sample(range(n), n)
    if target_slots_by_wave is not None:
        target_slots_by_wave = [list(map(int, wave)) for wave in target_slots_by_wave]
        if len(target_slots_by_wave) != rounds or any(len(w) != chunk or len(set(w)) != chunk
                or any(i < 0 or i >= n for i in w) for w in target_slots_by_wave):
            raise ValueError("explicit target slots must cover every wave exactly once per slot")
        if not repeated and len({i for w in target_slots_by_wave for i in w}) != rounds * chunk:
            raise ValueError("distinct-target history repeats an explicit target slot")
        if hot and any(w != target_slots_by_wave[0] for w in target_slots_by_wave):
            raise ValueError("hot history must preserve the same target slot order")
    events = []
    cursor = 0
    for step in range(rounds):
        targets = (ordered_slots[:chunk] if hot else rng.sample(range(n), chunk)) if repeated else ordered_slots[step * chunk:(step + 1) * chunk]
        if target_slots_by_wave is not None:
            targets = target_slots_by_wave[step]
        deletes, writes = [], []
        for slot in targets:
            previous = live.current[slots[slot]]
            if operation in {"delete", "churn", "reinsert"}:
                deletes.append(previous.id)
            if operation != "delete":
                row = replacements[cursor] if operation in {"churn", "upsert"} else previous.row
                identity = n + cursor if operation in {"churn", "reinsert"} else previous.id
                version = 0 if operation in {"churn", "reinsert"} else previous.version if operation == "same_vector" else previous.version + 1
                category = int(categories[row]) if categories is not None else previous.category
                if operation == "metadata":
                    category = previous.category + 1
                writes.append(Write(identity, row, version, category, row))
                slots[slot] = identity
                cursor += 1
        event = Event(tuple(deletes), tuple(writes), operation)
        live.apply(event)
        events.append(event)
    if combine:
        combined = Event(tuple(i for e in events for i in e.deletes),
                         tuple(w for e in events for w in e.writes), operation)
        other = Live(start, pool_size).apply(combined)
        if other.digest() != live.digest():
            raise AssertionError("combined and split paths do not reach exactly the same state")
        return [combined]
    return events
