"""Immutable real-data configuration and cell-local atomic evidence."""
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid


GIB = 1024 ** 3


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    """Unique temporary files; callers must still own the destination exclusively."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


@dataclass(frozen=True)
class Config:
    n: int = 10000
    dimension: int = 768
    pool_rows: int = 100000
    query_rows: int = 600
    calibration_queries: int = 200
    seeds: tuple = (11, 29, 47)
    calibration_seed: int = 7
    short_rounds: int = 5
    long_rounds: int = 50
    update_fraction: float = .1
    source_blocks: int = 32
    cpu_workers: int = 2
    db_workers: int = 1
    db_cpus: int = 2
    db_memory_gib: int = 4
    # User resumed on 2026-10-26 with "proceed anyway" after the 25 GiB gate.
    free_floor_bytes: int = 10 * GIB
    growth_cap_bytes: int = 8 * GIB
    max_payload_bytes: int = 256 * 1024 ** 2
    stage_seconds: int = 14400

    def validate(self):
        if self.dimension != 768 or self.db_workers != 1 or self.cpu_workers not in (1, 2):
            raise ValueError("real vectors keep their original 768 dimensions and DB concurrency is one")
        chunk = int(self.n * self.update_fraction)
        if self.n < 100 or chunk < 1 or self.pool_rows < self.n + chunk * self.long_rounds:
            raise ValueError("pool cannot supply initial and never-reused long-update vectors")
        if self.calibration_seed in self.seeds or len(set(self.seeds)) != len(self.seeds):
            raise ValueError("calibration and evaluation seeds must be separate")
        return asdict(self)


class ResourceGate(RuntimeError):
    """A blocked admission is not a successful or failed scientific measurement."""


def check_space(path, config=None, baseline_free=None, reserve=0):
    """Observe space only; the user removed all capacity and growth vetoes."""
    c = config or Config()
    c.validate()
    if reserve < 0:
        raise ValueError("negative space reservation")
    free = shutil.disk_usage(Path(path)).free
    budget_root = os.environ.get("REAL_STUDY_BUDGET_ROOT")
    if baseline_free is None and budget_root:
        from vectorbench.evidence.registry import Registry
        baseline_free = Registry(Path(budget_root) / "admission.sqlite").baseline_free()
    growth = 0 if baseline_free is None else max(0, baseline_free - free)
    return {"free_bytes": free, "growth_bytes": growth, "reserve_bytes": reserve,
            "disk_policy": "OBSERVE_ONLY"}


def evidence_key(dataset_sha, state_sha, queries_sha, metric, predicate=None):
    return hashlib.sha256(canonical({"dataset": dataset_sha, "state": state_sha,
        "queries": queries_sha, "metric": metric, "predicate": predicate}).encode()).hexdigest()


def owned_output_bytes(directory):
    """Conservative owned file allocation; hard links count once, symlinks are not followed."""
    import stat
    total=0; seen=set(); pending=[Path(directory)]
    while pending:
        parent=pending.pop()
        for entry in os.scandir(parent):
            try: info=entry.stat(follow_symlinks=False)
            except FileNotFoundError: continue
            if stat.S_ISDIR(info.st_mode): pending.append(Path(entry.path))
            elif stat.S_ISREG(info.st_mode):
                key=(info.st_dev,info.st_ino)
                if key not in seen:
                    seen.add(key); total+=max(info.st_size,info.st_blocks*512)
    return total
