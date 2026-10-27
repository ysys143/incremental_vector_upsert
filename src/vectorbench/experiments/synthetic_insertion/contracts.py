"""Pure JSON contracts usable by the Python 3.13 Dagster controller."""
import hashlib
import json
from pathlib import Path

GEOMETRIES = ("iid", "near", "far", "bridge")
DATABASES = ("postgres", "qdrant")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".new")
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temp.replace(path)


def load(path):
    return json.loads(Path(path).read_text())


def config(smoke=False):
    return {"n": 1000 if smoke else 10000, "dim": 32 if smoke else 128,
            "queries_per_cohort": 10 if smoke else 200,
            "seeds": [11] if smoke else [11, 29, 47],
            "ratios": [100] if smoke else [10, 100], "smoke": smoke,
            "calibration_seed": 7, "initial_batch_candidates": [100, 1000],
            "builds": [[16, 100], [32, 200]], "search_ef": [100, 128, 256],
            "conditional_search_ef": 512, "quality_target": .95,
            "stage_seconds": 14400, "free_floor_bytes": 25 * 1024**3,
            "space_cap_bytes": 8 * 1024**3, "counter_schema": "qdrant-v1.17.1-empty-omitted"}


def case_specs(c):
    cases = []
    for db in DATABASES:
        for seed in c["seeds"]:
            for geo in GEOMETRIES:
                for ratio in c["ratios"]:
                    for waves in (1, 5):
                        stage = "A" if geo == "iid" else "B"
                        key = f"{db}-s{seed}-{geo}-r{ratio}-w{waves}-random-settled"
                        cases.append({"id": key, "kind": "incremental", "stage": stage,
                                      "db": db, "seed": seed, "geometry": geo, "ratio": ratio,
                                      "waves": waves, "order": "random", "policy": "settled"})
            if not c["smoke"]:
                for geo in ("far", "bridge"):
                    for waves in (1, 5):
                        key = f"{db}-s{seed}-{geo}-r100-w{waves}-ordered-settled"
                        cases.append({"id": key, "kind": "incremental", "stage": "C", "db": db,
                                      "seed": seed, "geometry": geo, "ratio": 100, "waves": waves,
                                      "order": "ordered", "policy": "settled"})
                for geo in ("iid", "far"):
                    key = f"{db}-s{seed}-{geo}-r100-w5-random-continuous"
                    cases.append({"id": key, "kind": "incremental", "stage": "D", "db": db,
                                  "seed": seed, "geometry": geo, "ratio": 100, "waves": 5,
                                  "order": "random", "policy": "continuous"})
    return cases


def fixture_key(seed, geometry):
    return f"s{seed}-{geometry}"


def base_key(db, seed):
    return f"{db}-s{seed}"


def verify_files(root, receipt):
    if receipt.get("status") != "PASS":
        raise AssertionError("receipt is not PASS")
    for relative, expected in receipt["artifacts"].items():
        path = Path(root) / relative
        if not path.is_file() or sha(path) != expected:
            raise AssertionError("missing or changed artifact: " + relative)


def expected_counts(c):
    cases = case_specs(c)
    incremental = sum(2 if s["policy"] == "continuous" else s["waves"] + 1 for s in cases)
    return {"paths": len(cases), "incremental_checkpoints": incremental}


def read_ack(root, key):
    root = Path(root)
    entry = load(root / "ledger.json")[key]
    if entry["status"] != "PASS":
        raise AssertionError("dependency not acknowledged: " + key)
    ack = load(root / entry["ack"])
    path = root / ack["receipt"]
    if sha(path) != ack["receipt_sha256"]:
        raise AssertionError("acknowledged receipt changed: " + key)
    receipt = load(path)
    if receipt["campaign"] != root.name or receipt["key"] != key or ack["run_id"] != entry["run_id"]:
        raise AssertionError("receipt scope/run identity mismatch")
    verify_files(root, receipt)
    return receipt, ack


def source_revision(sources):
    return hashlib.sha256(canonical(sources).encode()).hexdigest()


def source_amendment(root):
    root = Path(root)
    pointer = root / "source-amendment-pointer.json"
    if not pointer.exists():
        return None
    ref = load(pointer)
    path = root / ref["path"]
    if sha(path) != ref["sha256"]:
        raise AssertionError("source amendment record changed")
    amendment = load(path)
    if amendment["manifest_sha256"] != sha(root / "manifest.json"):
        raise AssertionError("source amendment belongs to another immutable manifest")
    for name, expected in amendment["sources"].items():
        if sha(path.parent / "source" / name) != expected:
            raise AssertionError("amended source snapshot changed: " + name)
    return amendment


def effective_sources(root, manifest):
    amendment = source_amendment(root)
    return amendment["sources"] if amendment is not None else manifest["sources"]
