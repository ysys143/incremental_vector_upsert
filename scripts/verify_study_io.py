"""Real ARM64 DB snapshot and Qdrant search-counter acceptance probes."""
import datetime
from pathlib import Path
import json

import numpy as np

from vectorbench.experiments.pilot.model import Config, fixture, validate_snapshot
from vectorbench.experiments.synthetic_insertion.contracts import save, sha
from vectorbench.evaluation.synthetic_measure import telemetry
from vectorbench.orchestration.synthetic_resources import OwnedServer
from vectorbench.evidence.synthetic_telemetry import segments, verify_delta, UnverifiedTelemetry


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = Path("runs") / (stamp + "-study-io-checks")
    root.mkdir()
    c = Config.smoke()
    pool, queries, _ = fixture(c, 11)
    ids = np.arange(c.n)
    results = {}
    for db in ("postgres", "qdrant"):
        initial = root / db / "initial"
        initial.mkdir(parents=True)
        source = OwnedServer(db, initial)
        try:
            a = source.start()
            a.setup(ids, pool[ids], (16, 100))
            validate_snapshot(ids, pool[ids], *a.snapshot())
            snapshot = source.export(initial / "initial.tgz")
            save(initial / "snapshot.json", snapshot)
        finally:
            errors = source.cleanup()
            if errors:
                raise AssertionError(errors)
        clone_dir = root / db / "clone"
        clone_dir.mkdir()
        clone = OwnedServer(db, clone_dir, archive=initial / "initial.tgz",
                            archive_sha=snapshot["archive_sha"], files_hash=snapshot["files_hash"],
                            segment_signature=snapshot.get("segment_signature"), graph_hash=snapshot.get("graph_hash"))
        try:
            a = clone.start()
            a.set_ef(128)
            validate_snapshot(ids, pool[ids], *a.snapshot())
            state = a.state()
            if db == "postgres":
                if any(state[k] != snapshot["initial_state"][k] for k in ("table_oid", "index_oid")):
                    raise AssertionError("physical clone did not preserve initial index identity")
                a.path(queries[0])
                assert state["enable_seqscan"] == "off"
                assert len(a.query(queries[0])) == 100
                results[db] = {"snapshot_restored": True, "oid_preserved": True, "seqscan": "off"}
            else:
                before = telemetry(a)
                a.query(queries[0])
                a.query(queries[1])
                after = telemetry(a)
                save(clone_dir / "hnsw-before.json", before)
                save(clone_dir / "hnsw-after.json", after)
                normal = verify_delta(before, after, 2)
                a.request("POST", "/collections/items/points/query", {
                    "query": queries[0].tolist(), "limit": 100, "params": {"exact": True}})
                exact = telemetry(a)
                save(clone_dir / "exact-after.json", exact)
                try:
                    verify_delta(after, exact, 1)
                except UnverifiedTelemetry:
                    exact_rejected = True
                else:
                    raise AssertionError("exact positive control was admitted as ANN")
                # This is only a positive control on the disposable clone, after ANN acceptance.
                a.request("PATCH", "/collections/items", {"hnsw_config": {"full_scan_threshold": 10000000}})
                a.wait()
                plain_before = telemetry(a)
                a.query(queries[0])
                plain_after = telemetry(a)
                save(clone_dir / "plain-before.json", plain_before)
                save(clone_dir / "plain-after.json", plain_after)
                x, y = segments(plain_before), segments(plain_after)
                delta = sum(y[u]["counters"]["unfiltered_plain"]["count"] - x[u]["counters"]["unfiltered_plain"]["count"]
                            for u in x if x[u]["stable"]["count"])
                if delta != 1:
                    raise AssertionError("plain positive-control counter did not increment")
                try:
                    verify_delta(plain_before, plain_after, 1)
                except UnverifiedTelemetry:
                    plain_rejected = True
                else:
                    raise AssertionError("plain positive control was admitted as ANN")
                results[db] = {"snapshot_restored": True, "ann": normal, "exact_rejected": exact_rejected,
                               "plain_counter_delta": delta, "plain_rejected": plain_rejected,
                               "version_bound_empty_counter_omission_confirmed": True}
        finally:
            errors = clone.cleanup()
            if errors:
                raise AssertionError(errors)
        corrupt_dir = root / db / "corrupt"
        corrupt_dir.mkdir()
        corrupted = corrupt_dir / "corrupted.tgz"
        corrupted.write_bytes((initial / "initial.tgz").read_bytes() + b"intentional fault")
        bad = OwnedServer(db, corrupt_dir, archive=corrupted,
                          archive_sha=snapshot["archive_sha"], files_hash=snapshot["files_hash"])
        try:
            try:
                bad.start()
            except AssertionError:
                results[db]["corrupt_archive_rejected_before_start"] = True
            else:
                raise AssertionError("corrupted base snapshot was accepted")
        finally:
            errors = bad.cleanup()
            if errors:
                raise AssertionError(errors)
        if sha(initial / "initial.tgz") != snapshot["archive_sha"]:
            raise AssertionError("base archive was modified")
    save(root / "receipt.json", {"status": "PASS", "probes": results})
    print(json.dumps({"root": str(root), "status": "PASS", "probes": results}))


if __name__ == "__main__":
    main()
