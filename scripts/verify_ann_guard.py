"""Exercise fail-closed search routing against an owned real PostgreSQL server."""
import datetime
import json
from pathlib import Path
import traceback

import numpy as np

from vectorbench.experiments.pilot.model import Config, fixture, validate_snapshot
from vectorbench.experiments.pilot.run import IMAGES, Server, inventory, source_hashes, write_json


def verify():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = Path("runs") / (stamp + "-ann-guard")
    root.mkdir(exist_ok=False)
    before = inventory()
    write_json(root / "inventory-before.json", before)
    receipt = {"status": "running", "sources": source_hashes(), "images": IMAGES, "probes": []}
    server = Server("postgres", root)
    trace = []

    def traced(adapter):
        original = adapter.sql

        def call(sql):
            result = original(sql)
            trace.append({"sql": sql, "response": result})
            return result

        adapter.sql = call

    def expect_blocked(adapter, vector, name):
        start = len(trace)
        try:
            adapter.query(vector)
        except AssertionError as exc:
            statements = trace[start:]
            if any(s["sql"].startswith("SELECT coalesce(json_agg") for s in statements):
                raise AssertionError("fallback search was executed before rejection")
            receipt["probes"].append({"name": name, "blocked_before_search": True,
                                      "error": str(exc), "statements": statements})
            return
        raise AssertionError("invalid search was not blocked: " + name)

    try:
        adapter = server.start()
        config = Config.smoke()
        pool, queries, _ = fixture(config, 11)
        ids = np.arange(config.n)
        adapter.setup(ids, pool[ids], (16, 100))
        adapter.set_ef(128)
        traced(adapter)
        positive = adapter.path(queries[0])
        rows = adapter.query(queries[0])
        assert len(rows) == 100
        receipt["probes"].append({"name": "hnsw_positive", "state": adapter.state(), "plan": positive})

        adapter.sql("SET enable_seqscan=on;")
        expect_blocked(adapter, queries[0], "seqscan_setting_on")

        adapter.sql("SET enable_seqscan=off; DROP INDEX items_hnsw;")
        expect_blocked(adapter, queries[0], "index_missing_despite_seqscan_off")

        # Restore only this test's index, then verify reconnect applies the search settings.
        adapter.sql("CREATE INDEX items_hnsw ON items USING hnsw (embedding vector_l2_ops);")
        adapter = server.restart()
        traced(adapter)
        adapter.set_ef(128)
        validate_snapshot(ids, pool[ids], *adapter.snapshot())
        receipt["probes"].append({"name": "restart_reapplies_off", "state": adapter.state(),
                                  "plan": adapter.path(queries[0])})
        assert len(adapter.query(queries[0])) == 100
        receipt["status"] = "passed"
    except BaseException as exc:
        receipt["status"] = "failed"
        receipt["first_error"] = repr(exc)
        (root / "failure.txt").write_text(traceback.format_exc())
    finally:
        errors = server.cleanup()
        receipt["cleanup_errors"] = errors
        after = inventory()
        write_json(root / "inventory-after.json", after)
        receipt["existing_containers_preserved"] = before["containers"] == after["containers"]
        receipt["existing_volumes_preserved"] = before["volumes"] == after["volumes"]
        if errors or not receipt["existing_containers_preserved"] or not receipt["existing_volumes_preserved"]:
            receipt["status"] = "failed"
        write_json(root / "receipt.json", receipt)
        write_json(root / "sql-trace.json", trace)
    print(json.dumps({"root": str(root), "status": receipt["status"], "probes": len(receipt["probes"])}))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(verify())
