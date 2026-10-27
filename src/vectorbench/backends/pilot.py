"""Two explicit server adapters. No automatic retry of writes."""
import http.client
import json
import subprocess
import time

import numpy as np


def vector_text(vector):
    return "[" + ",".join(format(float(v), ".9g") for v in vector) + "]"


def assert_postgres_ann(plan, seqscan, index_name, analyzed=False):
    """Reject fallback plans; enable_seqscan=off alone is only a planner preference."""
    if seqscan != "off":
        raise AssertionError("enable_seqscan must be off in the search session")
    if not isinstance(plan, list) or len(plan) != 1 or "Plan" not in plan[0]:
        raise AssertionError("missing PostgreSQL search plan")
    pending = [plan[0]["Plan"]]
    indexes = []
    while pending:
        node = pending.pop()
        kind = node.get("Node Type")
        if kind not in {"Aggregate", "Limit", "Index Scan"}:
            raise AssertionError(f"non-HNSW search node forbidden: {kind}")
        if kind == "Index Scan":
            if node.get("Index Name") != index_name or "<->" not in node.get("Order By", ""):
                raise AssertionError("search must use the expected ordered HNSW index")
            if analyzed and node.get("Actual Loops", 0) < 1:
                raise AssertionError("HNSW index was not actually executed")
            indexes.append(node)
        pending.extend(node.get("Plans", []))
    if len(indexes) != 1:
        raise AssertionError("exactly one HNSW scan is required")


class Postgres:
    name = "postgres"

    def __init__(self, container, root):
        self.container = container
        self.stderr = open(root / "psql-stderr.log", "a")
        self.process = subprocess.Popen(
            ["docker", "exec", "-i", container, "psql", "-U", "postgres", "-X", "-qAt",
             "-v", "ON_ERROR_STOP=1", "-P", "pager=off"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            text=True, bufsize=1)
        self.sql("CREATE EXTENSION IF NOT EXISTS vector; SET enable_seqscan=off; SET enable_indexscan=on; SET hnsw.iterative_scan=strict_order; SET statement_timeout='120s';")
        self.batch = 1000
        self.ef = 128
        self.table = "items"

    def sql(self, sql):
        self.process.stdin.write(sql + "\n\\echo __PILOT_END__\n")
        self.process.stdin.flush()
        lines = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError("psql stopped; see psql-stderr.log")
            if line.strip() == "__PILOT_END__":
                return "\n".join(lines)
            lines.append(line.rstrip())

    def setup(self, ids, vectors, build, table="items"):
        if table not in {"items", "control"}:
            raise ValueError(table)
        self.table = table
        self.sql(f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id bigint PRIMARY KEY, embedding vector({vectors.shape[1]}));")
        t = time.perf_counter()
        self.insert(ids, vectors)
        load = time.perf_counter() - t
        t = time.perf_counter()
        self.sql(f"CREATE INDEX {table}_hnsw ON {table} USING hnsw (embedding vector_l2_ops) WITH (m={build[0]}, ef_construction={build[1]}); ANALYZE {table};")
        return {"load_s": load, "build_s": time.perf_counter() - t}

    def insert(self, ids, vectors):
        for start in range(0, len(ids), self.batch):
            rows = "\n".join(f"{int(i)}\t{vector_text(v)}" for i, v in
                             zip(ids[start:start + self.batch], vectors[start:start + self.batch]))
            self.sql(f"COPY {self.table} (id,embedding) FROM STDIN;\n{rows}\n\\.")

    def delete(self, ids):
        if ids:
            self.sql(f"DELETE FROM {self.table} WHERE id IN ({','.join(map(str, ids))});")

    def set_ef(self, value):
        self.ef = value
        self.sql(f"SET hnsw.ef_search={value};")

    def search_sql(self, vector, k=100):
        literal = vector_text(vector)
        return f"SELECT coalesce(json_agg(id),'[]') FROM (SELECT id FROM {self.table} ORDER BY embedding <-> '{literal}'::vector LIMIT {k}) q;"

    def checked_search_sql(self, vector, k=100):
        statement = self.search_sql(vector, k)
        raw = self.sql("SHOW enable_seqscan;\nEXPLAIN (FORMAT JSON) " + statement)
        seqscan, plan = raw.split("\n", 1)
        assert_postgres_ann(json.loads(plan), seqscan, f"{self.table}_hnsw")
        return statement

    def query(self, vector, k=100):
        # Same private connection and no concurrent DDL: reject fallback before executing search.
        statement = self.checked_search_sql(vector, k)
        return json.loads(self.sql(statement))

    def snapshot(self):
        rows = self.sql(f"SELECT id,embedding::text FROM {self.table} ORDER BY id;")
        ids, vectors = [], []
        for row in rows.splitlines():
            i, v = row.split("|", 1)
            ids.append(int(i))
            vectors.append(json.loads(v))
        return ids, vectors

    def state(self):
        raw = self.sql(f"SELECT json_build_object('table_oid','{self.table}'::regclass::oid,'index_oid','{self.table}_hnsw'::regclass::oid,'rows',(SELECT count(*) FROM {self.table}),'bytes',pg_total_relation_size('{self.table}'),'dead_tuples',(SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='{self.table}'),'version',version(),'vector_version',(SELECT extversion FROM pg_extension WHERE extname='vector'),'ef_search',current_setting('hnsw.ef_search'),'iterative_scan',current_setting('hnsw.iterative_scan'),'enable_seqscan',current_setting('enable_seqscan')); ")
        return json.loads(raw)

    def path(self, query):
        statement = self.checked_search_sql(query)
        plan = self.sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement)
        parsed = json.loads(plan)
        assert_postgres_ann(parsed, "off", f"{self.table}_hnsw", analyzed=True)
        return parsed

    def wait(self, timeout=120):
        return {"settled": True, "wait_s": 0, "state": self.state(),
                "meaning": "committed writes; no claim that VACUUM completed"}

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write("\\q\n")
            self.process.stdin.flush()
            self.process.wait(timeout=10)
        self.stderr.close()


class Qdrant:
    name = "qdrant"

    def __init__(self, port, root=None):
        self.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
        self.batch = 1000
        self.ef = 128
        self.table = "items"

    def request(self, method, path, body=None):
        encoded = json.dumps(body, separators=(",", ":")) if body is not None else None
        self.connection.request(method, path, body=encoded, headers={"Content-Type": "application/json"})
        response = self.connection.getresponse()
        raw = response.read().decode()
        if response.status >= 300:
            raise RuntimeError(f"Qdrant {method} {path}: {response.status}: {raw[:1000]}")
        return json.loads(raw)

    def setup(self, ids, vectors, build, table="items"):
        self.table = table
        exists = self.request("GET", f"/collections/{table}/exists")["result"]["exists"]
        if exists:
            self.request("DELETE", f"/collections/{table}")
        self.request("PUT", f"/collections/{table}", {
            "vectors": {"size": vectors.shape[1], "distance": "Euclid"},
            "shard_number": 1, "replication_factor": 1,
            "hnsw_config": {"m": build[0], "ef_construct": build[1], "full_scan_threshold": 10},
            "optimizers_config": {"indexing_threshold": 0, "default_segment_number": 1}})
        t = time.perf_counter()
        self.insert(ids, vectors)
        load = time.perf_counter() - t
        t = time.perf_counter()
        self.request("PATCH", f"/collections/{table}", {"optimizers_config": {"indexing_threshold": 1}})
        wait = self.wait()
        if not wait["settled"]:
            raise AssertionError("initial Qdrant indexing did not settle")
        return {"load_s": load, "build_s": time.perf_counter() - t, "index_wait": wait}

    def insert(self, ids, vectors):
        for start in range(0, len(ids), self.batch):
            batch_ids = list(map(int, ids[start:start + self.batch]))
            batch_vectors = vectors[start:start + self.batch].tolist()
            self.request("PUT", f"/collections/{self.table}/points?wait=true", {
                "batch": {"ids": batch_ids, "vectors": batch_vectors}})

    def delete(self, ids):
        if ids:
            self.request("POST", f"/collections/{self.table}/points/delete?wait=true", {"points": ids})

    def set_ef(self, value):
        self.ef = value

    def query(self, vector, k=100):
        result = self.request("POST", f"/collections/{self.table}/points/query", {
            "query": vector.tolist(), "limit": k,
            "params": {"hnsw_ef": self.ef, "exact": False},
            "with_payload": False, "with_vector": False})
        return [p["id"] for p in result["result"]["points"]]

    def snapshot(self):
        offset = None
        ids, vectors = [], []
        while True:
            body = {"limit": 1000, "with_vector": True, "with_payload": False}
            if offset is not None:
                body["offset"] = offset
            result = self.request("POST", f"/collections/{self.table}/points/scroll", body)["result"]
            for point in result["points"]:
                ids.append(point["id"])
                vectors.append(point["vector"])
            offset = result["next_page_offset"]
            if offset is None:
                return ids, vectors

    def state(self):
        return self.request("GET", f"/collections/{self.table}")["result"]

    def path(self, query):
        state = self.state()
        config = state["config"]["hnsw_config"]
        if state["indexed_vectors_count"] <= 0 or config["full_scan_threshold"] != 10:
            raise AssertionError("Qdrant HNSW state missing")
        return {"indexed_vectors_count": state["indexed_vectors_count"],
                "points_count": state["points_count"], "hnsw_config": config,
                "query_exact": False, "hnsw_ef": self.ef,
                "limit": "configuration + counters; not per-query internal execution trace"}

    def wait(self, timeout=120, poll_seconds=.25):
        t = time.perf_counter()
        samples = []
        while True:
            state = self.state()
            samples.append({**{k: state.get(k) for k in ("status", "optimizer_status", "points_count", "indexed_vectors_count", "segments_count")}, "elapsed_s": time.perf_counter() - t})
            if state.get("optimizer_status") != "ok":
                raise RuntimeError(f"optimizer error: {state}")
            settled = state["status"] == "green" and state["indexed_vectors_count"] >= state["points_count"]
            if settled or time.perf_counter() - t >= timeout:
                return {"settled": settled, "wait_s": time.perf_counter() - t,
                        "state": state, "samples": samples}
            time.sleep(poll_seconds)

    def close(self):
        self.connection.close()
