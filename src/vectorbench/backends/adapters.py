"""IP-aware real-study adapters without changing frozen synthetic-study adapters."""
import json
import time

import numpy as np

from vectorbench.backends.pilot import Postgres as LegacyPostgres, Qdrant as LegacyQdrant, vector_text


METRICS = {"ip": ("<#>", "vector_ip_ops", "Dot"), "l2": ("<->", "vector_l2_ops", "Euclid")}


def assert_ann(plan, seqscan, index_name="items_hnsw", metric="ip", analyzed=False):
    if metric not in METRICS or seqscan != "off":
        raise AssertionError("metric/seqscan guard failed")
    if not isinstance(plan, list) or len(plan) != 1 or "Plan" not in plan[0]:
        raise AssertionError("missing PostgreSQL plan")
    pending, found = [plan[0]["Plan"]], 0
    while pending:
        node = pending.pop()
        kind = node.get("Node Type")
        if kind not in {"Aggregate", "Limit", "Index Scan"}:
            raise AssertionError("non-HNSW node: " + str(kind))
        if kind == "Index Scan":
            if node.get("Index Name") != index_name or METRICS[metric][0] not in node.get("Order By", ""):
                raise AssertionError("wrong HNSW index or metric operator")
            if analyzed and node.get("Actual Loops", 0) < 1:
                raise AssertionError("HNSW did not execute")
            found += 1
        pending.extend(node.get("Plans", []))
    if found != 1:
        raise AssertionError("expected exactly one HNSW scan")


def validate_rows(ids, vectors, attributes=None):
    ids = list(map(int, ids))
    x = np.asarray(vectors, dtype=np.float32)
    if len(ids) != len(set(ids)) or x.ndim != 2 or len(ids) != len(x) or not np.isfinite(x).all():
        raise ValueError("duplicate IDs, wrong shape, or non-finite vectors")
    if any(i < 0 for i in ids):
        raise ValueError("negative IDs unsupported")
    a = np.zeros((len(ids), 3), dtype=np.int64) if attributes is None else np.asarray(attributes, dtype=np.int64)
    if a.shape != (len(ids), 3):
        raise ValueError("attributes must contain category, ordinal, version for every row")
    return ids, x, a


class Postgres(LegacyPostgres):
    def __init__(self, container, root, metric="ip"):
        if metric not in METRICS:
            raise ValueError(metric)
        super().__init__(container, root)
        self.metric = metric
        self.predicate = None

    def setup(self, ids, vectors, build, table="items", attributes=None):
        if table not in {"items", "control"}:
            raise ValueError(table)
        self.table = table
        self.sql(f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id bigint PRIMARY KEY, embedding vector({vectors.shape[1]}), category bigint NOT NULL, ordinal bigint NOT NULL, version bigint NOT NULL) WITH (parallel_workers=1);")
        started = time.perf_counter()
        self.insert(ids, vectors, attributes)
        load_s = time.perf_counter() - started
        if getattr(self, 'filter_labels', None) is not None:
            from vectorbench.evaluation.twelve_filters import install_postgres_labels
            install_postgres_labels(self, ids)
        started = time.perf_counter()
        # DDL has a separate bound; search sessions retain the 120s query limit.
        self.sql(f"SET statement_timeout='600s'; CREATE INDEX {table}_hnsw ON {table} USING hnsw (embedding {METRICS[self.metric][1]}) WITH (m={build[0]},ef_construction={build[1]}); ANALYZE {table}; SET statement_timeout='120s';")
        return {"load_s": load_s, "build_s": time.perf_counter() - started}

    def _copy(self, table, ids, vectors, attributes):
        ids, x, a = validate_rows(ids, vectors, attributes)
        rows = "\n".join(f"{i}\t{vector_text(v)}\t{int(m[0])}\t{int(m[1])}\t{int(m[2])}" for i, v, m in zip(ids, x, a))
        self.sql(f"COPY {table} (id,embedding,category,ordinal,version) FROM STDIN;\n{rows}\n\\.")

    def insert(self, ids, vectors, attributes=None):
        ids, x, a = validate_rows(ids, vectors, attributes)
        for start in range(0, len(ids), self.batch):
            self._copy(self.table, ids[start:start+self.batch], x[start:start+self.batch], a[start:start+self.batch])

    def upsert(self, ids, vectors, attributes=None):
        ids, x, a = validate_rows(ids, vectors, attributes)
        if not ids:
            return
        filtered = getattr(self, 'filter_labels', None) is not None
        if filtered:
            self.sql(f"BEGIN; CREATE TEMP TABLE ivu_stage ON COMMIT DROP AS SELECT id,embedding,category,ordinal,version FROM {self.table} WITH NO DATA;")
        else:
            self.sql(f"BEGIN; CREATE TEMP TABLE ivu_stage (LIKE {self.table} INCLUDING DEFAULTS) ON COMMIT DROP;")
        try:
            self._copy("ivu_stage", ids, x, a)
            columns = 'id,embedding,category,ordinal,version'
            selection = f'SELECT {columns} FROM ivu_stage'
            if filtered:
                from vectorbench.evaluation.twelve_filters import FIELDS
                selection = 'SELECT '+','.join('s.'+k for k in columns.split(','))+','+','.join('t.'+k for k in FIELDS)+f' FROM ivu_stage s JOIN {self.table} t ON t.id=s.id'
                columns += ','+','.join(FIELDS)
            self.sql(f"INSERT INTO {self.table} AS target ({columns}) {selection} ON CONFLICT (id) DO UPDATE SET embedding=EXCLUDED.embedding,category=EXCLUDED.category,ordinal=EXCLUDED.ordinal,version=EXCLUDED.version; COMMIT;")
        except BaseException:
            if self.process.poll() is None:
                try:
                    self.sql("ROLLBACK;")
                except Exception:
                    pass
            raise

    def search_sql(self, vector, k=100):
        where = "" if self.predicate is None else " WHERE " + self.predicate
        return f"SELECT coalesce(json_agg(id),'[]') FROM (SELECT id FROM {self.table}{where} ORDER BY embedding {METRICS[self.metric][0]} '{vector_text(vector)}'::vector LIMIT {int(k)}) q;"

    def checked_search_sql(self, vector, k=100):
        statement = self.search_sql(vector, k)
        raw = self.sql("SHOW enable_seqscan;\nEXPLAIN (FORMAT JSON) " + statement)
        setting, plan = raw.split("\n", 1)
        assert_ann(json.loads(plan), setting, self.table + "_hnsw", self.metric)
        return statement

    def path(self, query):
        raw = self.sql("SHOW enable_seqscan;\nEXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + self.search_sql(query))
        setting, plan = raw.split("\n", 1)
        plan = json.loads(plan)
        assert_ann(plan, setting, self.table + "_hnsw", self.metric, analyzed=True)
        return plan

    def metadata(self):
        raw = self.sql(f"SELECT coalesce(json_agg(r),'[]') FROM (SELECT id,category,ordinal,version FROM {self.table} ORDER BY id) r;")
        return json.loads(raw)


class Qdrant(LegacyQdrant):
    def __init__(self, port, root=None, metric="ip"):
        super().__init__(port, root)
        if metric not in METRICS:
            raise ValueError(metric)
        self.metric, self.predicate = metric, None

    def request(self, method, path, body=None):
        encoded = json.dumps(body, separators=(",", ":")).encode() if body is not None else None
        return self.request_encoded(method, path, encoded)

    def request_encoded(self, method, path, encoded):
        readonly = path.split("?", 1)[0].endswith(("/query", "/scroll", "/count"))
        if method in {"PUT", "PATCH", "DELETE"} or (method == "POST" and not readonly):
            # Serialize first, then start a fresh write connection. Never replay a failed write.
            self.connection.close()
        self.connection.request(method, path, body=encoded, headers={"Content-Type": "application/json"})
        response = self.connection.getresponse()
        raw = response.read().decode()
        if response.status >= 300:
            raise RuntimeError(f"Qdrant {method} {path}: {response.status}: {raw[:1000]}")
        return json.loads(raw)

    def setup(self, ids, vectors, build, table="items", attributes=None):
        self.table = table
        if self.request("GET", f"/collections/{table}/exists")["result"]["exists"]:
            self.request("DELETE", f"/collections/{table}")
        self.request("PUT", f"/collections/{table}", {
            "vectors": {"size": vectors.shape[1], "distance": METRICS[self.metric][2]},
            "shard_number": 1, "replication_factor": 1,
            "hnsw_config": {"m": build[0], "ef_construct": build[1], "full_scan_threshold": 10},
            "optimizers_config": {"indexing_threshold": 0, "default_segment_number": 1}})
        if getattr(self, 'filter_labels', None) is not None:
            from vectorbench.evaluation.twelve_filters import FIELDS
            for field in FIELDS:
                self.request('PUT',f'/collections/{table}/index?wait=true',{'field_name':field,'field_schema':'integer'})
        started = time.perf_counter()
        self.insert(ids, vectors, attributes)
        load_s = time.perf_counter() - started
        started = time.perf_counter()
        self.request("PATCH", f"/collections/{table}", {"optimizers_config": {"indexing_threshold": 1}})
        wait = self.wait()
        if not wait["settled"]:
            raise AssertionError("initial Qdrant indexing did not settle")
        return {"load_s": load_s, "build_s": time.perf_counter()-started, "index_wait": wait}

    def insert(self, ids, vectors, attributes=None):
        ids, x, a = validate_rows(ids, vectors, attributes)
        for start in range(0, len(ids), self.batch):
            chunk = slice(start, start+self.batch)
            payloads = [{"category": int(m[0]), "ordinal": int(m[1]), "version": int(m[2])} for m in a[chunk]]
            if getattr(self, 'filter_labels', None) is not None:
                payloads = [{**p, **self.filter_labels[str(i)]} for i,p in zip(ids[chunk],payloads)]
            self.request("PUT", f"/collections/{self.table}/points?wait=true", {
                "batch": {"ids": ids[chunk], "vectors": x[chunk].tolist(), "payloads": payloads}})

    upsert = insert

    def query(self, vector, k=100):
        body = {"query": vector.tolist(), "limit": k, "params": {"hnsw_ef": self.ef, "exact": False},
                "with_payload": False, "with_vector": False}
        if self.predicate is not None:
            body["filter"] = self.predicate
        result = self.request("POST", f"/collections/{self.table}/points/query", body)
        return [p["id"] for p in result["result"]["points"]]

    def metadata(self):
        result, offset = [], None
        while True:
            body = {"limit": 1000, "with_vector": False, "with_payload": True}
            if offset is not None:
                body["offset"] = offset
            page = self.request("POST", f"/collections/{self.table}/points/scroll", body)["result"]
            result.extend({"id": int(p["id"]), **{k:p['payload'][k] for k in ('category','ordinal','version')}} for p in page["points"])
            offset = page.get("next_page_offset")
            if offset is None:
                return sorted(result, key=lambda p: p["id"])
