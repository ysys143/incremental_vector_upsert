"""Transactional admission/leases only; native Dagster runs own execution success."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time

from vectorbench.evidence.contracts import Config, GIB, ResourceGate, canonical


class AdmissionConflict(RuntimeError):
    pass


class Registry:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connection() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS attempts (
                    node TEXT NOT NULL, run_id TEXT NOT NULL, spec_hash TEXT NOT NULL,
                    started REAL NOT NULL, ended REAL, outcome TEXT, receipt TEXT,
                    PRIMARY KEY(node,run_id));
                CREATE TABLE IF NOT EXISTS leases (
                    node TEXT PRIMARY KEY, run_id TEXT NOT NULL, class TEXT NOT NULL,
                    reserve_bytes INTEGER NOT NULL, started REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS retry_dispositions (
                    node TEXT NOT NULL, prior_run TEXT NOT NULL, reason TEXT NOT NULL,
                    created REAL NOT NULL, PRIMARY KEY(node,prior_run));
                CREATE TABLE IF NOT EXISTS phases (
                    phase TEXT PRIMARY KEY, started REAL NOT NULL, deadline REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS budget (
                    id INTEGER PRIMARY KEY CHECK(id=1), baseline_free INTEGER NOT NULL);
            """)

    @contextmanager
    def connection(self):
        c = sqlite3.connect(self.path, timeout=15)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def retry(self, node, prior_run, reason):
        if not reason.strip():
            raise ValueError("explicit retry reason required")
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            prior = c.execute("SELECT * FROM attempts WHERE node=? AND run_id=?", (node, prior_run)).fetchone()
            if prior is None or prior["ended"] is None or prior["outcome"] == "worker-completed":
                raise AdmissionConflict("only a reconciled failed attempt can receive retry disposition")
            c.execute("INSERT INTO retry_dispositions VALUES (?,?,?,?)", (node, prior_run, reason, time.time()))

    def claim(self, node, run_id, spec_hash):
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            prior = c.execute("SELECT * FROM attempts WHERE node=? ORDER BY started DESC LIMIT 1", (node,)).fetchone()
            if prior:
                if prior["spec_hash"] != spec_hash:
                    raise AdmissionConflict("changed scientific inputs require a new campaign")
                if prior["ended"] is None or prior["outcome"] == "worker-completed":
                    raise AdmissionConflict("running/completed node cannot execute again")
                approval = c.execute("SELECT 1 FROM retry_dispositions WHERE node=? AND prior_run=?", (node, prior["run_id"])).fetchone()
                if approval is None:
                    raise AdmissionConflict("explicit prior-attempt retry disposition required")
            c.execute("INSERT INTO attempts(node,run_id,spec_hash,started) VALUES (?,?,?,?)", (node, run_id, spec_hash, time.time()))

    def deadline(self, phase, seconds=14400):
        if not 0 < seconds <= 14400:
            raise ValueError("phase cap must remain within four hours")
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT deadline FROM phases WHERE phase=?", (phase,)).fetchone()
            if row is not None:
                return row["deadline"]
            started = time.time()
            c.execute("INSERT INTO phases VALUES (?,?,?)", (phase, started, started + seconds))
            return started + seconds

    def acquire(self, node, run_id, kind, reserve_bytes, free_bytes, config=None):
        cfig = config or Config()
        cfig.validate()
        if kind not in {"cpu", "io", "db"} or reserve_bytes < 0:
            raise ValueError("invalid resource request")
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            attempt = c.execute("SELECT * FROM attempts WHERE node=? AND run_id=?", (node, run_id)).fetchone()
            if attempt is None or attempt["ended"] is not None:
                raise AdmissionConflict("resource consumer has no active owned attempt")
            rows = c.execute("SELECT * FROM leases").fetchall()
            if any(r["node"] == node for r in rows):
                raise AdmissionConflict("node already has a resource lease")
            if kind == "db" and rows or kind != "db" and any(r["class"] == "db" for r in rows):
                raise ResourceGate("DB measurement excludes all other heavy tasks")
            limit = 1 if kind == "db" else 2
            if sum(r["class"] == kind for r in rows) >= limit:
                raise ResourceGate("resource class has no free slot")
            if kind != "db" and sum(r["class"] in {"cpu", "io"} for r in rows) >= 2:
                raise ResourceGate("combined heavy preparation limit is two")
            reserved = sum(r["reserve_bytes"] for r in rows)
            baseline = c.execute("SELECT baseline_free FROM budget WHERE id=1").fetchone()
            original_free = free_bytes if baseline is None else baseline["baseline_free"]
            if baseline is None:
                c.execute("INSERT INTO budget VALUES (1,?)", (original_free,))
            c.execute("INSERT INTO leases VALUES (?,?,?,?,?)", (node, run_id, kind, reserve_bytes, time.time()))

    def baseline_free(self):
        with self.connection() as c:
            row = c.execute("SELECT baseline_free FROM budget WHERE id=1").fetchone()
            return None if row is None else row["baseline_free"]

    def check_owned_storage(self,node,run_id,used_bytes,config=None):
        cfig=config or Config(); cfig.validate()
        with self.connection() as c:
            leases=c.execute("SELECT * FROM leases").fetchall()
            if not any(r["node"]==node and r["run_id"]==run_id for r in leases):
                raise AdmissionConflict("storage audit requires the resource lease")
            reserved=sum(r["reserve_bytes"] for r in leases)
            if used_bytes < 0: raise ValueError("negative observed storage size")
            return {"owned_bytes":used_bytes,"reserved_bytes":reserved,"cap_bytes":cfig.growth_cap_bytes,
                    "disk_policy":"OBSERVE_ONLY"}

    def inherit_baseline(self, original_free):
        if not isinstance(original_free, int) or original_free <= 0:
            raise ValueError("space baseline must be a positive original byte count")
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT baseline_free FROM budget WHERE id=1").fetchone()
            if row is not None and row["baseline_free"] != original_free:
                raise AdmissionConflict("a successor cannot reset the original space baseline")
            if row is None:
                c.execute("INSERT INTO budget VALUES (1,?)", (original_free,))

    def finish(self, node, run_id, outcome, receipt=None, retain_lease=False):
        if outcome not in {"worker-completed", "failed", "stopped"}:
            raise ValueError("worker completion is not native success")
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            current = c.execute("SELECT * FROM attempts WHERE node=? AND run_id=?", (node, run_id)).fetchone()
            if current is None or current["ended"] is not None:
                raise AdmissionConflict("cannot finish missing/already finished attempt")
            if not retain_lease:
                c.execute("DELETE FROM leases WHERE node=? AND run_id=?", (node, run_id))
            c.execute("UPDATE attempts SET ended=?,outcome=?,receipt=? WHERE node=? AND run_id=?",
                      (time.time(), outcome, receipt, node, run_id))

    def release_checked(self, node, run_id):
        """Release only after the native blocking check has finished its heavy reads."""
        with self.connection() as c:
            row = c.execute("SELECT outcome FROM attempts WHERE node=? AND run_id=?", (node, run_id)).fetchone()
            if row is None or row["outcome"] != "worker-completed":
                raise AdmissionConflict("blocking check has no completed owned worker")
            c.execute("DELETE FROM leases WHERE node=? AND run_id=?", (node, run_id))

    def snapshot(self):
        with self.connection() as c:
            return {name: [dict(r) for r in c.execute("SELECT * FROM " + name)]
                    for name in ("attempts", "leases", "retry_dispositions", "phases")}
