"""Bounded sequential real-server runner. Every invocation owns a new output root."""
import argparse
import dataclasses
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
import uuid

import numpy as np

from vectorbench.backends.pilot import Postgres, Qdrant
from vectorbench.experiments.pilot.model import Config, digest, exact, fixture, history, packed_results, score, validate_snapshot, validate_truth
from vectorbench.evidence.source_manifest import source_hashes as frozen_source_hashes


IMAGES = {
    "postgres": "pgvector/pgvector@sha256:7a7e9f22015b67edb4bef5c59daeebcd7e74bfa570df6ce60ae01237c8648a84",
    "qdrant": "qdrant/qdrant@sha256:94728574965d17c6485dd361aa3c0818b325b9016dac5ea6afec7b4b2700865f",
}
BUILDS = ((16, 100), (32, 200))
SCENARIOS = ("insert", "delete", "churn")
GIB = 1024 ** 3


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def source_hashes():
    return frozen_source_hashes()


def docker(*args, timeout=120):
    p = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {p.stderr}")
    return p.stdout.strip()


class Budget:
    def __init__(self, root):
        self.root = root
        self.started = time.monotonic()
        self.free_initial = shutil.disk_usage(".").free
        self.samples = []

    def check(self):
        free = shutil.disk_usage(".").free
        consumed = max(0, self.free_initial - free)
        image_bytes = sum(int(json.loads(docker("image", "inspect", v))[0]["Size"])
                          for v in IMAGES.values())
        sample = {"elapsed_s": time.monotonic() - self.started, "free_bytes": free,
                  "conservative_host_growth_bytes": consumed, "image_bytes": image_bytes}
        self.samples.append(sample)
        write_json(self.root / "budget.json", self.samples)
        if free < 25 * GIB or consumed + image_bytes > 8 * GIB:
            raise RuntimeError("disk budget exceeded (25GiB free / 8GiB new bytes)")
        if time.monotonic() - self.started > 4 * 3600:
            raise RuntimeError("four-hour runtime limit exceeded")


class Server:
    def __init__(self, db, root):
        self.db, self.root = db, root
        self.token = "ivu-" + uuid.uuid4().hex[:12]
        self.volume = self.token + "-data"
        self.created = False
        self.adapter = None

    def start(self):
        image = json.loads(docker("image", "inspect", IMAGES[self.db]))[0]
        if image["Architecture"] != "arm64":
            raise RuntimeError("native ARM64 image required")
        docker("volume", "create", "--label", "ivu.owner=" + self.token, self.volume)
        self.created = True
        target = "/var/lib/postgresql/data" if self.db == "postgres" else "/qdrant/storage"
        args = ["run", "-d", "--name", self.token, "--label", "ivu.owner=" + self.token,
                "--cpus", "2", "--memory", "4g", "--memory-swap", "4g",
                "-v", f"{self.volume}:{target}"]
        if self.db == "postgres":
            args += ["-e", "POSTGRES_HOST_AUTH_METHOD=trust"]
        else:
            args += ["-p", "127.0.0.1::6333", "-e", "QDRANT__TELEMETRY_DISABLED=true"]
        args += [IMAGES[self.db]]
        docker(*args)
        write_json(self.root / "container.json", json.loads(docker("inspect", self.token)))
        self.connect()
        return self.adapter

    def connect(self):
        deadline = time.monotonic() + 60
        last = ""
        while time.monotonic() < deadline:
            try:
                if self.db == "postgres":
                    # The entrypoint's temporary init server also answers pg_isready.
                    process_name = docker("exec", self.token, "cat", "/proc/1/comm", timeout=5)
                    if process_name != "postgres":
                        raise RuntimeError("PostgreSQL entrypoint initialization still running")
                    ready = docker("exec", self.token, "pg_isready", "-U", "postgres", timeout=5)
                    if "accepting connections" not in ready:
                        raise RuntimeError(ready)
                    self.adapter = Postgres(self.token, self.root)
                    write_json(self.root / "readiness.json", {"pid1_comm": process_name,
                        "pg_isready": ready, "accepted_final_postmaster": True})
                else:
                    port = int(docker("port", self.token, "6333/tcp").rsplit(":", 1)[1])
                    candidate = Qdrant(port)
                    candidate.request("GET", "/")
                    self.adapter = candidate
                return
            except (RuntimeError, OSError) as exc:
                last = str(exc)
                time.sleep(0.25)
        raise RuntimeError("server readiness failed: " + last)

    def restart(self):
        self.adapter.close()
        docker("restart", self.token)
        self.connect()
        return self.adapter

    def resources(self):
        return {"stats": docker("stats", "--no-stream", "--format", "{{json .}}", self.token),
                "disk": docker("exec", self.token, "df", "-Pk"),
                "volume_bytes": docker("exec", self.token, "du", "-sk",
                    "/var/lib/postgresql/data" if self.db == "postgres" else "/qdrant/storage")}

    def cleanup(self):
        errors = []
        if self.adapter is not None:
            try:
                self.adapter.close()
            except Exception as exc:
                errors.append(str(exc))
        if self.created:
            try:
                logs = subprocess.run(["docker", "logs", self.token], capture_output=True, text=True, timeout=30)
                (self.root / "server-stdout.log").write_text(logs.stdout)
                (self.root / "server-stderr.log").write_text(logs.stderr)
                if logs.returncode:
                    raise RuntimeError("server log capture failed: " + logs.stderr)
            except Exception as exc:
                errors.append(str(exc))
            try:
                owner = docker("inspect", "--format", '{{index .Config.Labels "ivu.owner"}}', self.token)
                if owner != self.token:
                    raise RuntimeError("container ownership mismatch")
                docker("rm", "-f", self.token)
            except Exception as exc:
                errors.append(str(exc))
            try:
                owner = docker("volume", "inspect", "--format", '{{index .Labels "ivu.owner"}}', self.volume)
                if owner != self.token:
                    raise RuntimeError("volume ownership mismatch")
                docker("volume", "rm", self.volume)
            except Exception as exc:
                errors.append(str(exc))
        write_json(self.root / "cleanup.json", {"errors": errors, "token": self.token})
        return errors


def measure(adapter, queries, budget):
    for q in queries:
        adapter.query(q)
    rows, latency = [], []
    for number, q in enumerate(queries):
        if number % 100 == 0:
            budget.check()
        t = time.perf_counter()
        rows.append(adapter.query(q))
        latency.append((time.perf_counter() - t) * 1000)
    return rows, latency


def tune(adapter, config, pool, queries, root, budget):
    ids = np.arange(config.n)
    chunk = config.n // 10
    probes = []
    for batch in (100, 1000):
        budget.check()
        adapter.batch = batch
        setup = adapter.setup(ids, pool[ids], BUILDS[0])
        added = np.arange(config.n, config.n + chunk)
        t = time.perf_counter()
        adapter.insert(added, pool[added])
        insert_s = time.perf_counter() - t
        expected = np.arange(config.n + chunk)
        validate_snapshot(expected, pool[expected], *adapter.snapshot())
        probes.append({"batch": batch, "setup": setup, "incremental_insert_s": insert_s})
        write_json(root / "batch-probes.json", probes)
    initial_batch = min(probes, key=lambda x: x["setup"]["load_s"])["batch"]
    insert_batch = min(probes, key=lambda x: x["incremental_insert_s"])["batch"]
    truth = exact(ids, pool[ids], queries)
    validate_truth(ids, pool[ids], queries, truth)
    candidates = []
    for build in BUILDS:
        budget.check()
        adapter.batch = initial_batch
        setup = adapter.setup(ids, pool[ids], build)
        validate_snapshot(ids, pool[ids], *adapter.snapshot())
        path = adapter.path(queries[0])
        for ef in sorted({max(100, v) for v in (32, 64, 128, 256)}):
            adapter.set_ef(ef)
            rows, ms = measure(adapter, queries, budget)
            quality = score(ids, truth, rows)
            key = f"m{build[0]}-ef{ef}"
            np.savez_compressed(root / f"tune-{key}.npz", truth=truth, latency_ms=ms, **packed_results(rows))
            candidate = {"build": build, "ef": ef, "p50_ms": float(np.median(ms)),
                         "setup": setup, "path": path, **quality}
            candidates.append(candidate)
            write_json(root / "candidates.json", candidates)
    passing = [c for c in candidates if c["recall10"] >= .95 and c["recall100"] >= .95]
    if not passing:
        raise RuntimeError("no tuning candidate meets both recall targets")
    chosen = min(passing, key=lambda c: c["p50_ms"])
    choice = {"build": chosen["build"], "ef": chosen["ef"],
              "initial_batch": initial_batch, "incremental_batch": insert_batch,
              "quality": {k: chosen[k] for k in ("recall10", "recall100", "p50_ms")}}
    write_json(root / "chosen.json", choice)
    return choice


def checkpoint(adapter, ids, pool, queries, root, step, budget, write_s, event, identity):
    budget.check()
    wait = adapter.wait()
    validate_snapshot(ids, pool[ids], *adapter.snapshot())
    state = adapter.state()
    if adapter.name == "postgres" and identity is not None:
        if (state["table_oid"], state["index_oid"]) != identity:
            raise AssertionError("index rebuilt inside incremental history")
    truth = exact(ids, pool[ids], queries)
    validate_truth(ids, pool[ids], queries, truth)
    path = adapter.path(queries[0])
    rows, ms = measure(adapter, queries, budget)
    quality = score(ids, truth, rows)
    receipt = {"step": step, "count": len(ids), "alive_hash": digest(ids, pool[ids]),
               "query_hash": hashlib.sha256(queries.tobytes()).hexdigest(), "write_s": write_s,
               "event": event, "maintenance": wait, "state": state, "path": path,
               "p50_ms": float(np.percentile(ms, 50)), "p95_ms": float(np.percentile(ms, 95)),
               "p99_ms": float(np.percentile(ms, 99)), **quality}
    np.savez_compressed(root / f"checkpoint-{step}.npz", alive_ids=ids, truth=truth,
                        latency_ms=ms, **packed_results(rows))
    receipt["raw_sha256"] = hashlib.sha256((root / f"checkpoint-{step}.npz").read_bytes()).hexdigest()
    write_json(root / f"checkpoint-{step}.json", receipt)
    return receipt


def run_history(adapter, server, config, seed, scenario, pool, queries, choice, root, budget, restart=False):
    root.mkdir(parents=True)
    adapter.batch = choice["initial_batch"]
    setup = adapter.setup(np.arange(config.n), pool[:config.n], choice["build"])
    write_json(root / "initial-build.json", setup)
    adapter.set_ef(choice["ef"])
    state = adapter.state()
    identity = (state["table_oid"], state["index_oid"]) if adapter.name == "postgres" else None
    if restart:
        adapter = server.restart()
        adapter.set_ef(choice["ef"])
        validate_snapshot(np.arange(config.n), pool[:config.n], *adapter.snapshot())
        write_json(root / "restart.json", {"passed": True, "state": adapter.state()})
    adapter.batch = choice["incremental_batch"]
    receipt = None
    for step, (ids, deleted, inserted) in enumerate(history(config, seed, scenario)):
        event = {"delete": deleted, "insert": inserted}
        write_json(root / f"event-{step}.json", event)
        t = time.perf_counter()
        adapter.delete(deleted)
        if inserted:
            adapter.insert(inserted, pool[inserted])
        write_s = time.perf_counter() - t
        receipt = checkpoint(adapter, ids, pool, queries, root, step, budget, write_s, event, identity)
        print(json.dumps({"db": adapter.name, "seed": seed, "scenario": scenario,
                          "step": step, "recall10": receipt["recall10"],
                          "recall100": receipt["recall100"]}), flush=True)
    adapter.batch = choice["initial_batch"]
    setup = adapter.setup(ids, pool[ids], choice["build"], table="control")
    write_json(root / "control-build.json", setup)
    adapter.set_ef(choice["ef"])
    control = checkpoint(adapter, ids, pool, queries, root, "control", budget, 0, {}, None)
    if receipt["alive_hash"] != control["alive_hash"]:
        raise AssertionError("rebuilt control data differs from incremental final state")
    write_json(root / "done.json", {"passed": True, "checkpoints": config.rounds + 1,
                "delta_recall10": receipt["recall10"] - control["recall10"],
                "delta_recall100": receipt["recall100"] - control["recall100"]})
    return adapter


def inventory():
    return {"containers": docker("ps", "-aq"), "volumes": docker("volume", "ls", "-q"),
            "images": docker("image", "ls", "--digests", "--format", "{{json .}}")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("smoke", "pilot"), required=True)
    args = parser.parse_args()
    config = Config.smoke() if args.stage == "smoke" else Config()
    sources = source_hashes()
    prerequisite = None
    if args.stage == "pilot":
        for path in sorted(Path("runs").glob("pilot-*/manifest.json"), reverse=True):
            previous = json.loads(path.read_text())
            if previous["stage"] == "smoke" and previous["status"] == "passed" and previous["sources"] == sources:
                from vectorbench.experiments.pilot.report import validate_run
                validate_run(path.parent)
                prerequisite = str(path)
                break
        if prerequisite is None:
            raise SystemExit("matching fully validated smoke receipt required")
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + args.stage + "-" + uuid.uuid4().hex[:6]
    root = Path("runs") / ("pilot-" + run_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {"id": run_id, "stage": args.stage, "status": "running", "config": dataclasses.asdict(config),
                "sources": sources, "images": IMAGES, "prerequisite": prerequisite,
                "planned_histories": 2 * len(config.seeds) * 3,
                "planned_checkpoints": 2 * len(config.seeds) * 3 * (config.rounds + 1),
                "completed_histories": 0, "cleanup_errors": [],
                "shared_chat": "UNVERIFIED_EGO_START_FAILED", "project_checks": "without-setup (explicit user choice); standalone checks separate"}
    write_json(root / "manifest.json", manifest)
    budget = Budget(root)
    try:
        write_json(root / "inventory-before.json", inventory())
        budget.check()
        for seed_index, seed in enumerate(config.seeds):
            pool, tuning, evaluation = fixture(config, seed)
            data_root = root / f"seed-{seed}"
            data_root.mkdir()
            np.savez_compressed(data_root / "fixture.npz", pool=pool, tune=tuning, evaluation=evaluation)
            write_json(data_root / "fixture.json", {"pool_hash": digest(np.arange(len(pool)), pool),
                        "seed": seed, "numpy": np.__version__, "python": sys.version})
            order = ("postgres", "qdrant") if seed_index % 2 == 0 else ("qdrant", "postgres")
            for db in order:
                db_root = data_root / db
                db_root.mkdir()
                server = Server(db, db_root)
                try:
                    adapter = server.start()
                    choice = tune(adapter, config, pool, tuning, db_root, budget)
                    for scenario_index, scenario in enumerate(SCENARIOS):
                        adapter = run_history(adapter, server, config, seed, scenario, pool, evaluation,
                            choice, db_root / scenario, budget, restart=scenario_index == 0)
                        manifest["completed_histories"] += 1
                        write_json(db_root / f"resources-{scenario}.json", server.resources())
                        write_json(root / "manifest.json", manifest)
                finally:
                    manifest["cleanup_errors"].extend(server.cleanup())
                    write_json(root / "manifest.json", manifest)
        if manifest["cleanup_errors"]:
            raise RuntimeError("resource cleanup incomplete")
        from vectorbench.experiments.pilot.report import validate_run
        validate_run(root)
        manifest["status"] = "passed"
    except BaseException as exc:
        manifest["status"] = "failed"
        manifest["first_error"] = repr(exc)
        (root / "failure.txt").write_text(traceback.format_exc())
        print(traceback.format_exc(), file=sys.stderr)
    finally:
        manifest["elapsed_s"] = time.monotonic() - budget.started
        try:
            write_json(root / "inventory-after.json", inventory())
        except Exception as exc:
            manifest["inventory_after_error"] = repr(exc)
        write_json(root / "manifest.json", manifest)
        from vectorbench.experiments.pilot.report import report
        report(root)
        print(f"RESULT_ROOT={root} STATUS={manifest['status']}", flush=True)
    return 0 if manifest["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
