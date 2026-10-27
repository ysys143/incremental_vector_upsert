"""One guarded, owned real-study DB with pinned images and verified IP adapters."""
import json
import subprocess
import os

from vectorbench.experiments.pilot.run import IMAGES, docker
from vectorbench.orchestration.synthetic_resources import OwnedServer
from vectorbench.backends.adapters import Postgres, Qdrant
from vectorbench.evidence.contracts import Config, check_space, save, sha


class Server(OwnedServer):
    def __init__(self, db, out, metric="ip", config=None, filter_labels=None, **archive):
        super().__init__(db, out, **archive)
        self.metric = metric
        self.config = config or Config()
        self.monitor = None
        self.filter_labels = filter_labels

    def start(self):
        check_space(self.root, self.config, reserve=512 * 1024 ** 2)
        if self.archive is not None and sha(self.archive) != self.archive_sha:
            raise AssertionError("initial snapshot archive changed")
        image = json.loads(docker("image", "inspect", IMAGES[self.db]))[0]
        if image["Architecture"] != "arm64":
            raise AssertionError("native ARM64 image required")
        docker("volume", "create", "--label", "ivu.owner=" + self.token, self.volume)
        self.created = True
        if self.archive is not None:
            with open(self.archive, "rb") as stream:
                self.helper(["tar", "-xzf", "-", "-C", "/data"], input_file=stream)
            actual = self.volume_digest()
            if actual != self.files_hash:
                raise AssertionError("restored initial graph files differ")
            save(self.root / "restore.json", {"archive_sha": self.archive_sha,
                 "files_hash": actual, "verified_before_server_start": True})
        target = "/var/lib/postgresql/data" if self.db == "postgres" else "/qdrant/storage"
        args = ["run", "-d", "--name", self.token, "--label", "ivu.owner=" + self.token,
                "--cpus", str(self.config.db_cpus), "--memory", str(self.config.db_memory_gib) + "g",
                "--memory-swap", str(self.config.db_memory_gib) + "g", "-v", self.volume + ":" + target]
        if self.db == "postgres":
            args += ["--shm-size", "1g", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGES[self.db],
                     "-c", "shared_buffers=1GB", "-c", "maintenance_work_mem=512MB",
                     "-c", "max_parallel_maintenance_workers=1"]
        else:
            mib = self.config.max_payload_bytes // 1024 ** 2
            args += ["-p", "127.0.0.1::6333", "-e", "QDRANT__TELEMETRY_DISABLED=true",
                     "-e", f"QDRANT__SERVICE__MAX_REQUEST_SIZE_MB={mib}", IMAGES[self.db]]
        docker(*args)
        save(self.root / "container.json", json.loads(docker("inspect", self.token)))
        self.connect()
        if self.archive is not None and self.db == "qdrant":
            if self.qdrant_signature() != self.segment_signature or self.graph_digest() != self.graph_hash:
                raise AssertionError("Qdrant index identity changed on clone/startup")
        if os.environ.get("REAL_STUDY_RESOURCE_SAMPLING") == "1":
            from vectorbench.orchestration.resource_monitor import Monitor
            self.monitor = Monitor(self.token, self.root)
        return self.adapter

    def cleanup(self):
        errors = []
        if self.monitor is not None:
            try:
                errors.extend(self.monitor.stop())
            except Exception as exc:
                errors.append("owned resource monitor cleanup: " + repr(exc))
            self.monitor = None
        errors.extend(super().cleanup())
        return errors

    def connect(self):
        # Reuse the already tested readiness consumer (including final postmaster detection).
        super().connect()
        self.adapter.close()
        if self.db == "postgres":
            self.adapter = Postgres(self.token, self.root, metric=self.metric)
        else:
            port = int(docker("port", self.token, "6333/tcp").rsplit(":", 1)[1])
            self.adapter = Qdrant(port, self.root, metric=self.metric)
        self.adapter.filter_labels = self.filter_labels
