"""Owned Docker snapshots and independent cleanup; never select resources by a broad prefix."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time
import uuid
from functools import partial

from vectorbench.experiments.pilot.run import IMAGES, Server, docker
from vectorbench.experiments.synthetic_insertion.contracts import load, save, sha
from vectorbench.orchestration.synthetic_cleanup import declare, release_journal
from vectorbench.evidence.synthetic_telemetry import segments


class OwnedServer(Server):
    def __init__(self, db, out, archive=None, archive_sha=None, files_hash=None, segment_signature=None, graph_hash=None):
        super().__init__(db, out)
        self.archive, self.archive_sha, self.files_hash = archive, archive_sha, files_hash
        self.segment_signature, self.graph_hash = segment_signature, graph_hash
        declare(out, "volume", self.volume, self.token)
        declare(out, "container", self.token, self.token)

    def connect(self):
        super().connect()
        if self.db == "qdrant":
            self.adapter.wait = partial(self.adapter.wait, poll_seconds=.5)

    def helper(self, command, input_file=None, output_file=None, readonly=False):
        name = self.token + "-helper-" + uuid.uuid4().hex[:6]
        declare(self.root, "container", name, self.token)
        args = ["docker", "run", "--rm", "-i", "--name", name, "--label", "ivu.owner=" + self.token,
                "-v", self.volume + ":/data" + (":ro" if readonly else ""),
                "--entrypoint", command[0], IMAGES["postgres"], *command[1:]]
        p = subprocess.run(args, stdin=input_file, stdout=output_file or subprocess.PIPE,
                           stderr=subprocess.PIPE, timeout=120)
        if p.returncode:
            raise RuntimeError("snapshot helper failed: " + p.stderr.decode(errors="replace"))
        return p.stdout

    def volume_digest(self):
        raw = self.helper(["sh", "-c", "cd /data && find . -type f -exec sha256sum {} + | LC_ALL=C sort"], readonly=True)
        return hashlib.sha256(raw).hexdigest()

    def qdrant_signature(self):
        raw = self.adapter.request("GET", "/telemetry?details_level=4&anonymize=false")
        return {uid: s["stable"] for uid, s in segments(raw).items() if s["stable"]["count"]}

    def graph_digest(self):
        raw = self.helper(["sh", "-c", "cd /data && find . -type f \\( -path '*/vector_index/*' -o -name 'graph.bin' -o -name '*links*' \\) -exec sha256sum {} + | LC_ALL=C sort"], readonly=True)
        if not raw.strip():
            raise AssertionError("no persisted Qdrant graph files found for clone verification")
        return hashlib.sha256(raw).hexdigest()

    def start(self):
        if self.archive is not None:
            if sha(self.archive) != self.archive_sha:
                raise AssertionError("initial snapshot archive changed")
            docker("volume", "create", "--label", "ivu.owner=" + self.token, self.volume)
            self.created = True
            with open(self.archive, "rb") as stream:
                self.helper(["tar", "-xzf", "-", "-C", "/data"], input_file=stream)
            actual = self.volume_digest()
            if actual != self.files_hash:
                raise AssertionError("restored initial index files differ")
            save(self.root / "restore.json", {"archive_sha": self.archive_sha,
                "files_hash": actual, "verified_before_server_start": True})
        adapter = super().start()
        if self.db == "qdrant" and self.archive is not None:
            if self.segment_signature is None or self.qdrant_signature() != self.segment_signature:
                raise AssertionError("nonempty Qdrant segment identity/index changed on restore")
            if self.graph_hash is None or self.graph_digest() != self.graph_hash:
                raise AssertionError("Qdrant graph files changed during restore/startup")
        return adapter

    def export(self, target):
        state = self.adapter.state()
        signature = None
        if self.db == "qdrant":
            interval = state["config"]["optimizer_config"]["flush_interval_sec"]
            time.sleep(interval + 1)
            # The server's keep-alive is 5s; reconnect before this new read, never retry a write.
            self.adapter.connection.close()
            signature = self.qdrant_signature()
        self.adapter.close()
        self.adapter = None
        docker("stop", "-t", "30", self.token)
        stopped = json.loads(docker("inspect", self.token))[0]["State"]
        save(self.root / "shutdown-state.json", stopped)
        graceful = stopped["ExitCode"] == 0
        if self.db == "qdrant" and stopped["ExitCode"] == 143:
            p = subprocess.run(["docker", "logs", self.token], capture_output=True, text=True, timeout=15)
            graceful = "SIGTERM received; starting graceful shutdown" in (p.stdout + p.stderr)
        if stopped["Running"] or stopped["OOMKilled"] or stopped["Error"] or not graceful:
            raise AssertionError("database did not shut down cleanly for snapshot")
        files_hash = self.volume_digest()
        graph_hash = self.graph_digest() if self.db == "qdrant" else None
        with open(target, "wb") as stream:
            self.helper(["tar", "-czf", "-", "-C", "/data", "."], output_file=stream, readonly=True)
        return {"archive_sha": sha(target), "files_hash": files_hash, "segment_signature": signature,
                "graph_hash": graph_hash,
                "initial_state": state, "clean_shutdown": stopped}


def space_guard(root):
    path = root / "space-baseline.json"
    if not path.exists():
        save(path, {"initial_free": shutil.disk_usage(root).free})
    free = shutil.disk_usage(root).free
    growth = max(0, load(path)["initial_free"] - free)
    sample = {"time": time.time(), "free_bytes": free, "host_growth_bytes": growth}
    with open(root / "space-samples.jsonl", "a") as stream:
        stream.write(json.dumps(sample) + "\n")
    if free < 25 * 1024**3 or growth > 8 * 1024**3:
        raise RuntimeError("space guard: 25GiB floor / 8GiB growth exceeded")
    return sample
