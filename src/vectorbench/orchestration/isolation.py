"""Record surrounding containers without blocking measured DB phases."""
import time

from vectorbench.experiments.pilot.run import docker
from vectorbench.evidence.contracts import save


def observed(server):
    names = docker("ps", "--format", "{{.Names}}").splitlines()
    return {"time": time.time(), "owned": server.token, "running": names,
            "foreign": [name for name in names if name != server.token]}


def measured(server, out, callback):
    before = observed(server)
    save(out / "isolation-before.json", before)
    try:
        result = callback()
    finally:
        after = observed(server)
        save(out / "isolation-after.json", after)
    return result
