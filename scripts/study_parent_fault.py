"""Disposable controller that dies after its owned fake workload starts."""
import os
from pathlib import Path
import subprocess
import sys
import time

from vectorbench.experiments.synthetic_insertion.contracts import save

root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True)
out = root / "worker"
out.mkdir()
stdout = open(out / "stdout.log", "w")
worker = subprocess.Popen([sys.executable, "-u", "-m", "scripts.study_resource_fault", str(out), "timeout"],
                          stdout=stdout, stderr=stdout, start_new_session=True)
save(out / "active-worker.json", {"pid": worker.pid})
save(root / "controller.json", {"pid": os.getpid(), "deadline": time.time() + 60,
                               "out": str(out), "finished": False})
log = open(root / "watchdog.log", "w")
subprocess.Popen([sys.executable, "-m", "vectorbench.orchestration.synthetic_supervision", str(root), "--parent", str(os.getpid())],
                 stdout=log, stderr=log, start_new_session=True)
for _ in range(100):
    stdout.flush()
    if "owned fake resource ready" in (out / "stdout.log").read_text():
        os._exit(9)
    time.sleep(.1)
raise RuntimeError("fake resource failed to become ready")
