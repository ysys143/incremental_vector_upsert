import datetime
import json
from pathlib import Path
import subprocess
import sys
import time

from vectorbench.experiments.synthetic_insertion.contracts import load, save

root = Path("runs") / (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-parent-death")
p = subprocess.run([sys.executable, "-m", "scripts.study_parent_fault", str(root)], timeout=30)
if p.returncode != 9:
    raise AssertionError("parent death was not injected")
for _ in range(100):
    if (root / "watchdog-receipt.json").exists():
        receipt = load(root / "watchdog-receipt.json")
        if receipt["parent_alive"] or receipt["release"]["errors"] or len(receipt["release"]["released"]) != 2:
            raise AssertionError("independent parent-death cleanup failed")
        save(root / "verification.json", {"status": "PASS", "receipt": receipt})
        print(json.dumps({"root": str(root), "status": "PASS"}))
        break
    time.sleep(.1)
else:
    raise AssertionError("independent watchdog did not finish")
