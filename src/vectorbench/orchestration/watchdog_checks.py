"""Kill an actual owner process; verify new watchdog removes only its tiny owned helper."""
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import uuid

from vectorbench.orchestration.synthetic_cleanup import release_journal
from vectorbench.evidence.contracts import load, save, sha
from vectorbench.evidence.registry import Registry


def parent(root, token):
    from vectorbench.orchestration.synthetic_cleanup import declare
    from vectorbench.experiments.pilot.run import IMAGES
    out = root / "cells" / "trial__a" / "parent-death"
    out.mkdir(parents=True)
    reg = Registry(root / "admission.sqlite")
    reg.claim("trial:a", "parent-death", "ORCHESTRATION_ONLY")
    reg.acquire("trial:a", "parent-death", "db", 0, 40 * 1024**3)
    volume = token + "-data"
    declare(out, "volume", volume, token)
    declare(out, "container", token, token)
    subprocess.run(["docker", "volume", "create", "--label", "ivu.owner=" + token, volume], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", token, "--label", "ivu.owner=" + token,
                    "--read-only", "--memory", "32m", "--cpus", ".1",
                    "--tmpfs", "/var/lib/postgresql/data:rw,size=1m", "-v", volume + ":/data",
                    "--entrypoint", "sleep", IMAGES["postgres"], "60"], check=True, capture_output=True)
    # Do not inherit the injected parent's captured stdout/stderr pipes: otherwise
    # communicate() waits for this child and cannot reap the already-dead owner.
    worker = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    save(out / "active-worker.json", {"pid": worker.pid})
    save(out / "watch.json", {"finished": False})
    log = (out / "watchdog.log").open("w")
    watcher = subprocess.Popen([sys.executable, "-m", "vectorbench.orchestration.watchdog", str(root), str(out),
        "--parent", str(os.getpid()), "--node", "trial:a", "--run-id", "parent-death",
        "--deadline", str(time.time()+20)], stdout=log, stderr=log, start_new_session=True)
    save(out / "started.json", {"parent": os.getpid(), "worker": worker.pid, "watchdog": watcher.pid,
                               "container": token, "volume": volume})
    os._exit(9)


def main():
    root = (Path("runs") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-watchdog-checks")).resolve()
    root.mkdir(parents=True)
    token = "ivu-watch-check-" + uuid.uuid4().hex[:10]
    out = root / "cells" / "trial__a" / "parent-death"
    try:
        p = subprocess.run([sys.executable, "-m", "vectorbench.orchestration.watchdog_checks", "--parent", str(root), token],
                           capture_output=True, text=True, timeout=20)
        (root / "parent-stdout.log").write_text(p.stdout)
        (root / "parent-stderr.log").write_text(p.stderr)
        if p.returncode != 9:
            raise AssertionError("injected owner exit did not reach its declared boundary")
        deadline = time.time()+15
        while not (out / "watchdog-receipt.json").exists():
            if time.time() > deadline:
                raise TimeoutError("independent watchdog did not reconcile owner death")
            time.sleep(.1)
        receipt = load(out / "watchdog-receipt.json")
        if receipt["parent_alive"] or receipt["status"] != "STOPPED" or receipt["cleanup"].get("errors"):
            raise AssertionError("watchdog did not stop and clean after actual owner death")
        for args in (["docker", "inspect", token], ["docker", "volume", "inspect", token + "-data"]):
            probe = subprocess.run(args, capture_output=True, text=True, timeout=10)
            if probe.returncode == 0 or not any(s in probe.stderr.lower() for s in ("no such", "not found")):
                raise AssertionError("owned cleanup was absent or unverified")
        if Registry(root / "admission.sqlite").snapshot()["leases"]:
            raise AssertionError("dead owner left a stale resource lease")
        save(root / "receipt.json", {"status": "PASS", "scope": "ORCHESTRATION_ONLY; sleeping helper, no database process or data",
             "checks": ["actual owner exited 9", "independent watchdog observed parent death",
                        "owned container and named empty volume absent before fallback cleanup", "lease reconciled"],
             "code_sha256": sha(Path(__file__)), "watchdog": receipt})
    except BaseException:
        save(root / "failure.json", {"status": "FAIL", "traceback": traceback.format_exc()})
        raise
    finally:
        if (out / "resources.json").exists():
            save(root / "final-idempotent-cleanup.json", release_journal(out))
    print(root / "receipt.json")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--parent":
        parent(Path(sys.argv[2]), sys.argv[3])
    else:
        main()
