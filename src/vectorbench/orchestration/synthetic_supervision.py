"""A separate process enforces deadlines and releases owned resources after parent failure."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import time

from vectorbench.orchestration.synthetic_cleanup import release_journal
from vectorbench.experiments.synthetic_insertion.contracts import load, save


def kill_group(pid, process=None):
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    if process is not None:
        try:
            process.wait(timeout=2)
            return
        except subprocess.TimeoutExpired:
            pass
    else:
        time.sleep(.2)
        state = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True)
        if state.returncode or not state.stdout.strip() or state.stdout.strip().startswith("Z"):
            return
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def supervised(command, out, seconds):
    out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    result = {"command": command, "started": started, "timeout_s": seconds}
    with open(out / "stdout.log", "w") as stdout, open(out / "stderr.log", "w") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
        save(out / "active-worker.json", {"pid": process.pid, "deadline": started + seconds})
        try:
            result["returncode"] = process.wait(timeout=seconds)
            result["timed_out"] = False
        except subprocess.TimeoutExpired as exc:
            result.update(timed_out=True, first_error=repr(exc))
            kill_group(process.pid, process)
            process.wait(timeout=10)
            result.update(returncode=process.returncode, timed_out=True)
        except BaseException as exc:
            result["first_error"] = repr(exc)
            kill_group(process.pid, process)
            process.wait(timeout=10)
            raise
        finally:
            result["elapsed_s"] = time.time() - started
            try:
                result["cleanup"] = release_journal(out)
            except Exception as exc:
                result["cleanup"] = {"errors": [repr(exc)], "released": [], "absent": []}
            save(out / "supervision.json", result)
            save(out / "active-worker.json", {"pid": None, "finished": time.time()})
    return result


def watchdog(root, parent):
    state_path = root / "controller.json"
    while True:
        try:
            os.kill(parent, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        state = load(state_path) if state_path.exists() else {}
        if state.get("finished"):
            return
        exceeded = state.get("deadline") is not None and time.time() >= state["deadline"]
        if not alive or exceeded:
            if state.get("out"):
                out = Path(state["out"])
                active = load(out / "active-worker.json") if (out / "active-worker.json").exists() else {}
                if active.get("pid"):
                    kill_group(active["pid"])
                try:
                    release = release_journal(out)
                except Exception as exc:
                    release = {"errors": [repr(exc)], "released": [], "absent": []}
            else:
                release = {"errors": [], "released": [], "absent": []}
            save(root / "watchdog-receipt.json", {"parent_alive": alive, "deadline_exceeded": exceeded,
                 "time": time.time(), "release": release, "status": "STOPPED"})
            if alive:
                os.kill(parent, signal.SIGTERM)
            return
        time.sleep(.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--parent", type=int, required=True)
    args = parser.parse_args()
    watchdog(args.root, args.parent)
