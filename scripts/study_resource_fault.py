"""Only called by the orchestration fault-injection check, on newly owned resources."""
import argparse
from pathlib import Path
import subprocess
import time
import uuid

from vectorbench.experiments.pilot.run import IMAGES, docker
from vectorbench.orchestration.synthetic_cleanup import declare

parser = argparse.ArgumentParser()
parser.add_argument("out", type=Path)
parser.add_argument("mode", choices=("failure", "timeout"))
args = parser.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
token = "ivu-fault-" + uuid.uuid4().hex[:10]
declare(args.out, "volume", token + "-data", token)
declare(args.out, "container", token, token)
docker("volume", "create", "--label", "ivu.owner=" + token, token + "-data")
docker("run", "-d", "--name", token, "--label", "ivu.owner=" + token,
       "--cpus", ".25", "--memory", "64m", "-v", token + "-data:/data",
       "--entrypoint", "sleep", IMAGES["postgres"], "600")
print("owned fake resource ready: " + token, flush=True)
if args.mode == "failure":
    raise SystemExit(7)
time.sleep(600)
