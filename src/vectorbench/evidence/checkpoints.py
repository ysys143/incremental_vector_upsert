"""One-owner checkpoint cache: share exact truth, preserve every distinct ledger version."""
import hashlib
from pathlib import Path
import numpy as np

from vectorbench.evidence.contracts import check_space, evidence_key, load, save, sha
from vectorbench.datasets.data import oracle
from vectorbench.evaluation.measure import metadata_arrays
from vectorbench.evaluation.metrics import state_hash
from vectorbench.datasets.storage import save_events, save_state


class Checkpoints:
    def __init__(self, pool, query_rows, out, config=None):
        self.pool, self.query_rows, self.out, self.config = pool, np.asarray(query_rows, dtype=np.int64), Path(out), config
        check_space(self.out.parent, config)
        self.out.mkdir(parents=True, exist_ok=False)
        self.query_sha = hashlib.sha256(np.asarray(pool.queries[self.query_rows], dtype="<f4").tobytes()).hexdigest()
        self.states, self.oracles = {}, {}

    def get(self, live):
        check_space(self.out, self.config, reserve=len(live.current)*20 + 65536)
        ledger = live.digest()
        if ledger not in self.states:
            path = self.out / "states" / (ledger + ".npz")
            info = save_state(path, live)
            self.states[ledger] = {"path": str(path), **info}
        state = self.states[ledger]
        if sha(state["path"]) != state["sha256"]:
            raise AssertionError("previously prepared live-set state changed")
        ids, rows, _ = metadata_arrays(live)
        vectors = state_hash(ids, self.pool.vectors[rows])
        # Oracle files bind source row maps as well as vector bytes. Preserve that distinction.
        mapped = hashlib.sha256(ids.astype("<i8").tobytes()+rows.astype("<i8").tobytes()).hexdigest()
        key = evidence_key(self.pool.base_sha, vectors+mapped, self.query_sha, "ip")
        if key not in self.oracles:
            path = self.out / "oracles" / (key + ".npz")
            path.parent.mkdir(parents=True, exist_ok=True)
            info = oracle(self.pool, live, self.query_rows, path, self.config)
            self.oracles[key] = {"path": str(path), "sha256": info["sha256"],
                                 "receipt_sha256": sha(path.with_suffix(path.suffix + ".receipt.json"))}
        control = self.oracles[key]
        if sha(control["path"]) != control["sha256"] or sha(Path(control["path"]).with_suffix(".npz.receipt.json")) != control["receipt_sha256"]:
            raise AssertionError("previously prepared oracle or its receipt changed")
        return {"state": state, "control": control, "current_ledger_sha256": ledger}

    def path(self, initial, compiled, out):
        """No embedding copies; continuous histories prepare only observed checkpoint truth."""
        from vectorbench.workloads.events import Live
        out = Path(out)
        out.mkdir(parents=True, exist_ok=False)
        save_events(out / "events.npz", compiled["events"])
        live = Live(initial, len(self.pool.vectors))
        controls, states = [None] * (len(compiled["events"])+1), {}
        for step in range(len(controls)):
            if step:
                live.apply(compiled["events"][step-1])
            if step in compiled["observed_steps"]:
                point = self.get(live)
                controls[step] = point["control"]
                states[str(step)] = point["state"]
        if live.digest() != compiled["final"].digest():
            raise AssertionError("compiled events and final live-set disagree")
        result = {"events": str(out / "events.npz"), "controls": controls, "states": states,
                  "observed_steps": compiled["observed_steps"], "counts": compiled["counts"],
                  "final_ledger_sha256": live.digest(), "scope": "PREPARED_INPUT_ONLY; DB NOT RUN"}
        save(out / "prepared.json", result)
        return result
