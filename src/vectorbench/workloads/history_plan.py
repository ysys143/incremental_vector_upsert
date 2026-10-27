"""Compile declared source-row orders into real incremental events without building a DB."""
from dataclasses import dataclass

from vectorbench.workloads.events import Event, Live, Write, replacement_history


@dataclass(frozen=True)
class Recipe:
    operation: str
    waves: int
    ratio: int = 50
    long: bool = False
    hot: bool = False
    seed: int = 11
    policy: str = "settled"


def partition(values, waves):
    values = list(values)
    if waves < 1 or len(values) < waves or len(values) % waves:
        raise ValueError("declared wave sizes must be integral and nonempty; no hidden uneven splitting")
    chunk = len(values) // waves
    return [values[i*chunk:(i+1)*chunk] for i in range(waves)]


def compile_history(start, source_rows, recipe, categories=None, target_order=None, long_targets=None):
    """Order/geometry are upstream source selections, never synthetic vector transformations."""
    start = tuple(start)
    rows = list(map(int, source_rows))
    n = len(start)
    if not n or [w.id for w in start] != list(range(n)):
        raise ValueError("history initial IDs must be ordered contiguous IDs starting at zero")
    if recipe.policy not in {"settled", "continuous"}:
        raise ValueError("unknown observation policy")
    if recipe.long:
        if recipe.waves != 50 or recipe.operation in {"insert", "delete"} or recipe.ratio != 10:
            raise ValueError("long replacement history requires 50 rounds of initial N times 10 percent")
        if n % 10:
            raise ValueError("10 percent wave is not an integral count")
        events = replacement_history(start, rows, rounds=50, chunk=n//10,
            operation=recipe.operation, seed=recipe.seed, repeated=True, hot=recipe.hot,
            categories=categories, target_slots_by_wave=long_targets)
    else:
        if recipe.waves not in {1, 5} or recipe.hot or long_targets is not None:
            raise ValueError("short one/five comparison requires distinct target IDs")
        if recipe.ratio not in {10, 25, 50, 100} or n*recipe.ratio % 100:
            raise ValueError("unsupported or fractional short update ratio")
        if recipe.operation == "delete" and recipe.ratio == 100:
            raise ValueError("declared local delete-only study retains at least half the initial live set")
        total = n*recipe.ratio//100
        if recipe.operation == "insert":
            if len(rows) < total or len(set(rows[:total])) != total or set(rows[:total]) & {w.row for w in start}:
                raise ValueError("insert must add the declared number of distinct previously absent source rows")
            writes = [Write(n+i, row, 0, int(categories[row]) if categories is not None else 0, row)
                      for i, row in enumerate(rows[:total])]
            events = [Event(writes=tuple(w)) for w in partition(writes, recipe.waves)]
        else:
            if recipe.operation != "delete" and recipe.ratio != 50:
                raise ValueError("short replacement comparison is 10 percent times five versus one 50 percent wave")
            slots = None
            if target_order is not None:
                order = list(map(int, target_order))
                if len(order) < total or len(set(order[:total])) != total:
                    raise ValueError("target order does not supply enough distinct slots")
                slots = partition(order[:total], 5)
            if total % 5:
                raise ValueError("one/five comparison needs integral 5-way target partitions")
            events = replacement_history(start, rows, rounds=5, chunk=total//5,
                operation=recipe.operation, seed=recipe.seed, combine=recipe.waves == 1,
                categories=categories, target_slots_by_wave=slots)
    pool_size = max([w.row for w in start] + rows + [0])+1
    live = Live(start, pool_size)
    for event in events:
        live.apply(event)
    observed = list(range(len(events)+1)) if recipe.policy == "settled" else [0, len(events)]
    return {"events": events, "observed_steps": observed, "final": live,
            "counts": {"waves": len(events), "writes": sum(len(e.writes) for e in events),
                       "deletes": sum(len(e.deletes) for e in events), "final_count": len(live.current)},
            "scope": "COMPILED_HISTORY; no real data qualification or DB execution"}
