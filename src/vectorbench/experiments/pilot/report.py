"""Verify raw receipts before summarizing; missing work never becomes PASS."""
import json
import hashlib
from pathlib import Path
import sys

import numpy as np

from vectorbench.experiments.pilot.model import Config, digest, history, score, unpacked_results, validate_truth
from vectorbench.backends.pilot import assert_postgres_ann


def load(path):
    return json.loads(path.read_text())


def validate_run(root):
    root = Path(root)
    manifest = load(root / "manifest.json")
    c = manifest["config"]
    config = Config(**{**c, "seeds": tuple(c["seeds"])})
    total = 0
    for seed in config.seeds:
        seed_root = root / f"seed-{seed}"
        with np.load(seed_root / "fixture.npz") as f:
            pool, queries = f["pool"], f["evaluation"]
        for db in ("postgres", "qdrant"):
            db_root = seed_root / db
            if not load(db_root / "insert" / "restart.json")["passed"]:
                raise AssertionError("restart failed")
            if load(db_root / "cleanup.json")["errors"]:
                raise AssertionError("cleanup failed")
            choice = load(db_root / "chosen.json")
            if min(choice["quality"]["recall10"], choice["quality"]["recall100"]) < .95:
                raise AssertionError("chosen tuning target not achieved")
            for scenario in ("insert", "delete", "churn"):
                run_root = db_root / scenario
                identity = None
                for step, (ids, deleted, inserted) in enumerate(history(config, seed, scenario)):
                    receipt = load(run_root / f"checkpoint-{step}.json")
                    if receipt["event"] != {"delete": deleted, "insert": inserted}:
                        raise AssertionError("event differs from deterministic history")
                    if receipt["alive_hash"] != digest(ids, pool[ids]):
                        raise AssertionError("alive state hash mismatch")
                    if receipt["raw_sha256"] != hashlib.sha256((run_root / f"checkpoint-{step}.npz").read_bytes()).hexdigest():
                        raise AssertionError("raw artifact hash mismatch")
                    with np.load(run_root / f"checkpoint-{step}.npz") as raw:
                        np.testing.assert_array_equal(raw["alive_ids"], ids)
                        validate_truth(ids, pool[ids], queries, raw["truth"])
                        quality = score(ids, raw["truth"], unpacked_results(raw))
                        for key, value in quality.items():
                            if not np.isclose(receipt[key], value, rtol=0, atol=1e-12):
                                raise AssertionError("summary differs from raw results")
                        if len(raw["latency_ms"]) != len(queries):
                            raise AssertionError("latency observations missing")
                    if db == "postgres":
                        assert_postgres_ann(receipt["path"], receipt["state"].get("enable_seqscan"), "items_hnsw", analyzed=True)
                        current = (receipt["state"]["table_oid"], receipt["state"]["index_oid"])
                        if identity is not None and identity != current:
                            raise AssertionError("incremental table/index identity changed")
                        identity = current
                    total += 1
                done = load(run_root / "done.json")
                if not done["passed"] or done["checkpoints"] != config.rounds + 1:
                    raise AssertionError("history not finished")
                control = load(run_root / "checkpoint-control.json")
                if control["alive_hash"] != receipt["alive_hash"]:
                    raise AssertionError("control data mismatch")
                if db == "postgres":
                    assert_postgres_ann(control["path"], control["state"].get("enable_seqscan"), "control_hnsw", analyzed=True)
                if control["raw_sha256"] != hashlib.sha256((run_root / "checkpoint-control.npz").read_bytes()).hexdigest():
                    raise AssertionError("control artifact hash mismatch")
                with np.load(run_root / "checkpoint-control.npz") as raw:
                    np.testing.assert_array_equal(raw["alive_ids"], ids)
                    validate_truth(ids, pool[ids], queries, raw["truth"])
                    quality = score(ids, raw["truth"], unpacked_results(raw))
                    for key, value in quality.items():
                        if not np.isclose(control[key], value, rtol=0, atol=1e-12):
                            raise AssertionError("control raw results mismatch")
    if total != manifest["planned_checkpoints"]:
        raise AssertionError(f"checkpoints {total} != {manifest['planned_checkpoints']}")
    if manifest["completed_histories"] != manifest["planned_histories"]:
        raise AssertionError("histories missing")
    return total


def report(root):
    root = Path(root)
    manifest = load(root / "manifest.json")
    checkpoints = list(root.glob("seed-*/*/*/checkpoint-[0-9]*.json"))
    controls = list(root.glob("seed-*/*/*/checkpoint-control.json"))
    done = list(root.glob("seed-*/*/*/done.json"))
    lines = [f"# {manifest['id']}", "", f"상태: **{manifest['status']}**", "",
             f"- 이력: {len(done)}/{manifest['planned_histories']}",
             f"- 증분 checkpoint: {len(checkpoints)}/{manifest['planned_checkpoints']}",
             f"- 재구축 대조군: {len(controls)}/{manifest['planned_histories']}",
             f"- 실행 시간: {manifest.get('elapsed_s', 0):.1f}초", "",
             "원본 float32 · 합성 데이터 · batch 증분 · 단일 동시성 · 공유 Mac/Colima.",
             "품질은 exact oracle 기준이며 지연은 서로 다른 클라이언트 경로를 포함한다.",
             "공유 ChatGPT 대화는 ego-browser 시작 실패로 미확인. WDL 프로젝트 검사 상태는 manifest 참조.", ""]
    if "first_error" in manifest:
        lines += ["## 첫 오류", "", "```text", manifest["first_error"], "```", "",
                  "실패 전 부분 결과이며 후속 단계의 전제는 충족되지 않았다.", ""]
    lines += ["## seed별 최종 결과", "",
              "| DB | seed | 시나리오 | 최종 R@10 | 최종 R@100 | 재구축 대비 ΔR@100 | p95 ms | 대기 조건 충족 |",
              "|---|---:|---|---:|---:|---:|---:|---|"]
    for path in sorted(done):
        info = load(path)
        final = load(path.parent / f"checkpoint-{manifest['config']['rounds']}.json")
        db = path.parent.parent.name
        seed = path.parent.parent.parent.name.split("-")[1]
        lines.append(f"| {db} | {seed} | {path.parent.name} | {final['recall10']:.4f} | {final['recall100']:.4f} | {info['delta_recall100']:+.4f} | {final['p95_ms']:.3f} | {final['maintenance']['settled']} |")
    lines += ["", "대기 조건은 DB별로 다르다. PostgreSQL의 True는 commit 완료이며 VACUUM 완료가 아니다. Qdrant는 optimizer/indexing 상태 조건을 뜻한다.",
              "", "## 해석 및 재현", "",
              "차이가 작거나 없더라도 대규모·장기 업데이트의 동등성으로 해석하지 않는다.",
              "PostgreSQL은 강제 HNSW 계획, Qdrant는 HNSW 설정과 인덱싱 카운터를 확인한다.",
              "Qdrant 내부 정리·segment 교체와 미인덱싱 tail은 원시 maintenance 상태를 함께 확인한다.",
              "각 checkpoint NPZ에 생존 ID·exact top-100·응답 ID·개별 지연이 보존된다.", "",
              "```sh", f"python3 -m vectorbench.experiments.pilot.run --stage {manifest['stage']}",
              f"python3 -m vectorbench.experiments.pilot.report {root}", "```", "",
              "동일 명령도 새 실행 ID를 생성한다. 실패 이력과 부분 로그는 덮어쓰지 않는다."]
    (root / "REPORT.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    target = Path(sys.argv[1])
    validate_run(target)
    report(target)
    print(target / "REPORT.md")
