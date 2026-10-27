"""Bounded public fbin reads, with a Docker DNS fallback preserving hostname/TLS."""
import argparse
from dataclasses import dataclass
import ipaddress
import json
import os
from pathlib import Path
import random
import socket
import struct
import subprocess
import time
from urllib.parse import urlsplit
import uuid

from vectorbench.evidence.contracts import Config, check_space, load, save, sha


RESOLVER_IMAGE = "pgvector/pgvector@sha256:7a7e9f22015b67edb4bef5c59daeebcd7e74bfa570df6ce60ae01237c8648a84"


@dataclass(frozen=True)
class Dataset:
    name: str
    base: str
    queries: str
    dimension: int = 768
    metric: str = "ip"
    dtype: str = "<f4"


DATASETS = {
    "msmarco": Dataset("msmarco",
        "https://msmarco.z22.web.core.windows.net/msmarcowebsearch/vectors/SimANS/passage_vectors/vectors.bin",
        "https://msmarco.z22.web.core.windows.net/msmarcowebsearch/vectors/SimANS/query_vectors/vectors.bin"),
    "wikipedia": Dataset("wikipedia",
        "https://comp21storage.z5.web.core.windows.net/wiki-cohere-35M/wikipedia_base.bin",
        "https://comp21storage.z5.web.core.windows.net/wiki-cohere-35M/wikipedia_query.bin"),
}


def resolve(host):
    try:
        addresses = sorted({x[4][0] for x in socket.getaddrinfo(host, 443, socket.AF_INET)})
        route = "host-resolver"
    except OSError as exc:
        token = "ivu-dns-" + uuid.uuid4().hex[:12]
        if os.environ.get("REAL_STUDY_TASK_OUT"):
            from vectorbench.orchestration.synthetic_cleanup import declare
            declare(Path(os.environ["REAL_STUDY_TASK_OUT"]), "container", token, token)
        args = ["docker", "run", "--rm", "--name", token, "--label", "ivu.owner=" + token,
                "--cpus", "1", "--memory", "128m", "--read-only",
                "--tmpfs", "/var/lib/postgresql/data:rw,size=1m", "--entrypoint", "getent",
                RESOLVER_IMAGE, "ahostsv4", host]
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=20)
            if p.returncode:
                raise RuntimeError("Docker DNS lookup failed: " + p.stderr[:300]) from exc
            addresses = sorted({line.split()[0] for line in p.stdout.splitlines() if line.split()})
            route = "docker-resolver; hostname and TLS certificate validation preserved"
        finally:
            # Only this exact newly owned helper; --rm normally already removed it.
            inspection = subprocess.run(["docker", "inspect", "--format", '{{index .Config.Labels "ivu.owner"}}', token],
                                        capture_output=True, text=True, timeout=10)
            if inspection.returncode == 0 and inspection.stdout.strip() == token:
                subprocess.run(["docker", "rm", "-f", "-v", token], capture_output=True, timeout=15, check=True)
    if not addresses:
        raise RuntimeError("no IPv4 address returned for " + host)
    for address in addresses:
        ipaddress.IPv4Address(address)
    return {"host": host, "addresses": addresses, "route": route, "time": time.time()}


def curl_args(url, resolution):
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname != resolution["host"]:
        raise ValueError("resolution does not belong to this HTTPS URL")
    return ["curl", "--silent", "--show-error", "--fail", "--proto", "=https",
            "--connect-timeout", "15", "--max-time", "120", "--resolve",
            f"{parts.hostname}:443:{resolution['addresses'][0]}"]


def headers(raw):
    blocks = raw.replace("\r\n", "\n").strip().split("\n\n")
    block = next((b for b in reversed(blocks) if b.startswith("HTTP/")), "")
    if not block:
        raise ValueError("missing HTTP status/headers")
    lines = block.splitlines()
    return int(lines[0].split()[1]), {k.lower().strip(): v.strip() for k, v in
        (line.split(":", 1) for line in lines[1:] if ":" in line)}


def probe(url, dimension=768):
    resolution = resolve(urlsplit(url).hostname)
    p = subprocess.run([*curl_args(url, resolution), "--head", url], capture_output=True, timeout=130)
    if p.returncode:
        raise RuntimeError("HEAD failed: " + p.stderr.decode(errors="replace")[:300])
    status, h = headers(p.stdout.decode())
    if status != 200 or h.get("accept-ranges", "").lower() != "bytes" or not h.get("etag"):
        raise ValueError("public source requires a pinned ETag and byte ranges")
    p = subprocess.run([*curl_args(url, resolution), "--range", "0-15", "--max-filesize", "16",
                        "--header", "If-Match: " + h["etag"], url], capture_output=True, timeout=130)
    if p.returncode or len(p.stdout) != 16:
        raise ValueError("bounded source header read failed: " + p.stderr.decode(errors="replace")[:300])
    rows, d = struct.unpack("<II", p.stdout[:8])
    if d != dimension or rows < 1 or int(h["content-length"]) != 8 + rows * d * 4:
        raise ValueError("source fbin header/length/dimension mismatch")
    return {"url": url, "rows": rows, "dimension": d, "dtype": "<f4", "metric": "ip",
            "etag": h["etag"], "content_length": int(h["content-length"]),
            "last_modified": h.get("last-modified"), "resolution": resolution}


def ranges(total, count, blocks, seed=1729):
    """Stratified contiguous blocks, not a biased single prefix or 100k tiny requests."""
    if not 0 < count <= total or not 0 < blocks <= count:
        raise ValueError("invalid source sample")
    rng = random.Random(seed)
    result = []
    for i in range(blocks):
        lo, hi = total * i // blocks, total * (i + 1) // blocks
        n = count // blocks + (i < count % blocks)
        if hi - lo < n:
            raise ValueError("stratum cannot hold this block")
        start = rng.randrange(lo, hi - n + 1)
        result.append((start, n))
    return result


def download_range(source, first, size, out):
    """Bounded idempotent GET retries; every failed response is retained separately."""
    expected = f"bytes {first}-{first+size-1}/{source['content_length']}"
    for attempt in (1, 2, 3):
        payload = out.with_name(out.name + f".attempt-{attempt}")
        head = payload.with_suffix(payload.suffix + ".headers")
        command = [*curl_args(source["url"], source["resolution"]), "--range", f"{first}-{first+size-1}",
                   "--max-filesize", str(size), "--header", "If-Match: " + source["etag"],
                   "--dump-header", str(head), "--output", str(payload), source["url"]]
        p = subprocess.run(command, capture_output=True, timeout=130)
        status, h = headers(head.read_text()) if head.exists() and head.stat().st_size else (0, {})
        actual = payload.stat().st_size if payload.exists() else 0
        info = {"attempt": attempt, "returncode": p.returncode, "stderr": p.stderr.decode(errors="replace"),
                "http_status": status, "expected_range": expected, "actual_range": h.get("content-range"),
                "expected_bytes": size, "actual_bytes": actual, "etag": h.get("etag")}
        save(payload.with_suffix(payload.suffix + ".json"), info)
        if p.returncode == 0 and status == 206 and h.get("content-range") == expected and h.get("etag") == source["etag"] and actual == size:
            return payload
        if status == 206 and (h.get("content-range") != expected or h.get("etag") != source["etag"]):
            raise ValueError("source identity/range changed; no retry: " + str(info))
        transient = p.returncode in {18, 28, 52, 55, 56} or status in {429, 500, 502, 503, 504}
        if not transient or attempt == 3:
            raise ValueError("bounded source read failed; all attempt evidence retained: " + str(info))
        time.sleep(attempt)
    raise AssertionError("unreachable")


def adopt_complete(source, cache, out, dataset):
    """A new producer validates raw cached bytes; failed old runs are never called SUCCESS."""
    from vectorbench.datasets.data import verified_file
    path = Path(cache["path"])
    receipt = path.with_suffix(path.suffix + ".receipt.json")
    if sha(path) != cache["sha256"] or sha(receipt) != cache["receipt_sha256"]:
        raise AssertionError("declared source cache changed")
    array, _, prior = verified_file(path, dataset, "base")
    if len(array) != Config().pool_rows or any(prior["source"][k] != source[k] for k in ("url", "etag", "rows", "dimension", "content_length")):
        raise AssertionError("cached base belongs to another source version")
    del array
    out = Path(out)
    if out.exists() or out.with_suffix(out.suffix + ".receipt.json").exists():
        raise FileExistsError(out)
    os.link(path, out)
    result = {**prior, "source": source, "adopted_raw_cache": cache,
              "adoption": "new native producer independently verified source bytes and every block"}
    save(out.with_suffix(out.suffix + ".receipt.json"), result)
    return result


def fetch(source, out, count, blocks, config=None):
    """Write a sampled fbin once; retain partial output on failure, never silently retry."""
    c = config or Config()
    out = Path(out)
    check_space(out.parent, c, reserve=8 + count * source["dimension"] * 4)
    out.parent.mkdir(parents=True, exist_ok=True)
    record = out.with_suffix(out.suffix + ".receipt.json")
    if out.exists() or record.exists():
        raise FileExistsError("source output already exists; inspect its original receipt")
    selected = ranges(source["rows"], count, blocks)
    items = []
    with out.open("xb") as stream:
        stream.write(struct.pack("<II", count, source["dimension"]))
    save(record, {"status": "RUNNING", "source": source, "blocks": items})
    try:
        for number, (start, n) in enumerate(selected):
            size = n * source["dimension"] * 4
            check_space(out.parent, c, reserve=size)
            first = 8 + start * source["dimension"] * 4
            temporary = out.with_name(out.name + f".block-{number}")
            expected_range = f"bytes {first}-{first + size - 1}/{source['content_length']}"
            # HTTP transfer pieces do not change the declared 32 sampled source blocks.
            with temporary.open("xb") as stream:
                for offset in range(0, size, 1024**2):
                    take = min(1024**2, size-offset)
                    piece = download_range(source, first+offset, take, temporary.with_name(temporary.name+f".piece-{offset}"))
                    with piece.open("rb") as item:
                        stream.write(item.read())
                    piece.unlink()  # Successful transfer cache only; failed attempts remain.
            digest = sha(temporary)
            with out.open("ab") as dest, temporary.open("rb") as block:
                while data := block.read(1024 * 1024):
                    dest.write(data)
            items.append({"source_row": start, "rows": n, "sha256": digest, "content_range": expected_range})
            save(record, {"status": "RUNNING", "source": source, "blocks": items})
            temporary.unlink()  # Reproducible transfer cache; original sampled bytes remain in out.
        save(record, {"status": "PASS", "source": source, "rows": count, "blocks": items,
                      "sample_rule": "stratified contiguous blocks, seed=1729", "sha256": sha(out)})
    except BaseException as exc:
        save(record, {"status": "FAIL", "source": source, "blocks": items, "error": repr(exc)})
        raise
    return load(record)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=DATASETS, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fetch", action="store_true")
    args = parser.parse_args()
    dataset = DATASETS[args.dataset]
    if (args.out / "probe.json").exists():
        if not args.fetch:
            raise FileExistsError("probe evidence already exists; use a new output directory")
        result = load(args.out / "probe.json")
        if result["dataset"] != dataset.name or result["base"]["url"] != dataset.base or result["queries"]["url"] != dataset.queries:
            raise AssertionError("existing source probe belongs to another dataset")
    else:
        result = {"dataset": dataset.name, "data_kind": "real-precomputed", "metric": dataset.metric,
                  "base": probe(dataset.base), "queries": probe(dataset.queries)}
        save(args.out / "probe.json", result)
    if args.fetch:
        fetch(result["base"], args.out / "base.fbin", Config().pool_rows, Config().source_blocks)
        fetch(result["queries"], args.out / "queries.fbin", result["queries"]["rows"], 1)
    print(json.dumps(result, indent=2))
