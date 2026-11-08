"""Replication vs erasure coding: storage overhead and repair traffic.

For each workload (small files, large files):
  1. create two buckets: one replicated (N copies), one erasure-coded (k+m);
  2. upload the same files into both and measure throughput;
  3. read each bucket's storage overhead (raw bytes on disk / unique bytes);
  4. stop one storage node, wait until the repair job has rebuilt everything,
     and record the repair traffic of each scheme (bytes read + written);
  5. download a sample while the node is down (degraded reads);
  6. start the node again.

Usage (cluster running with Docker Compose):
  python scripts/redundancy_benchmark.py --small datasets/small --large datasets/large
  python scripts/redundancy_benchmark.py --small datasets/small --small-limit 1000

The node is stopped / started with `docker compose stop|start storage-node-<n>`
(change with --stop-cmd / --start-cmd; {n} is the node number).
"""

import argparse
import hashlib
import json
import random
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import httpx

TOKEN = {"X-Cluster-Token": "dev-cluster-token-change-me"}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


class Cluster:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.c = httpx.Client(timeout=600)

    def get(self, path, **kw):
        r = self.c.get(self.url + path, **kw)
        r.raise_for_status()
        return r.json()

    def post(self, path, **kw):
        r = self.c.post(self.url + path, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path}: {r.status_code} {r.text[:300]}")
        return r.json()


def upload_all(url, bucket, files, workers, chunking):
    def one(path):
        with httpx.Client(timeout=3600) as c, path.open("rb") as f:
            r = c.post(f"{url}/v1/files",
                       files={"file": (path.name, f, "application/octet-stream")},
                       data={"bucket": bucket, "key": path.name,
                             "chunking": chunking})
            r.raise_for_status()
            return r.json()["file_id"], path

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(one, files))
    return results, time.perf_counter() - start


def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def degraded_reads(url, uploaded, sample):
    ok = 0
    picks = random.Random(1).sample(uploaded, min(sample, len(uploaded)))
    start = time.perf_counter()
    with httpx.Client(timeout=3600) as c:
        for fid, path in picks:
            h = hashlib.sha256()
            with c.stream("GET", f"{url}/v1/files/{fid}/download") as r:
                if r.status_code != 200:
                    continue
                for block in r.iter_bytes():
                    h.update(block)
            ok += h.hexdigest() == sha256_file(path)
    return {"sampled": len(picks), "verified_ok": ok,
            "seconds": round(time.perf_counter() - start, 2)}


def wait_for(cond, timeout, every=2.0):
    end = time.time() + timeout
    while time.time() < end:
        value = cond()
        if value:
            return value
        time.sleep(every)
    return None


def run_workload(cl, args, name, directory, limit):
    files = sorted(p for p in Path(directory).rglob("*") if p.is_file())
    if limit:
        files = files[:limit]
    if not files:
        log(f"{name}: no files in {directory}, skipped")
        return None
    stamp = datetime.now().strftime("%H%M%S")
    buckets = {
        "replication": f"bench-{name}-rep-{stamp}",
        "ec": f"bench-{name}-ec-{stamp}",
    }
    cl.post("/v1/buckets", json={"name": buckets["replication"],
                                 "redundancy": "replication"})
    cl.post("/v1/buckets", json={"name": buckets["ec"], "redundancy": "ec",
                                 "k": args.k, "m": args.m})

    total_bytes = sum(p.stat().st_size for p in files)
    result = {"workload": name, "files": len(files),
              "logical_MiB": round(total_bytes / 2**20, 2), "schemes": {}}
    uploaded = {}
    for scheme, bucket in buckets.items():
        log(f"{name}: uploading {len(files)} files to {bucket}")
        ids, seconds = upload_all(cl.url, bucket, files, args.workers,
                                  args.chunking)
        uploaded[scheme] = ids
        stats = cl.get(f"/v1/buckets/{bucket}/stats")
        result["schemes"][scheme] = {
            "bucket": bucket,
            "policy": stats["bucket"],
            "upload_seconds": round(seconds, 2),
            "upload_MiB_s": round(total_bytes / 2**20 / seconds, 2),
            "files_per_s": round(len(files) / seconds, 1),
            "unique_MiB": round(stats["unique_bytes"] / 2**20, 2),
            "raw_MiB_on_disk": round(stats["raw_bytes_on_disk"] / 2**20, 2),
            "storage_overhead": stats["storage_overhead"],
        }

    # ---- failure and repair ------------------------------------------------
    node = args.fail_node
    n = node.removeprefix("node")
    before = cl.get("/v1/repair/status")["totals"]
    log(f"{name}: stopping {node}")
    t_fail = time.time()
    subprocess.run(shlex.split(args.stop_cmd.format(n=n)), check=False)

    detected = wait_for(lambda: node in cl.get("/v1/repair/status")["failed_nodes"],
                        timeout=120, every=1)
    t_detect = time.time()
    log(f"{name}: failure detected after {t_detect - t_fail:.1f}s")

    result["degraded_reads"] = {
        scheme: degraded_reads(cl.url, ids, args.read_sample)
        for scheme, ids in uploaded.items()}

    def repaired():
        s = cl.get("/v1/cluster/status")
        return (s["chunks_degraded"] == 0 and s["repair"]["state"] == "idle"
                and s)
    done = wait_for(repaired, timeout=args.repair_timeout)
    t_repaired = time.time()
    after = cl.get("/v1/repair/status")["totals"]
    log(f"{name}: repair finished after {t_repaired - t_detect:.1f}s")

    result["failure"] = {
        "node": node,
        "detected": bool(detected),
        "detection_seconds": round(t_detect - t_fail, 1),
        "repair_seconds": round(t_repaired - t_detect, 1),
        "fully_repaired": bool(done),
    }
    for scheme in ("replication", "ec"):
        d = {k: after[scheme][k] - before[scheme][k] for k in after[scheme]}
        traffic = d["bytes_read"] + d["bytes_written"]
        result["schemes"][scheme]["repair"] = {
            **d,
            "traffic_MiB": round(traffic / 2**20, 2),
            "read_per_rebuilt_byte": round(d["bytes_read"] / d["bytes_written"], 2)
            if d["bytes_written"] else None,
        }

    log(f"{name}: starting {node} again")
    subprocess.run(shlex.split(args.start_cmd.format(n=n)), check=False)
    wait_for(lambda: node not in cl.get("/v1/repair/status")["failed_nodes"],
             timeout=120, every=1)
    return result


def to_markdown(results, k, m, n):
    lines = [
        f"Policies: replication N={n} | erasure coding k={k}, m={m}",
        "",
        "| workload | scheme | files | logical MiB | raw MiB on disk | overhead "
        "| upload MiB/s | files/s | repair read MiB | repair written MiB "
        "| repair traffic MiB | read per rebuilt byte |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        for scheme, s in r["schemes"].items():
            rep = s["repair"]
            lines.append(
                f"| {r['workload']} | {scheme} | {r['files']} | {r['logical_MiB']} "
                f"| {s['raw_MiB_on_disk']} | {s['storage_overhead']} "
                f"| {s['upload_MiB_s']} | {s['files_per_s']} "
                f"| {round(rep['bytes_read'] / 2**20, 2)} "
                f"| {round(rep['bytes_written'] / 2**20, 2)} "
                f"| {rep['traffic_MiB']} | {rep['read_per_rebuilt_byte']} |")
    lines += ["", "Theory: replication overhead = N, repair reads 1 byte per byte "
              f"rebuilt; EC overhead = (k+m)/k = {(k + m) / k:.2f}, repair reads "
              f"k = {k} bytes per byte rebuilt."]
    for r in results:
        f = r["failure"]
        lines.append(f"\n{r['workload']}: {f['node']} failure detected in "
                     f"{f['detection_seconds']} s, repaired in "
                     f"{f['repair_seconds']} s, degraded reads: "
                     + ", ".join(f"{s} {v['verified_ok']}/{v['sampled']} OK"
                                 for s, v in r["degraded_reads"].items()))
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--small")
    p.add_argument("--large")
    p.add_argument("--small-limit", type=int, default=0)
    p.add_argument("--large-limit", type=int, default=0)
    p.add_argument("--k", type=int, default=4)
    p.add_argument("--m", type=int, default=2)
    p.add_argument("--chunking", default="fixed", choices=["fixed", "cdc"])
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--fail-node", default="node4")
    p.add_argument("--stop-cmd", default="docker compose stop storage-node-{n}")
    p.add_argument("--start-cmd", default="docker compose start storage-node-{n}")
    p.add_argument("--read-sample", type=int, default=20)
    p.add_argument("--repair-timeout", type=float, default=1800)
    p.add_argument("--out", default="results/redundancy_benchmark")
    a = p.parse_args()
    if not (a.small or a.large):
        sys.exit("Give --small and/or --large")

    cl = Cluster(a.url)
    health = cl.get("/health")
    results = []
    for name, directory, limit in (("small", a.small, a.small_limit),
                                   ("large", a.large, a.large_limit)):
        if directory:
            r = run_workload(cl, a, name, directory, limit)
            if r:
                results.append(r)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(
        {"cluster": health, "results": results}, indent=2))
    md = to_markdown(results, a.k, a.m, health["replication_factor"])
    out.with_suffix(".md").write_text(md)
    print("\n" + md)
    log(f"saved {out}.json and {out}.md")


if __name__ == "__main__":
    main()
