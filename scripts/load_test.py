"""Load generator for the gateway(s): shows load balancing and autoscaling.

Uploads a set of small objects, then runs concurrent readers (and some
writers) for --duration seconds. Every 5 s it prints the request rate,
latency percentiles, status codes and WHICH GATEWAY answered (the
X-Served-By header), so you can see the load balancer spreading requests,
a gateway failing over, or new gateway pods appearing when the
HorizontalPodAutoscaler scales out.

  python scripts/load_test.py --url http://localhost:8080 --duration 60 --concurrency 16
"""

import argparse
import os
import random
import statistics
import threading
import time
from collections import Counter

import httpx

ADMIN_KEY = os.getenv("ADMIN_API_KEY", "dev-admin-key-change-me")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--objects", type=int, default=50)
    p.add_argument("--size", type=int, default=64 * 1024)
    p.add_argument("--write-ratio", type=float, default=0.1)
    p.add_argument("--rps-limit", type=float, default=100000,
                   help="rate limit of the test key (set low to see 429s)")
    a = p.parse_args()
    base = a.url.rstrip("/")

    with httpx.Client(base_url=base, headers={"X-API-Key": ADMIN_KEY},
                      timeout=60) as admin:
        key = admin.post("/admin/api-keys", json={
            "name": "load-test", "role": "readwrite",
            "rate_limit_rps": a.rps_limit}).json()
        bucket = f"loadtest-{int(time.time())}"
        admin.post("/buckets", json={"name": bucket, "redundancy": "replication"})
    print(f"bucket {bucket}, uploading {a.objects} objects of {a.size} bytes ...")

    headers = {"X-API-Key": key["api_key"]}
    with httpx.Client(base_url=base, headers=headers, timeout=60) as c:
        for i in range(a.objects):
            c.put(f"/buckets/{bucket}/objects/obj-{i}", content=os.urandom(a.size))

    lock = threading.Lock()
    stats = {"lat": [], "codes": Counter(), "served": Counter(), "n": 0}
    stop = time.time() + a.duration

    def worker(seed):
        rnd = random.Random(seed)
        with httpx.Client(base_url=base, headers=headers, timeout=60) as c:
            while time.time() < stop:
                i = rnd.randrange(a.objects)
                t0 = time.perf_counter()
                try:
                    if rnd.random() < a.write_ratio:
                        r = c.put(f"/buckets/{bucket}/objects/obj-{i}",
                                  content=os.urandom(a.size))
                    else:
                        r = c.get(f"/buckets/{bucket}/objects/obj-{i}")
                    code, served = r.status_code, r.headers.get("x-served-by", "?")
                except httpx.HTTPError:
                    code, served = "conn-error", "-"
                dt = time.perf_counter() - t0
                with lock:
                    stats["lat"].append(dt)
                    stats["codes"][code] += 1
                    stats["served"][served] += 1
                    stats["n"] += 1

    threads = [threading.Thread(target=worker, args=(s,), daemon=True)
               for s in range(a.concurrency)]
    for t in threads:
        t.start()
    print(f"running {a.concurrency} clients for {a.duration} s ...\n")
    last_n, last_t = 0, time.time()
    totals = Counter()
    while any(t.is_alive() for t in threads):
        time.sleep(5)
        with lock:
            lat, codes, served, n = (stats["lat"], dict(stats["codes"]),
                                     dict(stats["served"]), stats["n"])
            stats.update(lat=[], codes=Counter(), served=Counter())
        totals.update(served)
        now = time.time()
        rate = (n - last_n) / (now - last_t)
        last_n, last_t = n, now
        if lat:
            lat.sort()
            p50 = statistics.median(lat) * 1000
            p95 = lat[int(len(lat) * 0.95) - 1] * 1000
        else:
            p50 = p95 = 0
        print(f"{rate:7.1f} req/s  p50 {p50:6.1f} ms  p95 {p95:6.1f} ms  "
              f"codes {codes}  served by {served}")

    print(f"\nTotal requests: {stats['n']}")
    print("Requests per gateway:")
    for g, k in totals.most_common():
        print(f"  {g:30s} {k:7d}  ({100 * k / max(sum(totals.values()), 1):.1f} %)")
    with httpx.Client(base_url=base, headers={"X-API-Key": ADMIN_KEY}) as admin:
        admin.delete(f"/admin/api-keys/{key['key_id']}")
        admin.delete(f"/buckets/{bucket}", params={"force": "true"})


if __name__ == "__main__":
    main()
