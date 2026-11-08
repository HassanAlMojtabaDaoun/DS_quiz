"""Measure how many chunks move when a node joins or leaves.

Two modes:
  * synthetic (default): random SHA-256 addresses, pure computation;
  * --url: asks the running coordinator to simulate the change on the
    real chunk catalog (GET /v1/placement/simulate).

Both compare consistent hashing (with virtual nodes) against the naive
hash % N baseline.

Usage:
  python scripts/placement_experiment.py --chunks 100000
  python scripts/placement_experiment.py --url http://localhost:8000
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from coordinator.ring import ConsistentHashRing, modulo_placement  # noqa: E402


def compare(hashes, before, after, n, vnodes):
    rb = ConsistentHashRing(before, vnodes)
    ra = ConsistentHashRing(after, vnodes)
    ch = mod = touched = prim = prim_mod = 0
    for h in hashes:
        prim += rb.get_nodes(h, 1) != ra.get_nodes(h, 1)
        prim_mod += (modulo_placement(h, before, 1)
                     != modulo_placement(h, after, 1))
        b, a = set(rb.get_nodes(h, n)), set(ra.get_nodes(h, n))
        moved = len(a - b)
        ch += moved
        touched += moved > 0
        mod += len(set(modulo_placement(h, after, n))
                   - set(modulo_placement(h, before, n)))
    copies = len(hashes) * n
    return {
        "change": f"{len(before)} -> {len(after)} nodes",
        "chunks": len(hashes),
        "consistent_primary_moved_pct": round(100 * prim / len(hashes), 2),
        "modulo_primary_moved_pct": round(100 * prim_mod / len(hashes), 2),
        "consistent_copies_moved_pct": round(100 * ch / copies, 2),
        "consistent_chunks_touched_pct": round(100 * touched / len(hashes), 2),
        "modulo_copies_moved_pct": round(100 * mod / copies, 2),
        "theory_pct": round(100 * abs(len(after) - len(before))
                            / max(len(after), len(before)), 2),
    }


def balance(nodes, vnodes, hashes, n):
    ring = ConsistentHashRing(nodes, vnodes)
    counts = {x: 0 for x in nodes}
    for h in hashes:
        for x in ring.get_nodes(h, n):
            counts[x] += 1
    mean = sum(counts.values()) / len(counts)
    return {"vnodes": vnodes,
            "max_over_mean": round(max(counts.values()) / mean, 3),
            "min_over_mean": round(min(counts.values()) / mean, 3)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chunks", type=int, default=100_000)
    p.add_argument("--replicas", type=int, default=3)
    p.add_argument("--vnodes", type=int, default=128)
    p.add_argument("--url", help="coordinator URL to use the real catalog")
    p.add_argument("--out", default="results/placement_experiment")
    a = p.parse_args()

    nodes4 = ["node1", "node2", "node3", "node4"]
    results = {}

    if a.url:
        import httpx
        base = a.url.rstrip("/")
        results["add_node5"] = httpx.get(
            f"{base}/v1/placement/simulate", params={"add": "node5"},
            timeout=300).json()
        results["remove_node4"] = httpx.get(
            f"{base}/v1/placement/simulate", params={"remove": "node4"},
            timeout=300).json()
    else:
        hashes = [hashlib.sha256(os.urandom(32)).hexdigest()
                  for _ in range(a.chunks)]
        results["add_node5"] = compare(hashes, nodes4, nodes4 + ["node5"],
                                       a.replicas, a.vnodes)
        results["remove_node4"] = compare(hashes, nodes4, nodes4[:3] + [],
                                          min(a.replicas, 3), a.vnodes)
        results["grow_4_to_8"] = compare(
            hashes, nodes4, [f"node{i}" for i in range(1, 9)],
            a.replicas, a.vnodes)
        sample = hashes[:20_000]
        results["load_balance_vs_vnodes"] = [
            balance(nodes4, v, sample, a.replicas) for v in (1, 8, 32, 128, 512)]

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))
    print(f"\nSaved {out}.json")


if __name__ == "__main__":
    main()
