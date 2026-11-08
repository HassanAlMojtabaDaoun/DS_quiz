"""Generate the two test workloads required by the brief.

  (a) small: many small files (default 5,000 files of 1-50 KB, text + JSON)
  (b) large: a few large files (default 10 files of 100 MB - 1 GB)

To make deduplication measurable the large workload imitates successive
versions of disk images: every second file is a copy of the previous one
with a few random byte insertions and overwrites. Some small files are
also exact duplicates (e.g. repeated config/JSON documents).

Generation is deterministic (fixed seed), so anyone can regenerate the
same dataset instead of downloading it.

Usage:
  python scripts/gen_workloads.py small --out datasets/small
  python scripts/gen_workloads.py large --out datasets/large
  python scripts/gen_workloads.py large --out datasets/large --min-mb 50 --max-mb 200
"""

import argparse
import json
import random
from pathlib import Path

WORDS = ("node chunk replica quorum ring hash storage object bucket repair "
         "scrub parity shard heartbeat latency throughput metadata version "
         "failure recovery consistent partition cluster gateway").split()


def gen_small(out: Path, count: int, min_kb: int, max_kb: int, seed: int,
              dup_rate: float):
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    previous: list[bytes] = []
    total = 0
    for i in range(count):
        if previous and rng.random() < dup_rate:
            data = rng.choice(previous)                    # exact duplicate
        else:
            target = rng.randint(min_kb * 1024, max_kb * 1024)
            if i % 2 == 0:                                 # JSON document
                records, size = [], 2
                while size < target:
                    rec = {"id": rng.randint(0, 10**9),
                           "name": " ".join(rng.choices(WORDS, k=3)),
                           "value": round(rng.random() * 1000, 3),
                           "tags": rng.sample(WORDS, 3)}
                    records.append(rec)
                    size += len(json.dumps(rec)) + 2
                data = json.dumps(records, indent=1).encode()[:target]
            else:                                          # text document
                words = []
                size = 0
                while size < target:
                    w = rng.choice(WORDS)
                    words.append(w)
                    size += len(w) + 1
                data = " ".join(words).encode()[:target]
            previous.append(data)
            if len(previous) > 200:
                previous.pop(0)
        ext = "json" if data[:1] == b"[" else "txt"
        (out / f"file_{i:05d}.{ext}").write_bytes(data)
        total += len(data)
    print(f"small workload: {count} files, {total / 2**20:.1f} MiB -> {out}")


def gen_large(out: Path, count: int, min_mb: int, max_mb: int, seed: int,
              edits: int):
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    total = 0
    base = None
    for i in range(count):
        path = out / f"image_{i:02d}.img"
        if i % 2 == 1 and base is not None:
            # New "version" of the previous image: insert and overwrite a
            # few small byte ranges. Fixed-size chunking loses alignment
            # after an insertion; content-defined chunking does not.
            data = bytearray(base)
            for _ in range(edits):
                pos = rng.randrange(len(data))
                if rng.random() < 0.5:
                    data[pos:pos] = rng.randbytes(rng.randint(1, 4096))
                else:
                    data[pos:pos + 4096] = rng.randbytes(4096)
            data = bytes(data)
        else:
            size = rng.randint(min_mb, max_mb) * 2**20
            data = rng.randbytes(size)
            base = data
        path.write_bytes(data)
        total += len(data)
        print(f"  {path.name}: {len(data) / 2**20:.1f} MiB")
    print(f"large workload: {count} files, {total / 2**30:.2f} GiB -> {out}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="kind", required=True)

    s = sub.add_parser("small")
    s.add_argument("--out", default="datasets/small")
    s.add_argument("--count", type=int, default=5000)
    s.add_argument("--min-kb", type=int, default=1)
    s.add_argument("--max-kb", type=int, default=50)
    s.add_argument("--dup-rate", type=float, default=0.10)
    s.add_argument("--seed", type=int, default=42)

    l = sub.add_parser("large")
    l.add_argument("--out", default="datasets/large")
    l.add_argument("--count", type=int, default=10)
    l.add_argument("--min-mb", type=int, default=100)
    l.add_argument("--max-mb", type=int, default=1024)
    l.add_argument("--edits", type=int, default=20)
    l.add_argument("--seed", type=int, default=7)

    a = p.parse_args()
    if a.kind == "small":
        gen_small(Path(a.out), a.count, a.min_kb, a.max_kb, a.seed, a.dup_rate)
    else:
        gen_large(Path(a.out), a.count, a.min_mb, a.max_mb, a.seed, a.edits)


if __name__ == "__main__":
    main()
