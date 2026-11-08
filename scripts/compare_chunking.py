"""Compare fixed-size chunking with FastCDC on a dataset (offline).

For each method it reports chunk count, chunk-size distribution, how many
chunks are unique (i.e. what deduplication would actually store) and the
chunking throughput. No cluster is needed.

Usage:
  python scripts/compare_chunking.py datasets/small datasets/large
"""

import argparse
import hashlib
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from coordinator.chunking import iter_chunks  # noqa: E402

FIXED_SIZE = 4 * 1024 * 1024


def analyse(directory: Path, method: str) -> dict:
    files = sorted(p for p in directory.rglob("*") if p.is_file())
    seen: dict[str, int] = {}
    sizes: list[int] = []
    logical = 0
    start = time.perf_counter()
    for path in files:
        with path.open("rb") as f:
            for chunk in iter_chunks(f, method, FIXED_SIZE):
                sizes.append(len(chunk))
                logical += len(chunk)
                seen.setdefault(hashlib.sha256(chunk).hexdigest(), len(chunk))
    elapsed = time.perf_counter() - start
    unique = sum(seen.values())
    return {
        "dataset": str(directory),
        "method": method,
        "files": len(files),
        "logical_MiB": round(logical / 2**20, 2),
        "chunks": len(sizes),
        "unique_chunks": len(seen),
        "unique_MiB": round(unique / 2**20, 2),
        "dedup_ratio": round(logical / unique, 3) if unique else None,
        "space_saved_pct": round(100 * (1 - unique / logical), 2) if logical else 0,
        "avg_chunk_KiB": round(statistics.mean(sizes) / 1024, 1) if sizes else 0,
        "min_chunk_KiB": round(min(sizes) / 1024, 1) if sizes else 0,
        "max_chunk_KiB": round(max(sizes) / 1024, 1) if sizes else 0,
        "throughput_MiB_s": round(logical / 2**20 / elapsed, 1) if elapsed else None,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("datasets", nargs="+")
    p.add_argument("--out", default="results/chunking_comparison")
    a = p.parse_args()

    rows = []
    for d in a.datasets:
        for method in ("fixed", "cdc"):
            print(f"chunking {d} with {method} ...", flush=True)
            rows.append(analyse(Path(d), method))

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(rows, indent=2))

    cols = ["dataset", "method", "files", "logical_MiB", "chunks",
            "unique_chunks", "unique_MiB", "dedup_ratio", "space_saved_pct",
            "avg_chunk_KiB", "min_chunk_KiB", "max_chunk_KiB",
            "throughput_MiB_s"]
    md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    md += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    out.with_suffix(".md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    print(f"\nSaved {out}.json and {out}.md")


if __name__ == "__main__":
    main()
