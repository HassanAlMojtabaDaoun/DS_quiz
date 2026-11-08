"""Repair, scrub reports, degraded-object accounting and rebalancing.

Repair loop (every REPAIR_INTERVAL seconds, or immediately when woken):
  1. Ask the failure detector which nodes are failed.
  2. Select every chunk that has a copy/shard on a failed node or that has
     fewer copies/shards than its policy requires (e.g. after a read or the
     scrubber found a missing or corrupted copy).
  3. Replication: read one valid copy, write new copies on the next live
     nodes of the chunk's preference list until N copies exist.
     Erasure coding: read any k valid shards, decode, re-encode only the
     missing shards and store them on live nodes.
  4. Update the catalog. Record traffic (bytes read / written) per scheme,
     which is what the replication vs erasure-coding comparison uses.
"""

import asyncio
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone

import httpx

from coordinator import cluster, config, erasure
from coordinator.db import connect_db
from coordinator.redundancy import delete_unreferenced, still_referenced

STARTED_AT = datetime.now(timezone.utc)
REPAIR_LOCK = asyncio.Lock()


def _empty_counters():
    return {"chunks_repaired": 0, "copies_or_shards_rebuilt": 0,
            "bytes_read": 0, "bytes_written": 0, "chunks_lost": 0}


REPAIR_STATUS: dict = {
    "state": "idle",
    "current": None,
    "history": [],
    "totals": {"replication": _empty_counters(), "ec": _empty_counters()},
}
SCRUB_REPORTS: list[dict] = []
LAST_REBALANCE: dict = {}


def _now():
    return datetime.now(timezone.utc)


def _update_catalog(row: dict, nodes: list[str], shards=None) -> None:
    key = (row["chunk_hash"], row["policy"])
    with connect_db() as conn:
        if shards is None:
            conn.execute(
                "UPDATE chunk_catalog SET nodes = %s::jsonb "
                "WHERE chunk_hash = %s AND policy = %s",
                (json.dumps(nodes), *key))
        else:
            conn.execute(
                "UPDATE chunk_catalog SET nodes = %s::jsonb, shards = %s::jsonb "
                "WHERE chunk_hash = %s AND policy = %s",
                (json.dumps(nodes), json.dumps(shards), *key))


# ---------------------------------------------------------------------------
# Repairing one chunk
# ---------------------------------------------------------------------------

async def repair_replicated(client, row, failed, job) -> bool:
    """Returns True if the chunk was (at least partly) repaired."""
    digest, size, n = row["chunk_hash"], row["size"], config.REPLICATION_FACTOR
    live = [x for x in row["nodes"] if x not in failed]
    dead = [x for x in row["nodes"] if x in failed]
    if len(live) >= n:
        if dead:                       # enough copies elsewhere: forget dead
            _update_catalog(row, live)
        return False

    targets = [x for x in cluster.preference_list(digest) if x not in live]
    if not targets:
        return False                   # nowhere to put a new copy (yet)

    counters = job["by_scheme"]["replication"]
    data = None
    for node in list(live):
        blob, status = await cluster.get_blob(client, node, digest, size)
        if status == "ok":
            data = blob
            counters["bytes_read"] += size
            break
        if status in ("missing", "corrupt"):
            live.remove(node)
    if data is None:
        if not dead:
            counters["chunks_lost"] += 1
            job["lost"].append(digest)
        _update_catalog(row, live + dead)
        return False

    added = []
    for node in targets:
        if len(live) + len(added) >= n:
            break
        if await cluster.put_blob(client, node, digest, data):
            added.append(node)
            counters["bytes_written"] += size
            counters["copies_or_shards_rebuilt"] += 1

    keep = live + added
    if len(keep) < n:                  # still short: remember the dead copies,
        keep += dead[:n - len(keep)]   # the node may come back
    _update_catalog(row, keep)
    if added:
        counters["chunks_repaired"] += 1
    return bool(added)


async def repair_erasure_coded(client, row, failed, job) -> bool:
    digest, size = row["chunk_hash"], row["size"]
    k, m = row["ec_k"], row["ec_m"]
    counters = job["by_scheme"]["ec"]

    live = [s for s in row["shards"] if s["node"] not in failed]
    dead = [s for s in row["shards"] if s["node"] in failed]
    present = {s["index"] for s in live}
    missing = sorted(i for i in range(k + m) if i not in present)
    if not missing:
        if dead:
            _update_catalog(row, cluster.shard_nodes(live), live)
        return False

    load = Counter(s["node"] for s in live)
    candidates = cluster.preference_list(digest)
    if not any(load[c] < m for c in candidates):
        return False                   # nowhere to put a rebuilt shard

    # Read any k valid shards: the repair traffic of EC is k shard reads.
    available: dict[int, bytes] = {}
    for s in sorted(live, key=lambda s: s["index"]):
        if len(available) >= k:
            break
        if s["index"] in available:
            continue
        blob, status = await cluster.get_blob(client, s["node"], s["hash"],
                                              s["size"])
        if status == "ok":
            available[s["index"]] = blob
            counters["bytes_read"] += s["size"]
        elif status in ("missing", "corrupt"):
            live.remove(s)
            load[s["node"]] -= 1
            if s["index"] not in {x["index"] for x in live}:
                missing = sorted(set(missing) | {s["index"]})
    if len(available) < k:
        if not dead:
            counters["chunks_lost"] += 1
            job["lost"].append(digest)
        shards = sorted(live + dead, key=lambda s: s["index"])
        _update_catalog(row, cluster.shard_nodes(shards), shards)
        return False

    data = await asyncio.to_thread(erasure.decode, available, k, m, size,
                                   row["codec"])
    if hashlib.sha256(data).hexdigest() != digest:
        counters["chunks_lost"] += 1
        job["lost"].append(digest)
        return False
    all_shards = await asyncio.to_thread(erasure.encode, data, k, m,
                                         row["codec"])

    # Put each rebuilt shard on the live node holding the fewest shards of
    # this chunk (ties: ring order), never more than m shards per node.
    rebuilt = set()
    for index in missing:
        options = [c for c in candidates if load[c] < m]
        if not options:
            break
        node = min(options, key=lambda c: (load[c], candidates.index(c)))
        blob = all_shards[index]
        blob_hash = hashlib.sha256(blob).hexdigest()
        if await cluster.put_blob(client, node, blob_hash, blob):
            live.append({"index": index, "hash": blob_hash,
                         "size": len(blob), "node": node})
            load[node] += 1
            rebuilt.add(index)
            counters["bytes_written"] += len(blob)
            counters["copies_or_shards_rebuilt"] += 1

    # Keep references to dead shards that could not be rebuilt yet.
    shards = live + [s for s in dead
                     if s["index"] not in rebuilt
                     and s["index"] not in {x["index"] for x in live}]
    shards.sort(key=lambda s: s["index"])
    _update_catalog(row, cluster.shard_nodes(shards), shards)
    if rebuilt:
        counters["chunks_repaired"] += 1
    return bool(rebuilt)


# ---------------------------------------------------------------------------
# Repair job
# ---------------------------------------------------------------------------

def chunks_needing_repair(failed) -> list[dict]:
    with connect_db() as conn:
        return conn.execute(
            """
            SELECT * FROM chunk_catalog
            WHERE nodes ?| %s::text[]
               OR (scheme = 'replication' AND jsonb_array_length(nodes) < %s)
               OR (scheme = 'ec' AND jsonb_array_length(shards) < ec_k + ec_m)
            """,
            (list(failed), config.REPLICATION_FACTOR)).fetchall()


async def run_repair(reason: str) -> dict | None:
    if REPAIR_LOCK.locked():
        return None
    async with REPAIR_LOCK:
        failed = cluster.failed_nodes(force=True)
        rows = chunks_needing_repair(failed)
        if not rows:
            return None

        job = {
            "reason": reason,
            "failed_nodes": sorted(failed),
            "started_at": _now().isoformat(),
            "chunks_to_check": len(rows),
            "chunks_done": 0,
            "by_scheme": {"replication": _empty_counters(),
                          "ec": _empty_counters()},
            "lost": [],
        }
        REPAIR_STATUS.update(state="running", current=job)
        sem = asyncio.Semaphore(config.REPAIR_CONCURRENCY)

        async with httpx.AsyncClient(timeout=120) as client:
            async def one(row):
                async with sem:
                    try:
                        if row["scheme"] == "ec":
                            await repair_erasure_coded(client, row, failed, job)
                        else:
                            await repair_replicated(client, row, failed, job)
                    except Exception as exc:     # keep repairing the rest
                        print(f"[repair] {row['chunk_hash']}: {exc}", flush=True)
                    job["chunks_done"] += 1

            await asyncio.gather(*(one(r) for r in rows))

        finished = _now()
        REPAIR_STATUS.update(state="idle", current=None)
        did_work = any(v for c in job["by_scheme"].values() for v in c.values())
        if not did_work:
            return None
        job["finished_at"] = finished.isoformat()
        job["duration_seconds"] = round(
            (finished - datetime.fromisoformat(job["started_at"])).total_seconds(), 2)
        job["lost"] = job["lost"][:20]
        for scheme, counters in job["by_scheme"].items():
            for key, value in counters.items():
                REPAIR_STATUS["totals"][scheme][key] += value

        REPAIR_STATUS["history"] = ([job] + REPAIR_STATUS["history"])[:20]
        print(f"[repair] {reason}: {json.dumps(job['by_scheme'])}", flush=True)
        return job


async def repair_loop():
    # Grace period after start-up: heartbeats in the database may be old.
    await asyncio.sleep(config.HEARTBEAT_TIMEOUT + 1)
    while True:
        try:
            await asyncio.wait_for(cluster.REPAIR_WAKEUP.wait(),
                                   timeout=config.REPAIR_INTERVAL)
            reason = "requested"
        except asyncio.TimeoutError:
            reason = "periodic check"
        cluster.REPAIR_WAKEUP.clear()
        if not config.REPAIR_ENABLED:
            continue
        try:
            failed = cluster.failed_nodes(force=True)
            if failed and reason == "periodic check":
                reason = "node failure: " + ", ".join(sorted(failed))
            await run_repair(reason)
        except Exception as exc:
            print(f"[repair] loop error: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Scrub reports (bit rot found by a storage node)
# ---------------------------------------------------------------------------

def handle_scrub_report(node: str, corrupted: list[str]) -> dict:
    """The node already quarantined the bad copies: remove them from the
    catalog so the repair job re-creates them from a healthy copy."""
    affected = 0
    with connect_db() as conn:
        for digest in corrupted:
            r = conn.execute(
                """
                UPDATE chunk_catalog
                SET nodes = COALESCE((SELECT jsonb_agg(n) FROM jsonb_array_elements(nodes) n
                                      WHERE n <> to_jsonb(%s::text)), '[]'::jsonb)
                WHERE chunk_hash = %s AND scheme = 'replication' AND nodes ? %s
                """, (node, digest, node))
            affected += r.rowcount
            rows = conn.execute(
                "SELECT chunk_hash, policy, shards FROM chunk_catalog "
                "WHERE scheme = 'ec' AND shards @> %s::jsonb",
                (json.dumps([{"hash": digest, "node": node}]),)).fetchall()
            for row in rows:
                kept = [s for s in row["shards"]
                        if not (s["hash"] == digest and s["node"] == node)]
                conn.execute(
                    "UPDATE chunk_catalog SET shards = %s::jsonb, "
                    "nodes = %s::jsonb WHERE chunk_hash = %s AND policy = %s",
                    (json.dumps(kept), json.dumps(cluster.shard_nodes(kept)),
                     row["chunk_hash"], row["policy"]))
                affected += 1

    report = {"node_id": node, "corrupted": corrupted,
              "catalog_entries_affected": affected,
              "received_at": _now().isoformat()}
    SCRUB_REPORTS.insert(0, report)
    del SCRUB_REPORTS[50:]
    cluster.request_repair()
    return report


# ---------------------------------------------------------------------------
# Degraded objects / cluster health
# ---------------------------------------------------------------------------

def health_summary() -> dict:
    failed = cluster.failed_nodes()
    n = config.REPLICATION_FACTOR
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT chunk_hash, policy, size, nodes, scheme, ec_k, ec_m, "
            "shards FROM chunk_catalog").fetchall()
        objects = conn.execute(
            "SELECT bucket, object_key, version, object_id, chunk_hashes, "
            "policy FROM object_versions").fetchall()

    degraded, lost = set(), set()
    per_node = Counter()
    per_node_bytes = Counter()
    for r in rows:
        if r["scheme"] == "ec":
            for s in r["shards"] or []:
                per_node[s["node"]] += 1
                per_node_bytes[s["node"]] += s["size"]
            live = {s["index"] for s in r["shards"] or [] if s["node"] not in failed}
            if len(live) < r["ec_k"]:
                lost.add((r["chunk_hash"], r["policy"]))
            elif len(live) < r["ec_k"] + r["ec_m"]:
                degraded.add((r["chunk_hash"], r["policy"]))
        else:
            for x in r["nodes"]:
                per_node[x] += 1
                per_node_bytes[x] += r["size"]
            live = [x for x in r["nodes"] if x not in failed]
            if not live:
                lost.add((r["chunk_hash"], r["policy"]))
            elif len(live) < n:
                degraded.add((r["chunk_hash"], r["policy"]))

    degraded_objects, lost_objects = [], []
    for o in objects:
        hashes = {(h, o["policy"]) for h in o["chunk_hashes"]}
        ref = {"file_id": str(o["object_id"]), "bucket": o["bucket"],
               "key": o["object_key"], "version": o["version"]}
        if hashes & lost:
            lost_objects.append(ref)
        elif hashes & degraded:
            degraded_objects.append(ref)

    return {
        "failed_nodes": sorted(failed),
        "chunks_total": len(rows),
        "chunks_degraded": len(degraded),
        "chunks_unavailable": len(lost),
        "objects_total": len(objects),
        "objects_degraded": len(degraded_objects),
        "objects_unavailable": len(lost_objects),
        "degraded_objects_sample": degraded_objects[:20],
        "unavailable_objects_sample": lost_objects[:20],
        "blobs_per_node": dict(sorted(per_node.items())),
        "bytes_per_node": dict(sorted(per_node_bytes.items())),
    }


# ---------------------------------------------------------------------------
# Rebalancing after a membership change
# ---------------------------------------------------------------------------

async def rebalance(dry_run: bool) -> dict:
    """Move every chunk / shard to where the current ring wants it.

    Order for every move: copy to the new owner -> update the catalog ->
    delete the old copy. At no point are there fewer valid copies than
    before, so a crash in the middle never loses data.
    """
    started = _now()
    failed = cluster.failed_nodes(force=True)
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM chunk_catalog").fetchall()

    stats = {"chunks_total": len(rows), "chunks_moved": 0, "copies_added": 0,
             "copies_removed": 0, "bytes_transferred": 0, "failed_moves": 0}
    total_copies = 0

    async with httpx.AsyncClient(timeout=120) as client:
        for row in rows:
            digest = row["chunk_hash"]

            if row["scheme"] == "ec":
                k, m = row["ec_k"], row["ec_m"]
                total_copies += k + m
                try:
                    want = cluster.ec_assignment(digest, k, m)
                except Exception:
                    continue
                moves = [s for s in row["shards"]
                         if s["node"] != want[s["index"]]
                         and want[s["index"]] not in failed]
                if not moves:
                    continue
                stats["chunks_moved"] += 1
                if dry_run:
                    stats["copies_added"] += len(moves)
                    continue

                shards = [dict(s) for s in row["shards"]]
                old = []
                for shard in moves:
                    dst = want[shard["index"]]
                    blob, status = await cluster.get_blob(
                        client, shard["node"], shard["hash"], shard["size"])
                    if status != "ok" or not await cluster.put_blob(
                            client, dst, shard["hash"], blob):
                        stats["failed_moves"] += 1
                        continue
                    for s in shards:
                        if s["index"] == shard["index"]:
                            old.append((s["hash"], s["node"]))
                            s["node"] = dst
                    stats["copies_added"] += 1
                    stats["bytes_transferred"] += shard["size"]
                _update_catalog(row, cluster.shard_nodes(shards), shards)
                stats["copies_removed"] += await delete_unreferenced(client, old)

            else:
                n = config.REPLICATION_FACTOR
                total_copies += n
                want = cluster.replication_targets(digest)
                current = list(row["nodes"])
                to_add = [x for x in want if x not in current and x not in failed]
                to_remove = [x for x in current if x not in want]
                if not to_add and not to_remove:
                    continue
                stats["chunks_moved"] += 1
                if dry_run:
                    stats["copies_added"] += len(to_add)
                    continue

                nodes = current
                if to_add:
                    data = None
                    for src in current:
                        data, status = await cluster.get_blob(
                            client, src, digest, row["size"])
                        if status == "ok":
                            break
                    if data is None:
                        stats["failed_moves"] += 1
                        continue
                    for dst in to_add:
                        if await cluster.put_blob(client, dst, digest, data):
                            nodes.append(dst)
                            stats["copies_added"] += 1
                            stats["bytes_transferred"] += row["size"]
                        else:
                            stats["failed_moves"] += 1
                # Drop surplus copies only while N copies remain.
                removed = []
                for src in to_remove:
                    if len(nodes) > n:
                        nodes.remove(src)
                        removed.append((digest, src))
                _update_catalog(row, nodes)
                stats["copies_removed"] += await delete_unreferenced(client, removed)

    total = max(stats["chunks_total"], 1)
    stats.update({
        "dry_run": dry_run,
        "members": sorted(cluster.NODES),
        "chunks_moved_pct": round(100 * stats["chunks_moved"] / total, 2),
        "copies_moved_pct": round(100 * stats["copies_added"]
                                  / max(total_copies, 1), 2),
        "started_at": started.isoformat(),
        "duration_seconds": round((_now() - started).total_seconds(), 2),
    })
    LAST_REBALANCE.clear()
    LAST_REBALANCE.update(stats)
    return stats


# ---------------------------------------------------------------------------
# Orphan garbage collection
# ---------------------------------------------------------------------------

async def collect_orphans(min_age_seconds: float, dry_run: bool) -> dict:
    """Delete blobs that no catalog entry references.

    Orphans appear when a failed node comes back after its data was rebuilt
    elsewhere, or when an upload failed half-way. Only blobs older than
    min_age_seconds are touched, so chunks of an upload that is still in
    progress (written to nodes, not yet committed) are never removed.
    """
    import time
    now = time.time()
    result = {"dry_run": dry_run, "nodes": {}}
    async with httpx.AsyncClient(timeout=120) as client:
        for node in sorted(set(cluster.NODES) - set(cluster.failed_nodes(True))):
            try:
                r = await client.get(f"{cluster.node_url(node)}/v1/chunks",
                                     headers=cluster.node_headers())
                blobs = r.json()["chunks"]
            except (httpx.HTTPError, ValueError, KeyError):
                result["nodes"][node] = "unreachable"
                continue
            old = [(b["hash"], node) for b in blobs
                   if now - b["mtime"] >= min_age_seconds]
            with connect_db() as conn:
                orphans = [p for p in old if not still_referenced(conn, *p)]
            removed = 0 if dry_run else await delete_unreferenced(client, orphans)
            result["nodes"][node] = {"blobs": len(blobs),
                                     "orphans": len(orphans),
                                     "removed": removed}
    return result
