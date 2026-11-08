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

Steps 1-2 run in the coordinator (it PLANS the work); steps 3-4 run in the
workers, which receive the chunks in batches through the job queue
(coordinator/jobs.py). Rebalancing and garbage collection work the same way.
"""

import asyncio
import hashlib
import json
import time
from collections import Counter
from datetime import datetime, timezone

import httpx

from coordinator import cluster, config, erasure, jobs
from coordinator.db import connect_db
from coordinator.redundancy import delete_unreferenced, still_referenced

STARTED_AT = datetime.now(timezone.utc)
REPAIR_LOCK = asyncio.Lock()
# Set when a node joins the ring automatically: rebalance at this time.
PENDING_REBALANCE = {"at": None, "nodes": []}


def _empty_counters():
    return {"chunks_repaired": 0, "copies_or_shards_rebuilt": 0,
            "bytes_read": 0, "bytes_written": 0, "chunks_lost": 0}


SCRUB_REPORTS: list[dict] = []


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


def repair_action(row: dict, failed) -> str:
    """'rebuild' (needs a worker), 'cleanup' (only drop references to dead
    copies, done here) or 'none' (healthy, lost, or no room to rebuild)."""
    if row["scheme"] == "ec":
        k, m = row["ec_k"], row["ec_m"]
        live = [s for s in row["shards"] if s["node"] not in failed]
        dead = len(live) != len(row["shards"])
        present = {s["index"] for s in live}
        if len(present) == k + m:
            return "cleanup" if dead else "none"
        if len(present) < k:
            return "none"                         # unrecoverable for now
        load = Counter(s["node"] for s in live)
        return "rebuild" if any(load[c] < m for c in
                                cluster.preference_list(row["chunk_hash"])) else "none"
    live = [x for x in row["nodes"] if x not in failed]
    dead = len(live) != len(row["nodes"])
    if len(live) >= config.REPLICATION_FACTOR:
        return "cleanup" if dead else "none"
    if not live:
        return "none"                             # no copy left to read
    targets = [x for x in cluster.preference_list(row["chunk_hash"])
               if x not in live]
    return "rebuild" if targets else "none"


def _cleanup(row: dict, failed) -> None:
    if row["scheme"] == "ec":
        live = [s for s in row["shards"] if s["node"] not in failed]
        _update_catalog(row, cluster.shard_nodes(live), live)
    else:
        _update_catalog(row, [x for x in row["nodes"] if x not in failed])


async def run_repair(reason: str) -> dict | None:
    """Plan a repair: find the chunks to rebuild and enqueue them."""
    if REPAIR_LOCK.locked():
        return None
    async with REPAIR_LOCK:
        await asyncio.to_thread(jobs.expire_stale_jobs)
        if await asyncio.to_thread(jobs.active_job, "repair"):
            return None                           # one repair job at a time
        failed = cluster.failed_nodes(force=True)
        rows = await asyncio.to_thread(chunks_needing_repair, failed)
        todo = []
        for row in rows:
            action = repair_action(row, failed)
            if action == "cleanup":
                await asyncio.to_thread(_cleanup, row, failed)
            elif action == "rebuild":
                todo.append([row["chunk_hash"], row["policy"]])
        if not todo:
            return None
        job = await jobs.create_job("repair", reason, todo,
                                    meta={"failed_nodes": sorted(failed)})
        print(f"[repair] {reason}: {len(todo)} chunks queued "
              f"({job['batches']} batches, {job['queue']})", flush=True)
        return job


def _load_rows(items) -> list[dict]:
    if not items:
        return []
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM chunk_catalog WHERE (chunk_hash, policy) IN "
            "(SELECT x->>0, x->>1 FROM jsonb_array_elements(%s::jsonb) x)",
            (json.dumps(items),)).fetchall()


async def execute_repair_batch(items: list, job_id: str) -> dict:
    """Worker side: rebuild the chunks of one batch."""
    rows = await asyncio.to_thread(_load_rows, items)
    failed = cluster.failed_nodes(force=True)
    job = {"by_scheme": {"replication": _empty_counters(), "ec": _empty_counters()},
           "lost": []}
    sem = asyncio.Semaphore(config.REPAIR_CONCURRENCY)
    async with httpx.AsyncClient(timeout=120) as client:
        async def one(row):
            async with sem:
                try:
                    if row["scheme"] == "ec":
                        await repair_erasure_coded(client, row, failed, job)
                    else:
                        await repair_replicated(client, row, failed, job)
                except Exception as exc:          # keep repairing the rest
                    print(f"[repair] {row['chunk_hash']}: {exc}", flush=True)
        await asyncio.gather(*(one(r) for r in rows))
    await asyncio.to_thread(jobs.record_progress, job_id, len(items),
                            job["by_scheme"], job["lost"])
    return job["by_scheme"]


def status_view() -> dict:
    """Repair state for the API and the UI (from the jobs table)."""
    active = jobs.active_job("repair")
    history = [jobs.describe(j) for j in jobs.recent_jobs("repair", 20)
               if j["status"] != "running"]
    totals = {"replication": _empty_counters(), "ec": _empty_counters()}
    with connect_db() as conn:
        for row in conn.execute("SELECT counters FROM jobs WHERE type = 'repair'"
                                ).fetchall():
            jobs._add(totals, row["counters"] or {})
    return {"state": "running" if active else "idle",
            "current": jobs.describe(active), "history": history,
            "totals": totals}


async def repair_loop():
    """Leader loop in the coordinator: failure detection -> repair plans,
    and the delayed rebalance after a node joined."""
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
        try:
            if PENDING_REBALANCE["at"] and time.monotonic() >= PENDING_REBALANCE["at"]:
                joined = ", ".join(PENDING_REBALANCE["nodes"])
                PENDING_REBALANCE.update(at=None, nodes=[])
                await start_rebalance(f"node joined: {joined}")
            if not config.REPAIR_ENABLED:
                continue
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

def plan_moves(row: dict, failed) -> list:
    """Copies/shards of a chunk that are not where the ring wants them."""
    if row["scheme"] == "ec":
        try:
            want = cluster.ec_assignment(row["chunk_hash"], row["ec_k"], row["ec_m"])
        except Exception:
            return []
        return [(s["index"], want[s["index"]]) for s in row["shards"]
                if s["node"] != want[s["index"]] and want[s["index"]] not in failed]
    want = cluster.replication_targets(row["chunk_hash"])
    current = list(row["nodes"])
    to_add = [x for x in want if x not in current and x not in failed]
    to_remove = [x for x in current if x not in want]
    return [("add", x) for x in to_add] + [("remove", x) for x in to_remove]


async def start_rebalance(reason: str, dry_run: bool = False) -> dict:
    """Plan a rebalance and enqueue the chunks that must move."""
    failed = cluster.failed_nodes(force=True)
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM chunk_catalog").fetchall()
    total_copies = sum(r["ec_k"] + r["ec_m"] if r["scheme"] == "ec"
                       else config.REPLICATION_FACTOR for r in rows)
    todo, copies_to_add = [], 0
    for row in rows:
        moves = plan_moves(row, failed)
        if moves:
            todo.append([row["chunk_hash"], row["policy"]])
            copies_to_add += sum(1 for m in moves if m[0] != "remove")
    meta = {"chunks_total": len(rows), "copies_total": total_copies,
            "copies_planned": copies_to_add, "members": sorted(cluster.NODES)}
    if dry_run:
        return {"dry_run": True, "chunks_moved": len(todo), **meta,
                "chunks_moved_pct": round(100 * len(todo) / max(len(rows), 1), 2),
                "copies_moved_pct": round(100 * copies_to_add / max(total_copies, 1), 2)}
    return await jobs.create_job("rebalance", reason, todo, meta=meta)


def rebalance_summary(job: dict) -> dict:
    c, meta = job.get("counters") or {}, job.get("meta") or {}
    return {**job,
            "chunks_total": meta.get("chunks_total"),
            "chunks_moved": c.get("chunks_moved", 0),
            "copies_added": c.get("copies_added", 0),
            "copies_removed": c.get("copies_removed", 0),
            "bytes_transferred": c.get("bytes_transferred", 0),
            "failed_moves": c.get("failed_moves", 0),
            "members": meta.get("members"),
            "chunks_moved_pct": round(100 * c.get("chunks_moved", 0)
                                      / max(meta.get("chunks_total") or 0, 1), 2),
            "copies_moved_pct": round(100 * c.get("copies_added", 0)
                                      / max(meta.get("copies_total") or 0, 1), 2)}


async def rebalance_row(client, row, failed, stats) -> None:
    """Move one chunk's copies/shards. Order: copy to the new owner ->
    update the catalog -> delete the old copy, so a crash never loses data."""
    digest = row["chunk_hash"]
    if row["scheme"] == "ec":
        try:
            want = cluster.ec_assignment(digest, row["ec_k"], row["ec_m"])
        except Exception:
            return
        moves = [s for s in row["shards"]
                 if s["node"] != want[s["index"]] and want[s["index"]] not in failed]
        if not moves:
            return
        stats["chunks_moved"] += 1
        shards = [dict(s) for s in row["shards"]]
        old = []
        for shard in moves:
            dst = want[shard["index"]]
            blob, status = await cluster.get_blob(client, shard["node"],
                                                  shard["hash"], shard["size"])
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
        await asyncio.to_thread(_update_catalog, row, cluster.shard_nodes(shards),
                                shards)
        stats["copies_removed"] += await delete_unreferenced(client, old)
        return

    n = config.REPLICATION_FACTOR
    want = cluster.replication_targets(digest)
    current = list(row["nodes"])
    to_add = [x for x in want if x not in current and x not in failed]
    to_remove = [x for x in current if x not in want]
    if not to_add and not to_remove:
        return
    stats["chunks_moved"] += 1
    nodes = current
    if to_add:
        data = None
        for src in current:
            data, status = await cluster.get_blob(client, src, digest, row["size"])
            if status == "ok":
                break
        if data is None:
            stats["failed_moves"] += 1
            return
        for dst in to_add:
            if await cluster.put_blob(client, dst, digest, data):
                nodes.append(dst)
                stats["copies_added"] += 1
                stats["bytes_transferred"] += row["size"]
            else:
                stats["failed_moves"] += 1
    removed = []
    for src in to_remove:                         # keep at least N copies
        if len(nodes) > n:
            nodes.remove(src)
            removed.append((digest, src))
    await asyncio.to_thread(_update_catalog, row, nodes)
    stats["copies_removed"] += await delete_unreferenced(client, removed)


async def execute_rebalance_batch(items: list, job_id: str) -> dict:
    rows = await asyncio.to_thread(_load_rows, items)
    failed = cluster.failed_nodes(force=True)
    stats = {"chunks_moved": 0, "copies_added": 0, "copies_removed": 0,
             "bytes_transferred": 0, "failed_moves": 0}
    async with httpx.AsyncClient(timeout=120) as client:
        for row in rows:
            try:
                await rebalance_row(client, row, failed, stats)
            except Exception as exc:
                stats["failed_moves"] += 1
                print(f"[rebalance] {row['chunk_hash']}: {exc}", flush=True)
    await asyncio.to_thread(jobs.record_progress, job_id, len(items), stats)
    return stats


async def execute_gc_batch(items: list, job_id: str) -> dict:
    """Delete blobs (hash, node) that no catalog entry references any more."""
    async with httpx.AsyncClient(timeout=60) as client:
        removed = await delete_unreferenced(client, [tuple(i) for i in items])
    await asyncio.to_thread(jobs.record_progress, job_id, len(items),
                            {"blobs_removed": removed})
    return {"blobs_removed": removed}


HANDLERS = {"repair": execute_repair_batch, "rebalance": execute_rebalance_batch,
            "gc": execute_gc_batch}


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
