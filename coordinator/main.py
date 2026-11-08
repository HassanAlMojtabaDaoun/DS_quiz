"""Coordinator: metadata, placement, redundancy and repair service.

Modules
  config.py      settings from environment variables
  db.py          PostgreSQL schema (metadata store)
  ring.py        consistent hashing ring with virtual nodes
  chunking.py    fixed-size and FastCDC chunking
  erasure.py     Reed-Solomon codec (zfec or built-in)
  cluster.py     membership, failure detector, node I/O, placement
  redundancy.py  write / read paths for replication and erasure coding
  repair.py      repair job, scrub reports, health, rebalancing, GC
  main.py        HTTP API (this file)
"""

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import httpx
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from coordinator import (api_keys, api_objects, cluster, config, erasure,
                         jobs, objects, repair, search_index, worker)
from fastapi.responses import Response
from observability.metrics import REGISTRY, instrument_app
from coordinator.db import connect_db, init_db
from coordinator.objects import describe_bucket, ensure_bucket
from coordinator.redundancy import policy_key
from coordinator.ring import ConsistentHashRing, modulo_placement

NODE_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,32}")
BUCKET_RE = objects.BUCKET_RE
CONFIG_WARNINGS = config.validate()


@asynccontextmanager
async def lifespan(app: FastAPI):
    for warning in CONFIG_WARNINGS:
        print(f"[config] WARNING: {warning}", flush=True)
    init_db()
    cluster.load_membership()
    ensure_bucket("default")
    tasks = [asyncio.create_task(repair.repair_loop()),
             asyncio.create_task(search_index.sync_loop())]
    # In-process job workers (0 when separate worker containers are used).
    tasks += worker.start_workers(config.LOCAL_WORKERS, "coordinator")
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="Distributed Content-Addressed Storage",
              version="1.4.0", lifespan=lifespan)
# S3-style object API and API-key management (used by the gateway).
app.include_router(api_objects.router)
app.include_router(api_keys.router)
instrument_app(app, "coordinator")


def check_cluster_token(token: str | None) -> None:
    if not token or token != config.CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def parse_object_id(object_id: str) -> UUID:
    try:
        return UUID(object_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file ID")


# ---------------------------------------------------------------------------
# Health and node membership
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "node_count": len(cluster.NODES),
        "replication_factor": config.REPLICATION_FACTOR,
        "write_quorum": config.WRITE_QUORUM,
        "read_quorum": config.READ_QUORUM,
        "erasure_codec": erasure.DEFAULT_CODEC,
        "warnings": CONFIG_WARNINGS,
    }


@app.post("/v1/nodes/heartbeat")
def receive_heartbeat(payload: dict,
                      x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    required = {"node_id", "free_bytes", "total_bytes", "chunk_count",
                "stored_bytes"}
    if not required.issubset(payload):
        raise HTTPException(status_code=400, detail="Missing heartbeat fields")

    node_id = str(payload["node_id"])
    if not NODE_ID_RE.fullmatch(node_id):
        raise HTTPException(status_code=400, detail="Invalid node id")
    # Nodes that are not ring members are accepted and shown as "standby":
    # an operator can then add them to the ring with /v1/admin/nodes.
    url = payload.get("url")
    if url is not None and not re.fullmatch(r"https?://\S+", str(url)):
        raise HTTPException(status_code=400, detail="Invalid node url")
    if url:
        cluster.KNOWN_URLS.setdefault(node_id, str(url).rstrip("/"))
        if config.AUTO_JOIN and node_id not in cluster.NODES:
            auto_join(node_id, str(url).rstrip("/"))

    try:
        free_bytes = int(payload["free_bytes"])
        total_bytes = int(payload["total_bytes"])
        chunk_count = int(payload["chunk_count"])
        stored_bytes = int(payload["stored_bytes"])
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid metrics")
    if min(free_bytes, total_bytes, chunk_count, stored_bytes) < 0:
        raise HTTPException(status_code=400, detail="Negative metrics")
    if free_bytes > total_bytes:
        raise HTTPException(status_code=400, detail="Invalid disk metrics")

    now = datetime.now(timezone.utc)
    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO node_heartbeats (node_id, free_bytes, total_bytes,
                                         chunk_count, stored_bytes,
                                         last_seen, url)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (node_id) DO UPDATE SET
                url = COALESCE(EXCLUDED.url, node_heartbeats.url),
                free_bytes = EXCLUDED.free_bytes,
                total_bytes = EXCLUDED.total_bytes,
                chunk_count = EXCLUDED.chunk_count,
                stored_bytes = EXCLUDED.stored_bytes,
                last_seen = EXCLUDED.last_seen
            """,
            (node_id, free_bytes, total_bytes, chunk_count, stored_bytes,
             url.rstrip("/") if url else None))
    return {"accepted": True, "node_id": node_id, "last_seen": now.isoformat()}


@app.get("/v1/nodes")
def get_nodes():
    """Cluster membership as seen through heartbeats."""
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT node_id, free_bytes, total_bytes, chunk_count, "
            "stored_bytes, last_seen, url, "
            "EXTRACT(EPOCH FROM (NOW() - last_seen)) AS age "
            "FROM node_heartbeats").fetchall()
    seen = {row["node_id"]: row for row in rows}
    ownership = cluster.RING.ownership()

    results = []
    for node_id in sorted(set(cluster.NODES) | set(seen)):
        row = seen.get(node_id)
        in_ring = node_id in cluster.NODES
        url = cluster.NODES.get(node_id) or cluster.KNOWN_URLS.get(node_id)
        if row is None:
            results.append({"node_id": node_id, "url": url,
                            "in_ring": in_ring, "status": "unknown"})
            continue
        age = max(0.0, float(row["age"]))
        results.append({
            "node_id": node_id,
            "url": url,
            "in_ring": in_ring,
            "ring_share": round(ownership.get(node_id, 0.0), 4),
            "status": "healthy" if age <= config.HEARTBEAT_TIMEOUT else "failed",
            "free_bytes": row["free_bytes"],
            "total_bytes": row["total_bytes"],
            "chunk_count": row["chunk_count"],
            "stored_bytes": row["stored_bytes"],
            "last_seen": row["last_seen"].isoformat(),
            "heartbeat_age_seconds": round(age, 2),
        })
    return {"heartbeat_timeout_seconds": config.HEARTBEAT_TIMEOUT,
            "nodes": results}


# ---------------------------------------------------------------------------
# Buckets (each bucket has its own redundancy policy)
# ---------------------------------------------------------------------------

class BucketIn(BaseModel):
    name: str
    redundancy: str = config.DEFAULT_REDUNDANCY       # "replication" | "ec"
    k: int = config.EC_K
    m: int = config.EC_M


@app.post("/v1/buckets", status_code=201)
def create_bucket(bucket: BucketIn):
    if not BUCKET_RE.fullmatch(bucket.name):
        raise HTTPException(status_code=400,
                            detail="Bucket names: 2-63 chars, a-z 0-9 . -")
    if bucket.redundancy not in ("replication", "ec"):
        raise HTTPException(status_code=400,
                            detail="redundancy must be 'replication' or 'ec'")
    k = m = None
    if bucket.redundancy == "ec":
        k, m = bucket.k, bucket.m
        if k < 1 or m < 1 or k + m > 255:
            raise HTTPException(status_code=400, detail="Invalid k / m")
        # Fails early if the cluster is too small for this k+m.
        cluster.ec_assignment("0" * 64, k, m)
    with connect_db() as conn:
        row = conn.execute(
            "INSERT INTO buckets (name, redundancy, ec_k, ec_m) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING "
            "RETURNING *", (bucket.name, bucket.redundancy, k, m)).fetchone()
    if row is None:
        raise HTTPException(status_code=409, detail="Bucket already exists")
    return describe_bucket(row)


@app.get("/v1/buckets")
def list_buckets():
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM buckets ORDER BY name").fetchall()
        counts = conn.execute(
            "SELECT bucket, COUNT(*) AS objects, SUM(size) AS bytes "
            "FROM object_versions GROUP BY bucket").fetchall()
    counts = {c["bucket"]: c for c in counts}
    out = []
    for row in rows:
        b = describe_bucket(row)
        c = counts.get(row["name"])
        b["objects"] = c["objects"] if c else 0
        b["logical_bytes"] = int(c["bytes"]) if c else 0
        out.append(b)
    return {"buckets": out}


RAW_BYTES_SQL = """
    CASE WHEN c.scheme = 'ec'
         THEN (SELECT COALESCE(SUM((s->>'size')::bigint), 0)
               FROM jsonb_array_elements(c.shards) s)
         ELSE c.size * jsonb_array_length(c.nodes) END
"""


@app.get("/v1/buckets/{name}/stats")
def bucket_stats(name: str):
    """Storage overhead of one bucket: raw bytes on disk / unique bytes."""
    policy = objects.bucket_policy(name)
    if policy is None:
        raise HTTPException(status_code=404, detail="Bucket not found")
    with connect_db() as conn:
        logical = conn.execute(
            "SELECT COUNT(*) AS objects, COALESCE(SUM(size), 0) AS bytes "
            "FROM object_versions WHERE bucket = %s", (name,)).fetchone()
        rows = conn.execute(
            f"""
            WITH hs AS (
                SELECT DISTINCT jsonb_array_elements_text(chunk_hashes) AS h
                FROM object_versions WHERE bucket = %s)
            SELECT c.scheme, COUNT(*) AS chunks,
                   COALESCE(SUM(c.size), 0) AS unique_bytes,
                   COALESCE(SUM({RAW_BYTES_SQL}), 0) AS raw_bytes
            FROM hs JOIN chunk_catalog c
                 ON c.chunk_hash = hs.h AND c.policy = %s
            GROUP BY c.scheme
            """, (name, policy_key(policy))).fetchall()
    unique = sum(int(r["unique_bytes"]) for r in rows)
    raw = sum(int(r["raw_bytes"]) for r in rows)
    return {
        "bucket": describe_bucket(policy),
        "objects": logical["objects"],
        "logical_bytes": int(logical["bytes"]),
        "unique_bytes": unique,
        "raw_bytes_on_disk": raw,
        "storage_overhead": round(raw / unique, 3) if unique else None,
        "by_scheme": [{k: (int(v) if k != "scheme" else v)
                       for k, v in r.items()} for r in rows],
    }


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------

async def _upload_blocks(file: UploadFile):
    while True:
        block = await file.read(1024 * 1024)
        if not block:
            return
        yield block


@app.post("/v1/files", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    bucket: str = Form("default"),
    key: str | None = Form(None),
    chunking: str = Form(config.DEFAULT_CHUNKING),
    tags: str | None = Form(None),
):
    """Multipart-form upload (used by the CLI client and the web UI)."""
    object_key = key or file.filename or "download.bin"
    if not bucket.strip() or not object_key.strip():
        raise HTTPException(status_code=400,
                            detail="Bucket and key cannot be empty")
    try:
        return await objects.put_object(
            bucket, object_key, _upload_blocks(file), chunking=chunking,
            content_type=file.content_type, tags=objects.parse_tags(tags),
            create_bucket=True)
    finally:
        await file.close()


def load_by_id(file_id: str) -> dict:
    object_id = parse_object_id(file_id)
    with connect_db() as conn:
        obj = conn.execute("SELECT * FROM object_versions WHERE object_id = %s",
                           (object_id,)).fetchone()
    if obj is None:
        raise HTTPException(status_code=404, detail="File not found")
    return obj


@app.get("/v1/files/{file_id}")
def get_manifest(file_id: str):
    obj = load_by_id(file_id)
    return {**objects.describe_version(obj), "chunks": objects.chunks_of(obj)}


@app.get("/v1/files/{file_id}/download")
async def download_file(file_id: str):
    obj = load_by_id(file_id)
    chunks = objects.chunks_of(obj)
    # Check availability BEFORE streaming, so the client gets a clean 503
    # instead of a connection cut half-way through the file.
    objects.check_available(chunks)
    filename = (obj["object_key"].rsplit("/", 1)[-1].replace('"', "")
                or "download.bin")
    return StreamingResponse(
        objects.stream_plan(objects.plan_read(chunks, None),
                            obj["sha256"], obj["size"]),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                 "Content-Length": str(obj["size"]),
                 "X-File-SHA256": obj["sha256"] or ""})


@app.delete("/v1/files/{file_id}")
async def delete_file(file_id: str):
    """Delete one object version; chunks whose ref_count reaches 0 are
    removed from the storage nodes."""
    object_id = parse_object_id(file_id)
    result = await objects.delete_versions("object_id = %s", (object_id,))
    if not result["versions_deleted"]:
        raise HTTPException(status_code=404, detail="File not found")
    return {"file_id": str(object_id), "deleted": True, **result}


# ---------------------------------------------------------------------------
# Ring inspection and placement experiments
# ---------------------------------------------------------------------------

@app.get("/v1/ring")
def ring_info():
    return {"vnodes_per_node": config.VNODES,
            "replication_factor": config.REPLICATION_FACTOR,
            "members": cluster.NODES,
            "ownership": {n: round(v, 4)
                          for n, v in cluster.RING.ownership().items()}}


def movement_report(hashes, before_nodes, after_nodes, n):
    """Compare placements before/after a membership change."""
    ring_before = ConsistentHashRing(before_nodes, vnodes=config.VNODES)
    ring_after = ConsistentHashRing(after_nodes, vnodes=config.VNODES)
    touched = copies_moved = mod_touched = mod_copies = 0
    primary = mod_primary = 0
    for h in hashes:
        primary += ring_before.get_nodes(h, 1) != ring_after.get_nodes(h, 1)
        mod_primary += (modulo_placement(h, list(before_nodes), 1)
                        != modulo_placement(h, list(after_nodes), 1))
        b = set(ring_before.get_nodes(h, n))
        a = set(ring_after.get_nodes(h, n))
        moved = len(a - b)
        copies_moved += moved
        touched += moved > 0
        m = len(set(modulo_placement(h, list(after_nodes), n))
                - set(modulo_placement(h, list(before_nodes), n)))
        mod_copies += m
        mod_touched += m > 0

    total = max(len(hashes), 1)
    copies = total * n
    return {
        "chunks": len(hashes),
        "replication_factor": n,
        "nodes_before": sorted(before_nodes),
        "nodes_after": sorted(after_nodes),
        "consistent_hashing": {
            "primary_owner_changed_pct": round(100 * primary / total, 2),
            "chunks_with_a_moved_copy_pct": round(100 * touched / total, 2),
            "chunk_copies_moved_pct": round(100 * copies_moved / copies, 2)},
        "modulo_hashing_baseline": {
            "primary_owner_changed_pct": round(100 * mod_primary / total, 2),
            "chunks_with_a_moved_copy_pct": round(100 * mod_touched / total, 2),
            "chunk_copies_moved_pct": round(100 * mod_copies / copies, 2)},
        "theory_copies_moved_pct": round(
            100 * abs(len(after_nodes) - len(before_nodes))
            / max(len(after_nodes), len(before_nodes)), 2),
    }


@app.get("/v1/placement/simulate")
def simulate_placement(add: str | None = None, remove: str | None = None):
    """What-if: how many chunks would move if a node joined or left?"""
    before = sorted(cluster.NODES)
    after = set(before)
    if add:
        after.add(add)
    if remove:
        after.discard(remove)
    if len(after) < config.REPLICATION_FACTOR:
        raise HTTPException(status_code=400,
                            detail="Not enough nodes for the replication factor")
    with connect_db() as conn:
        hashes = [r["chunk_hash"] for r in
                  conn.execute("SELECT chunk_hash FROM chunk_catalog").fetchall()]
    return movement_report(hashes, before, sorted(after),
                           config.REPLICATION_FACTOR)


# ---------------------------------------------------------------------------
# Membership changes and rebalancing
# ---------------------------------------------------------------------------

class NodeIn(BaseModel):
    node_id: str
    url: str | None = None


@app.post("/v1/admin/nodes", status_code=201)
def add_ring_node(node: NodeIn,
                  x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    if not NODE_ID_RE.fullmatch(node.node_id):
        raise HTTPException(status_code=400, detail="Invalid node id")
    url = (node.url or cluster.KNOWN_URLS.get(node.node_id) or "").rstrip("/")
    if not url:
        raise HTTPException(status_code=400,
                            detail="URL unknown: pass it or let the node "
                                   "send a heartbeat first")
    with connect_db() as conn:
        conn.execute(
            "INSERT INTO ring_members (node_id, url) VALUES (%s, %s) "
            "ON CONFLICT (node_id) DO UPDATE SET url = EXCLUDED.url",
            (node.node_id, url))
        conn.execute("DELETE FROM decommissioned_nodes WHERE node_id = %s",
                     (node.node_id,))
    cluster.load_membership()
    return {"added": node.node_id, "url": url,
            "members": sorted(cluster.NODES),
            "next_step": "POST /v1/admin/rebalance to move chunks"}


@app.delete("/v1/admin/nodes/{node_id}")
def remove_ring_node(node_id: str,
                     x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    if node_id not in cluster.NODES:
        raise HTTPException(status_code=404, detail="Node is not in the ring")
    if len(cluster.NODES) - 1 < config.REPLICATION_FACTOR:
        raise HTTPException(status_code=400,
                            detail="Removing it would leave fewer nodes "
                                   "than the replication factor")
    with connect_db() as conn:
        conn.execute("DELETE FROM ring_members WHERE node_id = %s", (node_id,))
        # Remember the decision, so AUTO_JOIN does not add it back while it
        # keeps sending heartbeats during its decommissioning.
        conn.execute("INSERT INTO decommissioned_nodes (node_id) VALUES (%s) "
                     "ON CONFLICT DO NOTHING", (node_id,))
    cluster.load_membership()
    return {"removed": node_id, "members": sorted(cluster.NODES),
            "next_step": "POST /v1/admin/rebalance to move chunks"}


def auto_join(node_id: str, url: str) -> None:
    """A new node announced itself: add it to the ring and schedule a
    rebalance (debounced, so several new nodes produce one rebalance)."""
    with connect_db() as conn:
        if conn.execute("SELECT 1 FROM decommissioned_nodes WHERE node_id = %s",
                        (node_id,)).fetchone():
            return
        conn.execute("INSERT INTO ring_members (node_id, url) VALUES (%s, %s) "
                     "ON CONFLICT (node_id) DO NOTHING", (node_id, url))
    cluster.load_membership()
    import time as _time
    repair.PENDING_REBALANCE["at"] = _time.monotonic() + config.REBALANCE_DELAY
    repair.PENDING_REBALANCE["nodes"].append(node_id)
    print(f"[membership] {node_id} joined the ring automatically; rebalance in "
          f"{config.REBALANCE_DELAY:.0f} s", flush=True)


@app.post("/v1/admin/rebalance")
async def rebalance(dry_run: bool = False, wait: bool = True,
                    x_cluster_token: str | None = Header(default=None)):
    """Queue a rebalance job (moves to workers). With wait=true (default)
    the call returns when the job has finished (or after 10 minutes)."""
    check_cluster_token(x_cluster_token)
    if not dry_run and jobs.active_job("rebalance"):
        raise HTTPException(status_code=409, detail="Rebalance already running")
    job = await repair.start_rebalance("manual", dry_run=dry_run)
    if dry_run or not wait:
        return repair.rebalance_summary(job) if not dry_run else job
    deadline = asyncio.get_running_loop().time() + 600
    while job["status"] == "running" and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.5)
        job = jobs.describe(jobs.get_job(job["job_id"]))
    return repair.rebalance_summary(job)


@app.get("/v1/admin/rebalance")
def last_rebalance():
    recent = jobs.recent_jobs("rebalance", 1)
    return repair.rebalance_summary(jobs.describe(recent[0])) if recent \
        else {"status": "never run"}


@app.get("/v1/jobs")
def list_jobs(type: str | None = None, limit: int = 20):
    """Recent background jobs (repair, rebalance, gc) and the queue state."""
    return {"backend": config.JOB_BACKEND, "queue": jobs.STATE,
            "pending_batches_in_postgres": jobs.queue_length(),
            "jobs": [jobs.describe(j) for j in jobs.recent_jobs(type, min(limit, 100))]}


@app.get("/v1/jobs/{job_id}")
def get_job(job_id: str):
    try:
        row = jobs.get_job(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job id")
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs.describe(row)


# ---------------------------------------------------------------------------
# Failure detection, repair and scrubbing
# ---------------------------------------------------------------------------

@app.get("/v1/repair/status")
def repair_status():
    view = repair.status_view()
    return {"failed_nodes": sorted(cluster.failed_nodes()),
            "state": view["state"],
            "current_job": view["current"],
            "totals": view["totals"],
            "history": view["history"],
            "recent_scrub_reports": repair.SCRUB_REPORTS[:10]}


@app.post("/v1/admin/repair")
async def trigger_repair(x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    job = await repair.run_repair("manual")
    return job or {"status": "nothing to repair (or a repair is running)",
                   **repair.status_view()}


class ScrubReport(BaseModel):
    node_id: str
    corrupted: list[str]


@app.post("/v1/scrub/report")
def scrub_report(report: ScrubReport,
                 x_cluster_token: str | None = Header(default=None)):
    """Called by a storage node whose scrubber found corrupted chunks."""
    check_cluster_token(x_cluster_token)
    return repair.handle_scrub_report(report.node_id, report.corrupted)


@app.post("/v1/admin/scrub")
async def scrub_all(x_cluster_token: str | None = Header(default=None)):
    """Run a scrub pass on every live node now."""
    check_cluster_token(x_cluster_token)
    started = datetime.now(timezone.utc)
    results = {}
    async with httpx.AsyncClient(timeout=600) as client:
        for node in sorted(set(cluster.NODES) - set(cluster.failed_nodes(True))):
            try:
                r = await client.post(f"{cluster.node_url(node)}/v1/scrub",
                                      headers=cluster.node_headers())
                results[node] = r.json()
            except (httpx.HTTPError, ValueError) as exc:
                results[node] = {"error": str(exc)}
    await asyncio.sleep(0.5)      # let the reports reach us
    job = await repair.run_repair("scrub")
    if job is None:
        # The background loop may already have queued the repair.
        async with repair.REPAIR_LOCK:
            pass
        recent = jobs.recent_jobs("repair", 1)
        if recent and recent[0]["created_at"] >= started:
            job = jobs.describe(recent[0])
    if job and job["status"] == "running":       # wait for the workers
        for _ in range(120):
            await asyncio.sleep(0.5)
            job = jobs.describe(jobs.get_job(job["job_id"]))
            if job["status"] != "running":
                break
    return {"scrub": results, "repair": job}


class CorruptIn(BaseModel):
    node_id: str
    hash: str


@app.post("/v1/admin/fault/corrupt")
async def inject_corruption(req: CorruptIn,
                            x_cluster_token: str | None = Header(default=None)):
    """Demo helper: flip a byte of a stored blob on one node (bit rot)."""
    check_cluster_token(x_cluster_token)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{cluster.node_url(req.node_id)}/v1/debug/corrupt/{req.hash}",
            headers=cluster.node_headers())
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


@app.post("/v1/admin/gc")
async def garbage_collect(min_age_seconds: float = 600, dry_run: bool = False,
                          x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    return await repair.collect_orphans(min_age_seconds, dry_run)


# ---------------------------------------------------------------------------
# Statistics and cluster status
# ---------------------------------------------------------------------------

@app.get("/v1/stats")
def storage_stats():
    with connect_db() as conn:
        objects = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes, "
            "COALESCE(SUM(jsonb_array_length(chunk_hashes)), 0) AS refs "
            "FROM object_versions").fetchone()
        schemes = conn.execute(
            f"SELECT c.scheme, COUNT(*) AS chunks, "
            f"COALESCE(SUM(c.size), 0) AS unique_bytes, "
            f"COALESCE(SUM({RAW_BYTES_SQL}), 0) AS raw_bytes "
            f"FROM chunk_catalog c GROUP BY c.scheme").fetchall()
        by_method = conn.execute(
            "SELECT chunking, COUNT(*) AS objects, SUM(size) AS bytes "
            "FROM object_versions GROUP BY chunking").fetchall()

    logical = int(objects["bytes"])
    unique = sum(int(r["unique_bytes"]) for r in schemes)
    raw = sum(int(r["raw_bytes"]) for r in schemes)
    return {
        "objects": objects["n"],
        "logical_bytes": logical,
        "chunk_references": int(objects["refs"]),
        "unique_chunks": sum(r["chunks"] for r in schemes),
        "unique_bytes": unique,
        "raw_bytes_on_disk": raw,
        "dedup_ratio": round(logical / unique, 3) if unique else None,
        "space_saved_by_dedup_pct": round(100 * (1 - unique / logical), 2)
        if logical else 0.0,
        "by_scheme": [{"scheme": r["scheme"], "chunks": r["chunks"],
                       "unique_bytes": int(r["unique_bytes"]),
                       "raw_bytes": int(r["raw_bytes"]),
                       "overhead": round(int(r["raw_bytes"])
                                         / max(int(r["unique_bytes"]), 1), 3)}
                      for r in schemes],
        "by_chunking": [{"chunking": r["chunking"], "objects": r["objects"],
                         "bytes": int(r["bytes"] or 0)} for r in by_method],
    }


@app.get("/v1/cluster/status")
def cluster_status():
    """Nodes, free space, blobs per node, degraded objects, repair state."""
    nodes = get_nodes()["nodes"]
    health_info = cached_health()
    view = repair.status_view()
    return {
        "nodes": nodes,
        "total_free_bytes": sum(n.get("free_bytes", 0) for n in nodes
                                if n.get("status") == "healthy"),
        **health_info,
        "repair": {"state": view["state"],
                   "current_job": view["current"],
                   "last_job": (view["history"] or [None])[0]},
        "jobs": {"backend": config.JOB_BACKEND,
                 "pending_batches": jobs.queue_length()},
    }


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_HEALTH_CACHE = {"at": 0.0, "value": None}


def cached_health() -> dict:
    """health_summary() scans the whole catalog: cache it for 5 seconds."""
    import time as _time
    now = _time.monotonic()
    if _HEALTH_CACHE["value"] is None or now - _HEALTH_CACHE["at"] > 5:
        _HEALTH_CACHE.update(at=now, value=repair.health_summary())
    return _HEALTH_CACHE["value"]


def _node_rows():
    with connect_db() as conn:
        return conn.execute(
            "SELECT node_id, free_bytes, total_bytes, stored_bytes, chunk_count, "
            "EXTRACT(EPOCH FROM (NOW() - last_seen)) AS age FROM node_heartbeats"
        ).fetchall()


def _per_node(field):
    def fn():
        return [({"node": r["node_id"]}, r[field]) for r in _node_rows()]
    return fn


def _health_value(field):
    return lambda: [({}, cached_health()[field])]


def _repair_totals(field):
    def fn():
        totals = repair.status_view()["totals"]
        return [({"scheme": s}, totals[s][field]) for s in ("replication", "ec")]
    return fn


def _storage(field):
    def fn():
        s = storage_stats()
        return [({}, s[field])]
    return fn


REGISTRY.gauge("ds_node_up", "1 if the node's heartbeat is recent", ["node"],
               fn=lambda: [({"node": r["node_id"]},
                            int(float(r["age"]) <= config.HEARTBEAT_TIMEOUT))
                           for r in _node_rows()])
REGISTRY.gauge("ds_node_free_bytes", "Free disk space per node", ["node"],
               fn=_per_node("free_bytes"))
REGISTRY.gauge("ds_node_total_bytes", "Disk size per node", ["node"],
               fn=_per_node("total_bytes"))
REGISTRY.gauge("ds_node_stored_bytes", "Bytes of chunks stored per node", ["node"],
               fn=_per_node("stored_bytes"))
REGISTRY.gauge("ds_node_blobs", "Blobs (replicas + shards) stored per node",
               ["node"], fn=_per_node("chunk_count"))
REGISTRY.gauge("ds_ring_members", "Nodes in the consistent hashing ring",
               fn=lambda: [({}, len(cluster.NODES))])
for _field, _help in (("objects_total", "Objects (versions)"),
                      ("chunks_total", "Unique chunks"),
                      ("objects_degraded", "Objects below full redundancy"),
                      ("objects_unavailable", "Objects that cannot be read"),
                      ("chunks_degraded", "Chunks below full redundancy"),
                      ("chunks_unavailable", "Chunks that cannot be read")):
    REGISTRY.gauge(f"ds_{_field}", _help, fn=_health_value(_field))
REGISTRY.gauge("ds_logical_bytes", "Bytes uploaded by users (all versions)",
               fn=_storage("logical_bytes"))
REGISTRY.gauge("ds_raw_bytes", "Bytes on disk including redundancy",
               fn=_storage("raw_bytes_on_disk"))
for _field in ("bytes_read", "bytes_written", "chunks_repaired",
               "copies_or_shards_rebuilt", "chunks_lost"):
    REGISTRY.gauge(f"ds_repair_{_field}_total",
                   f"Repair {_field.replace('_', ' ')} since the beginning "
                   "(monotonic, use rate())", ["scheme"],
                   fn=_repair_totals(_field))
REGISTRY.gauge("ds_jobs_pending_batches", "Batches waiting in the PostgreSQL queue",
               fn=lambda: [({}, jobs.queue_length())])
REGISTRY.gauge("ds_jobs_running", "Running jobs per type", ["type"],
               fn=lambda: [({"type": t}, int(jobs.active_job(t) is not None))
                           for t in ("repair", "rebalance", "gc")])


@app.get("/metrics")
def metrics():
    return Response(REGISTRY.render(), media_type="text/plain; version=0.0.4")
