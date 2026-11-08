import asyncio
import hashlib
import json
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
import psycopg
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

from coordinator.chunking import iter_chunks
from coordinator.ring import ConsistentHashRing, modulo_placement

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ["DATABASE_URL"]
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "dev-cluster-token-change-me")


def parse_nodes(raw: str) -> dict[str, str]:
    """Parse NODE_URLS into {node_id: url}.

    Accepted formats (comma separated):
      node1=http://storage-node-1:8000   (explicit id)
      http://storage-node-1:8000         (id defaults to node<position>)
    """
    nodes: dict[str, str] = {}
    entries = [e.strip() for e in raw.split(",") if e.strip()]
    for position, entry in enumerate(entries, start=1):
        if "=" in entry:
            node_id, url = entry.split("=", 1)
        else:
            node_id, url = f"node{position}", entry
        nodes[node_id.strip()] = url.strip().rstrip("/")
    return nodes


# Initial ring members; later changes go through /v1/admin/nodes and are
# persisted in the ring_members table.
SEED_NODES = parse_nodes(os.getenv("NODE_URLS", ""))
VNODES = int(os.getenv("VNODES", "128"))
DEFAULT_CHUNKING = os.getenv("DEFAULT_CHUNKING", "fixed")
NODE_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,32}")

# In-memory view of the cluster, rebuilt from the database on every change.
NODES: dict[str, str] = dict(SEED_NODES)       # ring members: id -> url
KNOWN_URLS: dict[str, str] = dict(SEED_NODES)  # every node ever seen
RING = ConsistentHashRing(NODES, vnodes=VNODES)
MEMBERSHIP_LOCK = threading.Lock()

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(1024 * 1024 * 1024)))
# A node is considered failed after this many seconds without a heartbeat
# (default = 3 missed heartbeats at a 5 s interval).
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "15"))

if CHUNK_SIZE <= 0 or MAX_FILE_SIZE < 0:
    raise RuntimeError("Invalid size configuration")

if REPLICATION_FACTOR < 1 or REPLICATION_FACTOR > len(SEED_NODES):
    raise RuntimeError("Invalid replication factor or insufficient nodes")


def node_url(node: str) -> str:
    """Resolve a node id to its URL (also accepts a URL, for old rows)."""
    if node.startswith("http://") or node.startswith("https://"):
        return node.rstrip("/")
    url = NODES.get(node) or KNOWN_URLS.get(node)
    if url is None:
        raise HTTPException(status_code=500, detail=f"Unknown node id {node}")
    return url


def node_headers():
    return {"X-Cluster-Token": CLUSTER_TOKEN}


def check_cluster_token(token: str | None) -> None:
    if not token or token != CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def parse_object_id(object_id: str) -> UUID:
    try:
        return UUID(object_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file ID")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def connect_db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    with connect_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS node_heartbeats (
                node_id TEXT PRIMARY KEY,
                free_bytes BIGINT NOT NULL,
                total_bytes BIGINT NOT NULL,
                chunk_count BIGINT NOT NULL,
                stored_bytes BIGINT NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL
            )
        """)

        # One row per version of an object (S3 model: bucket + key + version).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS object_versions (
                bucket TEXT NOT NULL,
                object_key TEXT NOT NULL,
                version BIGINT NOT NULL,
                size BIGINT NOT NULL CHECK (size >= 0),
                chunk_hashes JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (bucket, object_key, version),
                CHECK (jsonb_typeof(chunk_hashes) = 'array')
            )
        """)
        # Columns added after the first version of the schema.
        conn.execute(
            "ALTER TABLE object_versions "
            "ADD COLUMN IF NOT EXISTS object_id UUID UNIQUE"
        )
        conn.execute(
            "ALTER TABLE object_versions "
            "ADD COLUMN IF NOT EXISTS sha256 TEXT"
        )
        conn.execute(
            "ALTER TABLE object_versions "
            "ADD COLUMN IF NOT EXISTS chunking TEXT NOT NULL DEFAULT 'fixed'"
        )
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_object_versions_created_at
            ON object_versions (created_at DESC)
        """)

        # One row per unique chunk: where it lives and how many objects use it.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chunk_catalog (
                chunk_hash TEXT PRIMARY KEY,
                size BIGINT NOT NULL CHECK (size >= 0),
                ref_count BIGINT NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
                nodes JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (jsonb_typeof(nodes) = 'array')
            )
        """)

        conn.execute(
            "ALTER TABLE node_heartbeats ADD COLUMN IF NOT EXISTS url TEXT"
        )

        # Membership of the consistent hashing ring.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ring_members (
                node_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Seed the ring only the very first time (empty table).
        if conn.execute("SELECT COUNT(*) AS n FROM ring_members"
                        ).fetchone()["n"] == 0:
            for node_id, url in SEED_NODES.items():
                conn.execute(
                    "INSERT INTO ring_members (node_id, url) VALUES (%s, %s)",
                    (node_id, url),
                )

        # The old file_id-based table is replaced by object_versions.
        conn.execute("DROP TABLE IF EXISTS manifests")

    load_membership()


def load_membership():
    """Rebuild NODES / KNOWN_URLS / RING from the database."""
    global NODES, RING
    with connect_db() as conn:
        members = conn.execute(
            "SELECT node_id, url FROM ring_members ORDER BY node_id"
        ).fetchall()
        seen = conn.execute(
            "SELECT node_id, url FROM node_heartbeats WHERE url IS NOT NULL"
        ).fetchall()

    with MEMBERSHIP_LOCK:
        for row in seen:
            KNOWN_URLS[row["node_id"]] = row["url"]
        NODES = {row["node_id"]: row["url"] for row in members}
        KNOWN_URLS.update(NODES)
        RING = ConsistentHashRing(NODES, vnodes=VNODES)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Distributed Content-Addressed Storage",
    version="1.2.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health and node membership
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "node_count": len(NODES),
        "replication_factor": REPLICATION_FACTOR,
    }


@app.post("/v1/nodes/heartbeat")
def receive_heartbeat(
    payload: dict,
    x_cluster_token: str | None = Header(default=None),
):
    check_cluster_token(x_cluster_token)

    required = {"node_id", "free_bytes", "total_bytes",
                "chunk_count", "stored_bytes"}
    if not required.issubset(payload):
        raise HTTPException(status_code=400, detail="Missing heartbeat fields")

    node_id = str(payload["node_id"])
    if not NODE_ID_RE.fullmatch(node_id):
        raise HTTPException(status_code=400, detail="Invalid node id")
    # Nodes that are not ring members are accepted and shown as "standby":
    # an operator can then add them to the ring with /v1/admin/nodes.
    url = payload.get("url")
    if url is not None and not re.fullmatch(r"https?://[^\s]+", str(url)):
        raise HTTPException(status_code=400, detail="Invalid node url")
    if url:
        KNOWN_URLS.setdefault(node_id, str(url).rstrip("/"))

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
            INSERT INTO node_heartbeats (
                node_id, free_bytes, total_bytes,
                chunk_count, stored_bytes, last_seen, url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET
                url = COALESCE(EXCLUDED.url, node_heartbeats.url),
                free_bytes = EXCLUDED.free_bytes,
                total_bytes = EXCLUDED.total_bytes,
                chunk_count = EXCLUDED.chunk_count,
                stored_bytes = EXCLUDED.stored_bytes,
                last_seen = EXCLUDED.last_seen
            """,
            (node_id, free_bytes, total_bytes, chunk_count, stored_bytes,
             now, url.rstrip("/") if url else None),
        )

    return {"accepted": True, "node_id": node_id, "last_seen": now.isoformat()}


@app.get("/v1/nodes")
def get_nodes():
    """Cluster membership as seen through heartbeats."""
    now = datetime.now(timezone.utc)

    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT node_id, free_bytes, total_bytes,
                   chunk_count, stored_bytes, last_seen, url
            FROM node_heartbeats
            """
        ).fetchall()
    seen = {row["node_id"]: row for row in rows}
    ownership = RING.ownership()

    results = []
    for node_id in sorted(set(NODES) | set(seen)):
        row = seen.get(node_id)
        in_ring = node_id in NODES
        url = NODES.get(node_id) or KNOWN_URLS.get(node_id)
        if row is None:
            results.append({"node_id": node_id, "url": url,
                            "in_ring": in_ring, "status": "unknown"})
            continue

        age = max(0.0, (now - row["last_seen"]).total_seconds())
        results.append({
            "node_id": node_id,
            "url": url,
            "in_ring": in_ring,
            "ring_share": round(ownership.get(node_id, 0.0), 4),
            "status": "healthy" if age <= HEARTBEAT_TIMEOUT else "failed",
            "free_bytes": row["free_bytes"],
            "total_bytes": row["total_bytes"],
            "chunk_count": row["chunk_count"],
            "stored_bytes": row["stored_bytes"],
            "last_seen": row["last_seen"].isoformat(),
            "heartbeat_age_seconds": round(age, 2),
        })

    return {"heartbeat_timeout_seconds": HEARTBEAT_TIMEOUT, "nodes": results}


# ---------------------------------------------------------------------------
# Placement: consistent hashing ring
# ---------------------------------------------------------------------------

def choose_nodes(digest: str) -> list[str]:
    """Replica set of a chunk = first N distinct nodes clockwise on the ring."""
    return RING.get_nodes(digest, REPLICATION_FACTOR)


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------

@app.post("/v1/files", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    bucket: str = Form("default"),
    key: str | None = Form(None),
    chunking: str = Form(DEFAULT_CHUNKING),
):
    if chunking not in ("fixed", "cdc"):
        raise HTTPException(status_code=400,
                            detail="chunking must be 'fixed' or 'cdc'")
    object_id = uuid4()
    file_hash = hashlib.sha256()
    file_size = 0
    chunks = []          # ordered list of {"sha256", "size", "nodes"}
    new_chunks = 0
    deduplicated_chunks = 0

    object_key = key or file.filename or "download.bin"
    if not bucket.strip() or not object_key.strip():
        raise HTTPException(status_code=400,
                            detail="Bucket and key cannot be empty")

    try:
        chunk_iter = iter_chunks(file.file, chunking, CHUNK_SIZE)
        async with httpx.AsyncClient(timeout=120) as client:
            while True:
                # Chunking (especially CDC) is CPU-bound: run it in a worker
                # thread so the event loop keeps serving heartbeats.
                block = await asyncio.to_thread(next, chunk_iter, None)
                if block is None:
                    break

                file_size += len(block)
                if file_size > MAX_FILE_SIZE:
                    raise HTTPException(status_code=413,
                                        detail="File exceeds maximum size")

                digest = hashlib.sha256(block).hexdigest()
                file_hash.update(block)

                # Deduplication: is this chunk already stored in the cluster?
                with connect_db() as conn:
                    existing = conn.execute(
                        """
                        SELECT size, nodes FROM chunk_catalog
                        WHERE chunk_hash = %s AND ref_count > 0
                        """,
                        (digest,),
                    ).fetchone()

                if existing:
                    if existing["size"] != len(block):
                        raise HTTPException(
                            status_code=500,
                            detail="Chunk catalog integrity error",
                        )
                    # Identical content already stored: reuse it, send nothing.
                    chunks.append({"sha256": digest, "size": len(block),
                                   "nodes": existing["nodes"]})
                    deduplicated_chunks += 1
                    continue

                target_nodes = choose_nodes(digest)

                for node_id in target_nodes:
                    response = await client.put(
                        f"{node_url(node_id)}/v1/chunks/{digest}",
                        content=block,
                        headers={**node_headers(),
                                 "Content-Type": "application/octet-stream"},
                    )
                    if response.status_code not in (200, 201):
                        raise HTTPException(
                            status_code=502,
                            detail=f"Chunk replication failed on {node_id}",
                        )

                chunks.append({"sha256": digest, "size": len(block),
                               "nodes": target_nodes})
                new_chunks += 1

        # Commit metadata in a single transaction.
        with connect_db() as conn:
            for chunk in chunks:
                conn.execute(
                    """
                    INSERT INTO chunk_catalog (chunk_hash, size, ref_count, nodes)
                    VALUES (%s, %s, 1, %s::jsonb)
                    ON CONFLICT (chunk_hash) DO UPDATE
                    SET ref_count = chunk_catalog.ref_count + 1
                    """,
                    (chunk["sha256"], chunk["size"], json.dumps(chunk["nodes"])),
                )

            # Serialize version numbering for this bucket/key.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                (bucket, object_key),
            )
            row = conn.execute(
                """
                INSERT INTO object_versions
                    (object_id, bucket, object_key, version, size,
                     sha256, chunk_hashes, chunking)
                VALUES (
                    %s, %s, %s,
                    COALESCE((SELECT MAX(version) FROM object_versions
                              WHERE bucket = %s AND object_key = %s), 0) + 1,
                    %s, %s, %s::jsonb, %s
                )
                RETURNING version, created_at
                """,
                (
                    object_id, bucket, object_key, bucket, object_key,
                    file_size, file_hash.hexdigest(),
                    json.dumps([c["sha256"] for c in chunks]),
                    chunking,
                ),
            ).fetchone()

        return {
            "file_id": str(object_id),
            "bucket": bucket,
            "key": object_key,
            "version": row["version"],
            "size": file_size,
            "sha256": file_hash.hexdigest(),
            "created_at": row["created_at"].isoformat(),
            "replication_factor": REPLICATION_FACTOR,
            "chunking": chunking,
            "chunk_count": len(chunks),
            "new_chunks": new_chunks,
            "deduplicated_chunks": deduplicated_chunks,
            "chunks": chunks,
        }

    finally:
        await file.close()


def load_object(object_id: UUID):
    """Return the object row plus its chunks (hash, size, nodes) in order."""
    with connect_db() as conn:
        obj = conn.execute(
            "SELECT * FROM object_versions WHERE object_id = %s",
            (object_id,),
        ).fetchone()
        if obj is None:
            raise HTTPException(status_code=404, detail="File not found")

        hashes = obj["chunk_hashes"]
        rows = conn.execute(
            """
            SELECT chunk_hash, size, nodes FROM chunk_catalog
            WHERE chunk_hash = ANY(%s)
            """,
            (list(set(hashes)),),
        ).fetchall()

    catalog = {r["chunk_hash"]: r for r in rows}
    missing = [h for h in hashes if h not in catalog]
    if missing:
        raise HTTPException(status_code=500,
                            detail=f"Chunk catalog missing {missing[0]}")

    chunks = [{"sha256": h, "size": catalog[h]["size"],
               "nodes": catalog[h]["nodes"]} for h in hashes]
    return obj, chunks


@app.get("/v1/files/{file_id}")
def get_manifest(file_id: str):
    obj, chunks = load_object(parse_object_id(file_id))
    return {
        "file_id": str(obj["object_id"]),
        "bucket": obj["bucket"],
        "key": obj["object_key"],
        "version": obj["version"],
        "size": obj["size"],
        "sha256": obj["sha256"],
        "chunking": obj["chunking"],
        "created_at": obj["created_at"].isoformat(),
        "chunks": chunks,
    }


@app.get("/v1/files/{file_id}/download")
async def download_file(file_id: str):
    obj, chunks = load_object(parse_object_id(file_id))
    expected_file_hash = obj["sha256"]
    expected_size = obj["size"]
    filename = obj["object_key"].rsplit("/", 1)[-1].replace('"', "") or "download.bin"

    async def stream_file():
        file_hasher = hashlib.sha256()
        total_size = 0

        async with httpx.AsyncClient(timeout=120) as client:
            for chunk in chunks:
                digest = chunk["sha256"]
                received = False

                # Try each replica until one returns a valid copy.
                for node in chunk["nodes"]:
                    try:
                        response = await client.get(
                            f"{node_url(node)}/v1/chunks/{digest}",
                            headers=node_headers(),
                        )
                    except httpx.HTTPError:
                        continue
                    if response.status_code != 200:
                        continue

                    data = response.content
                    if (hashlib.sha256(data).hexdigest() != digest
                            or len(data) != chunk["size"]):
                        continue

                    file_hasher.update(data)
                    total_size += len(data)
                    yield data
                    received = True
                    break

                if not received:
                    raise RuntimeError(
                        f"No valid replica available for chunk {digest}")

        if total_size != expected_size:
            raise RuntimeError("Reconstructed file size mismatch")
        if expected_file_hash and file_hasher.hexdigest() != expected_file_hash:
            raise RuntimeError("Reconstructed file SHA-256 mismatch")

    return StreamingResponse(
        stream_file(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(expected_size),
            "X-File-SHA256": expected_file_hash or "",
        },
    )


@app.delete("/v1/files/{file_id}")
async def delete_file(file_id: str):
    """Delete one object version.

    Each chunk's ref_count is decremented; a chunk is physically removed
    from the storage nodes only when no object references it any more.
    """
    object_id = parse_object_id(file_id)
    garbage = []

    with connect_db() as conn:
        obj = conn.execute(
            "DELETE FROM object_versions WHERE object_id = %s "
            "RETURNING chunk_hashes",
            (object_id,),
        ).fetchone()
        if obj is None:
            raise HTTPException(status_code=404, detail="File not found")

        for digest in obj["chunk_hashes"]:
            conn.execute(
                "UPDATE chunk_catalog SET ref_count = ref_count - 1 "
                "WHERE chunk_hash = %s AND ref_count > 0",
                (digest,),
            )

        garbage = conn.execute(
            "DELETE FROM chunk_catalog WHERE ref_count = 0 "
            "AND chunk_hash = ANY(%s) RETURNING chunk_hash, nodes",
            (list(set(obj["chunk_hashes"])),),
        ).fetchall()

    # Garbage-collect unreferenced chunks (best effort).
    removed = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for row in garbage:
            for node in row["nodes"]:
                try:
                    r = await client.delete(
                        f"{node_url(node)}/v1/chunks/{row['chunk_hash']}",
                        headers=node_headers(),
                    )
                    if r.status_code == 200:
                        removed += 1
                except httpx.HTTPError:
                    pass

    return {
        "file_id": str(object_id),
        "deleted": True,
        "chunks_released": len(obj["chunk_hashes"]),
        "chunks_garbage_collected": len(garbage),
        "chunk_copies_removed": removed,
    }


# ---------------------------------------------------------------------------
# Ring inspection and placement experiments
# ---------------------------------------------------------------------------

@app.get("/v1/ring")
def ring_info():
    return {
        "vnodes_per_node": VNODES,
        "replication_factor": REPLICATION_FACTOR,
        "members": NODES,
        "ownership": {n: round(v, 4) for n, v in RING.ownership().items()},
    }


def all_chunk_hashes() -> list[str]:
    with connect_db() as conn:
        rows = conn.execute("SELECT chunk_hash FROM chunk_catalog").fetchall()
    return [r["chunk_hash"] for r in rows]


def movement_report(hashes, before_nodes, after_nodes, n):
    """Compare placements before/after a membership change."""
    ring_before = ConsistentHashRing(before_nodes, vnodes=VNODES)
    ring_after = ConsistentHashRing(after_nodes, vnodes=VNODES)

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

        mb = set(modulo_placement(h, list(before_nodes), n))
        ma = set(modulo_placement(h, list(after_nodes), n))
        m = len(ma - mb)
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
            "chunk_copies_moved_pct": round(100 * copies_moved / copies, 2),
        },
        "modulo_hashing_baseline": {
            "primary_owner_changed_pct": round(100 * mod_primary / total, 2),
            "chunks_with_a_moved_copy_pct": round(100 * mod_touched / total, 2),
            "chunk_copies_moved_pct": round(100 * mod_copies / copies, 2),
        },
        "theory_copies_moved_pct": round(
            100 * abs(len(after_nodes) - len(before_nodes))
            / max(len(after_nodes), len(before_nodes)), 2),
    }


@app.get("/v1/placement/simulate")
def simulate_placement(add: str | None = None, remove: str | None = None):
    """What-if: how many chunks would move if a node joined or left?"""
    before = sorted(NODES)
    after = set(before)
    if add:
        after.add(add)
    if remove:
        after.discard(remove)
    if len(after) < REPLICATION_FACTOR:
        raise HTTPException(status_code=400,
                            detail="Not enough nodes for the replication factor")
    return movement_report(all_chunk_hashes(), before, sorted(after),
                           REPLICATION_FACTOR)


# ---------------------------------------------------------------------------
# Membership changes and rebalancing
# ---------------------------------------------------------------------------

class NodeIn(BaseModel):
    node_id: str
    url: str | None = None


@app.post("/v1/admin/nodes", status_code=201)
def add_ring_node(node: NodeIn, x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    if not NODE_ID_RE.fullmatch(node.node_id):
        raise HTTPException(status_code=400, detail="Invalid node id")
    url = (node.url or KNOWN_URLS.get(node.node_id) or "").rstrip("/")
    if not url:
        raise HTTPException(status_code=400,
                            detail="URL unknown: pass it or let the node "
                                   "send a heartbeat first")
    with connect_db() as conn:
        conn.execute(
            "INSERT INTO ring_members (node_id, url) VALUES (%s, %s) "
            "ON CONFLICT (node_id) DO UPDATE SET url = EXCLUDED.url",
            (node.node_id, url),
        )
    load_membership()
    return {"added": node.node_id, "url": url, "members": sorted(NODES),
            "next_step": "POST /v1/admin/rebalance to move chunks"}


@app.delete("/v1/admin/nodes/{node_id}")
def remove_ring_node(node_id: str,
                     x_cluster_token: str | None = Header(default=None)):
    check_cluster_token(x_cluster_token)
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail="Node is not in the ring")
    if len(NODES) - 1 < REPLICATION_FACTOR:
        raise HTTPException(status_code=400,
                            detail="Removing it would leave fewer nodes "
                                   "than the replication factor")
    with connect_db() as conn:
        conn.execute("DELETE FROM ring_members WHERE node_id = %s", (node_id,))
    load_membership()
    return {"removed": node_id, "members": sorted(NODES),
            "next_step": "POST /v1/admin/rebalance to move chunks"}


REBALANCE_LOCK = asyncio.Lock()
LAST_REBALANCE: dict = {}


async def fetch_valid_copy(client, digest, size, sources):
    for node in sources:
        try:
            r = await client.get(f"{node_url(node)}/v1/chunks/{digest}",
                                 headers=node_headers())
        except httpx.HTTPError:
            continue
        if (r.status_code == 200 and len(r.content) == size
                and hashlib.sha256(r.content).hexdigest() == digest):
            return r.content
    return None


@app.post("/v1/admin/rebalance")
async def rebalance(dry_run: bool = False,
                    x_cluster_token: str | None = Header(default=None)):
    """Move every chunk to the replica set the current ring assigns it.

    For each chunk: copy it to the new owners (from any valid replica),
    update the catalog, then delete it from nodes that no longer own it.
    """
    check_cluster_token(x_cluster_token)
    if REBALANCE_LOCK.locked():
        raise HTTPException(status_code=409, detail="Rebalance already running")

    async with REBALANCE_LOCK:
        started = datetime.now(timezone.utc)
        with connect_db() as conn:
            rows = conn.execute(
                "SELECT chunk_hash, size, nodes FROM chunk_catalog"
            ).fetchall()

        stats = {"chunks_total": len(rows), "chunks_moved": 0,
                 "copies_added": 0, "copies_removed": 0,
                 "bytes_transferred": 0, "failed_chunks": 0}

        async with httpx.AsyncClient(timeout=120) as client:
            for row in rows:
                digest, size = row["chunk_hash"], row["size"]
                current = list(row["nodes"])
                target = choose_nodes(digest)
                to_add = [n for n in target if n not in current]
                to_remove = [n for n in current if n not in target]
                if not to_add and not to_remove:
                    continue

                stats["chunks_moved"] += 1
                if dry_run:
                    stats["copies_added"] += len(to_add)
                    stats["copies_removed"] += len(to_remove)
                    continue

                ok = True
                if to_add:
                    data = await fetch_valid_copy(client, digest, size, current)
                    if data is None:
                        ok = False
                    else:
                        for node in to_add:
                            try:
                                r = await client.put(
                                    f"{node_url(node)}/v1/chunks/{digest}",
                                    content=data,
                                    headers={**node_headers(), "Content-Type":
                                             "application/octet-stream"})
                                copied = r.status_code in (200, 201)
                            except httpx.HTTPError:
                                copied = False
                            ok = ok and copied
                            if copied:
                                stats["copies_added"] += 1
                                stats["bytes_transferred"] += size
                if not ok:
                    stats["failed_chunks"] += 1
                    continue

                with connect_db() as conn:
                    conn.execute(
                        "UPDATE chunk_catalog SET nodes = %s::jsonb "
                        "WHERE chunk_hash = %s",
                        (json.dumps(target), digest),
                    )

                for node in to_remove:
                    try:
                        await client.delete(
                            f"{node_url(node)}/v1/chunks/{digest}",
                            headers=node_headers())
                        stats["copies_removed"] += 1
                    except httpx.HTTPError:
                        pass   # orphan copy; harmless, cleaned by a later GC

        total = max(stats["chunks_total"], 1)
        stats.update({
            "dry_run": dry_run,
            "members": sorted(NODES),
            "chunks_moved_pct": round(100 * stats["chunks_moved"] / total, 2),
            "copies_moved_pct": round(
                100 * stats["copies_added"] / (total * REPLICATION_FACTOR), 2),
            "started_at": started.isoformat(),
            "duration_seconds": round(
                (datetime.now(timezone.utc) - started).total_seconds(), 2),
        })
        LAST_REBALANCE.clear()
        LAST_REBALANCE.update(stats)
        return stats


@app.get("/v1/admin/rebalance")
def last_rebalance():
    return LAST_REBALANCE or {"status": "never run"}


# ---------------------------------------------------------------------------
# Storage statistics (deduplication)
# ---------------------------------------------------------------------------

@app.get("/v1/stats")
def storage_stats():
    with connect_db() as conn:
        objects = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes, "
            "COALESCE(SUM(jsonb_array_length(chunk_hashes)), 0) AS refs "
            "FROM object_versions"
        ).fetchone()
        chunks = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes, "
            "COALESCE(SUM(size * jsonb_array_length(nodes)), 0) AS raw "
            "FROM chunk_catalog"
        ).fetchone()
        by_method = conn.execute(
            "SELECT chunking, COUNT(*) AS objects, SUM(size) AS bytes "
            "FROM object_versions GROUP BY chunking"
        ).fetchall()

    logical = int(objects["bytes"])
    unique = int(chunks["bytes"])
    return {
        "objects": objects["n"],
        "logical_bytes": logical,
        "chunk_references": int(objects["refs"]),
        "unique_chunks": chunks["n"],
        "unique_bytes": unique,
        "raw_bytes_on_disk": int(chunks["raw"]),
        "dedup_ratio": round(logical / unique, 3) if unique else None,
        "space_saved_by_dedup_pct": round(100 * (1 - unique / logical), 2)
        if logical else 0.0,
        "by_chunking": [dict(r) for r in by_method],
    }
