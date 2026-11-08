import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
import psycopg
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from psycopg.rows import dict_row

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


NODES = parse_nodes(os.getenv("NODE_URLS", ""))

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(1024 * 1024 * 1024)))
# A node is considered failed after this many seconds without a heartbeat
# (default = 3 missed heartbeats at a 5 s interval).
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "15"))

if CHUNK_SIZE <= 0 or MAX_FILE_SIZE < 0:
    raise RuntimeError("Invalid size configuration")

if REPLICATION_FACTOR < 1 or REPLICATION_FACTOR > len(NODES):
    raise RuntimeError("Invalid replication factor or insufficient nodes")


def node_url(node: str) -> str:
    """Resolve a node id to its URL (also accepts a URL, for old rows)."""
    if node.startswith("http://") or node.startswith("https://"):
        return node.rstrip("/")
    try:
        return NODES[node]
    except KeyError:
        raise HTTPException(status_code=500, detail=f"Unknown node id {node}")


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

        # The old file_id-based table is replaced by object_versions.
        conn.execute("DROP TABLE IF EXISTS manifests")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Distributed Content-Addressed Storage",
    version="1.1.0",
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

    node_id = payload["node_id"]
    if node_id not in NODES:
        raise HTTPException(status_code=400, detail="Unknown node")

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
                chunk_count, stored_bytes, last_seen
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET
                free_bytes = EXCLUDED.free_bytes,
                total_bytes = EXCLUDED.total_bytes,
                chunk_count = EXCLUDED.chunk_count,
                stored_bytes = EXCLUDED.stored_bytes,
                last_seen = EXCLUDED.last_seen
            """,
            (node_id, free_bytes, total_bytes, chunk_count, stored_bytes, now),
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
                   chunk_count, stored_bytes, last_seen
            FROM node_heartbeats
            """
        ).fetchall()
    seen = {row["node_id"]: row for row in rows}

    results = []
    for node_id, url in sorted(NODES.items()):
        row = seen.get(node_id)
        if row is None:
            results.append({"node_id": node_id, "url": url,
                            "status": "unknown"})
            continue

        age = max(0.0, (now - row["last_seen"]).total_seconds())
        results.append({
            "node_id": node_id,
            "url": url,
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
# Placement (temporary: replaced by the consistent hashing ring in Step A)
# ---------------------------------------------------------------------------

def choose_nodes(digest: str) -> list[str]:
    node_ids = sorted(NODES)
    return node_ids[:REPLICATION_FACTOR]


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------

@app.post("/v1/files", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    bucket: str = Form("default"),
    key: str | None = Form(None),
):
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
        async with httpx.AsyncClient(timeout=120) as client:
            while True:
                block = await file.read(CHUNK_SIZE)
                if not block:
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
                    %s, %s, %s::jsonb, 'fixed'
                )
                RETURNING version, created_at
                """,
                (
                    object_id, bucket, object_key, bucket, object_key,
                    file_size, file_hash.hexdigest(),
                    json.dumps([c["sha256"] for c in chunks]),
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
