"""Object-level operations shared by every API (old /v1/files, S3-style
/v1/objects, multipart uploads): buckets, ingest, versions, reads, deletes.

Write path (ingest)
  request body (stream) -> StreamChunker -> for every chunk: SHA-256 ->
  dedup check (same policy) -> store with the bucket's redundancy policy ->
  finally ONE transaction: chunk references + object version row.

Read path
  object version -> ordered chunk list -> (range) only the chunks needed ->
  read each chunk (quorum read or erasure decode) -> slice -> stream.
"""

import asyncio
import hashlib
import json
import re
from typing import AsyncIterator
from uuid import uuid4

import httpx
from fastapi import HTTPException

from coordinator import cluster, config, jobs, search_index
from coordinator.chunking import StreamChunker
from coordinator.db import connect_db
from coordinator.ranges import chunks_for_range
from coordinator.redundancy import (blobs_of, insert_catalog_entry, policy_key,
                                    read_chunk, store_chunk)

BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9.-]{1,62}")
TAG_RE = re.compile(r"[A-Za-z0-9_.:/=+@ -]{1,128}")
MAX_TAGS = 10


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------

def bucket_policy(name: str) -> dict | None:
    with connect_db() as conn:
        return conn.execute("SELECT * FROM buckets WHERE name = %s",
                            (name,)).fetchone()


def require_bucket(name: str) -> dict:
    policy = bucket_policy(name)
    if policy is None:
        raise HTTPException(status_code=404, detail=f"Bucket '{name}' not found")
    return policy


def ensure_bucket(name: str) -> dict:
    """Existing policy, or create the bucket with the default policy."""
    policy = bucket_policy(name)
    if policy is None:
        if not BUCKET_RE.fullmatch(name):
            raise HTTPException(status_code=400, detail="Invalid bucket name")
        ec = config.DEFAULT_REDUNDANCY == "ec"
        with connect_db() as conn:
            conn.execute(
                "INSERT INTO buckets (name, redundancy, ec_k, ec_m) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING",
                (name, config.DEFAULT_REDUNDANCY,
                 config.EC_K if ec else None, config.EC_M if ec else None))
        policy = bucket_policy(name)
    return policy


def describe_bucket(row: dict) -> dict:
    out = {"name": row["name"], "redundancy": row["redundancy"],
           "created_at": row["created_at"].isoformat()}
    if row["redundancy"] == "ec":
        k, m = row["ec_k"], row["ec_m"]
        out.update({"k": k, "m": m, "storage_overhead": round((k + m) / k, 3),
                    "tolerates_lost_shards": m})
    else:
        n = config.REPLICATION_FACTOR
        out.update({"replicas": n, "storage_overhead": float(n),
                    "tolerates_lost_copies": n - 1,
                    "write_quorum": config.WRITE_QUORUM,
                    "read_quorum": config.READ_QUORUM})
    return out


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def parse_tags(raw) -> dict:
    """Accepts a dict, a JSON object string, or "k1=v1,k2=v2"."""
    if raw is None or raw == "":
        return {}
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith("{"):
            try:
                raw = json.loads(raw)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid tags JSON")
        else:
            pairs = {}
            for item in raw.split(","):
                if not item.strip():
                    continue
                if "=" not in item:
                    raise HTTPException(status_code=400,
                                        detail="Tags must be key=value pairs")
                k, v = item.split("=", 1)
                pairs[k.strip()] = v.strip()
            raw = pairs
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="Tags must be an object")
    if len(raw) > MAX_TAGS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_TAGS} tags")
    tags = {}
    for k, v in raw.items():
        k, v = str(k), str(v)
        if not TAG_RE.fullmatch(k) or not (v == "" or TAG_RE.fullmatch(v)):
            raise HTTPException(status_code=400, detail=f"Invalid tag {k!r}")
        tags[k] = v
    return tags


# ---------------------------------------------------------------------------
# Ingest (write path)
# ---------------------------------------------------------------------------

async def ingest(source: AsyncIterator[bytes], policy: dict, chunking: str,
                 max_size: int) -> dict:
    """Chunk a byte stream and store every new chunk with the bucket policy.

    Nothing is committed to the catalog here; the caller does that in one
    transaction with commit_chunk_refs(). If the upload fails half-way, the
    stored chunks are unreferenced orphans that /v1/admin/gc removes.
    """
    if chunking not in ("fixed", "cdc"):
        raise HTTPException(status_code=400,
                            detail="chunking must be 'fixed' or 'cdc'")
    pkey = policy_key(policy)
    chunker = StreamChunker(chunking, config.CHUNK_SIZE)
    file_hash = hashlib.sha256()
    state = {"size": 0, "hashes": [], "entries": {}, "new": 0, "dedup": 0,
             "degraded": 0}

    async def handle(client, block: bytes):
        digest = hashlib.sha256(block).hexdigest()
        state["hashes"].append(digest)
        if digest in state["entries"]:
            state["dedup"] += 1
            return
        with connect_db() as conn:
            existing = conn.execute(
                "SELECT size FROM chunk_catalog WHERE chunk_hash = %s "
                "AND policy = %s AND ref_count > 0", (digest, pkey)).fetchone()
        if existing:
            if existing["size"] != len(block):
                raise HTTPException(status_code=500,
                                    detail="Chunk catalog integrity error")
            state["dedup"] += 1
            return
        entry = await store_chunk(client, digest, block, policy)
        state["entries"][digest] = entry
        state["new"] += 1
        state["degraded"] += entry["degraded"]

    async with httpx.AsyncClient(timeout=120) as client:
        async for data in source:
            if not data:
                continue
            state["size"] += len(data)
            if state["size"] > max_size:
                raise HTTPException(status_code=413,
                                    detail="Object exceeds maximum size")
            file_hash.update(data)
            # Chunking (CDC especially) is CPU-bound: worker thread.
            for block in await asyncio.to_thread(chunker.feed, data):
                await handle(client, block)
        for block in await asyncio.to_thread(chunker.finish):
            await handle(client, block)

    state["sha256"] = file_hash.hexdigest()
    state["policy"] = pkey
    return state


def commit_chunk_refs(conn, state: dict) -> None:
    """Add one reference per chunk occurrence (new chunks are inserted)."""
    entries = dict(state["entries"])
    for digest in state["hashes"]:
        if digest in entries:
            insert_catalog_entry(conn, digest, state["policy"],
                                 entries.pop(digest))
        else:
            conn.execute(
                "UPDATE chunk_catalog SET ref_count = ref_count + 1 "
                "WHERE chunk_hash = %s AND policy = %s",
                (digest, state["policy"]))


def insert_version(conn, bucket: str, key: str, *, size: int, sha256,
                   etag: str, hashes: list[str], chunking: str, policy: str,
                   content_type, tags: dict) -> dict:
    """New version of bucket/key (version numbers serialised by a lock)."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                 (bucket, key))
    return conn.execute(
        """
        INSERT INTO object_versions
            (object_id, bucket, object_key, version, size, sha256, etag,
             chunk_hashes, chunking, policy, content_type, tags)
        VALUES (%s, %s, %s,
                COALESCE((SELECT MAX(version) FROM object_versions
                          WHERE bucket = %s AND object_key = %s), 0) + 1,
                %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb)
        RETURNING *
        """,
        (uuid4(), bucket, key, bucket, key, size, sha256, etag,
         json.dumps(hashes), chunking, policy, content_type,
         json.dumps(tags))).fetchone()


async def put_object(bucket: str, key: str, source: AsyncIterator[bytes], *,
                     chunking: str, content_type=None, tags=None,
                     create_bucket: bool = False) -> dict:
    if not key or len(key) > 1024:
        raise HTTPException(status_code=400, detail="Invalid object key")
    policy = ensure_bucket(bucket) if create_bucket else require_bucket(bucket)
    state = await ingest(source, policy, chunking, config.MAX_FILE_SIZE)
    with connect_db() as conn:
        commit_chunk_refs(conn, state)
        row = insert_version(
            conn, bucket, key, size=state["size"], sha256=state["sha256"],
            etag=state["sha256"], hashes=state["hashes"], chunking=chunking,
            policy=state["policy"], content_type=content_type,
            tags=tags or {})
    search_index.mark_dirty(bucket, key)
    return {**describe_version(row),
            "redundancy": describe_bucket(policy),
            "chunk_count": len(state["hashes"]),
            "new_chunks": state["new"],
            "deduplicated_chunks": state["dedup"],
            "degraded_chunks": state["degraded"]}


def describe_version(row: dict) -> dict:
    return {
        "file_id": str(row["object_id"]),
        "bucket": row["bucket"],
        "key": row["object_key"],
        "version": row["version"],
        "size": row["size"],
        "sha256": row["sha256"],
        "etag": row.get("etag") or row["sha256"],
        "content_type": row.get("content_type"),
        "tags": row.get("tags") or {},
        "chunking": row["chunking"],
        "created_at": row["created_at"].isoformat(),
    }


# ---------------------------------------------------------------------------
# Lookup and read path
# ---------------------------------------------------------------------------

def get_version(bucket: str, key: str, version: int | None = None) -> dict:
    with connect_db() as conn:
        if version is None:
            row = conn.execute(
                "SELECT * FROM object_versions WHERE bucket = %s "
                "AND object_key = %s ORDER BY version DESC LIMIT 1",
                (bucket, key)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM object_versions WHERE bucket = %s "
                "AND object_key = %s AND version = %s",
                (bucket, key, version)).fetchone()
    if row is None:
        raise HTTPException(status_code=404,
                            detail=f"Object '{bucket}/{key}' not found")
    return row


def chunks_of(obj: dict) -> list[dict]:
    """Ordered chunk list of a version, with each chunk's placement."""
    hashes = obj["chunk_hashes"]
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT chunk_hash, size, nodes, scheme, ec_k, ec_m, codec, shards "
            "FROM chunk_catalog WHERE chunk_hash = ANY(%s) AND policy = %s",
            (list(set(hashes)), obj["policy"])).fetchall()
    catalog = {r["chunk_hash"]: r for r in rows}
    missing = [h for h in hashes if h not in catalog]
    if missing:
        raise HTTPException(status_code=500,
                            detail=f"Chunk catalog missing {missing[0]}")
    chunks = []
    for h in hashes:
        c = catalog[h]
        entry = {"sha256": h, "size": c["size"], "scheme": c["scheme"],
                 "nodes": c["nodes"]}
        if c["scheme"] == "ec":
            entry.update({"ec_k": c["ec_k"], "ec_m": c["ec_m"],
                          "codec": c["codec"], "shards": c["shards"]})
        chunks.append(entry)
    return chunks


def check_available(chunks: list[dict]) -> None:
    """Fail fast with 503 (before streaming starts) if a chunk is unreadable."""
    failed = cluster.failed_nodes(force=True)
    for chunk in chunks:
        if chunk["scheme"] == "ec":
            live = {s["index"] for s in chunk["shards"] if s["node"] not in failed}
            if len(live) < chunk["ec_k"]:
                raise HTTPException(
                    status_code=503,
                    detail=f"Chunk {chunk['sha256'][:12]} unavailable: "
                           f"{len(live)} of {chunk['ec_k']} shards on live nodes")
        else:
            live = [n for n in chunk["nodes"] if n not in failed]
            if len(live) < config.READ_QUORUM:
                raise HTTPException(
                    status_code=503,
                    detail=f"Read quorum not met for chunk {chunk['sha256'][:12]}: "
                           f"{len(live)} live replicas, R={config.READ_QUORUM}")


def plan_read(chunks: list[dict], byte_range):
    """List of (chunk, slice_start, slice_end) for a range (None = all)."""
    if byte_range is None:
        return [(c, 0, c["size"]) for c in chunks]
    start, end = byte_range
    return [(chunks[i], a, b) for i, a, b in
            chunks_for_range([c["size"] for c in chunks], start, end)]


async def stream_plan(plan, expected_sha256=None, expected_size=None):
    """Read the planned chunks in order and yield the requested bytes.

    When the whole object is read, its SHA-256 and size are verified at the
    end (end-to-end integrity check)."""
    hasher = hashlib.sha256() if expected_sha256 else None
    total = 0
    async with httpx.AsyncClient(timeout=120) as client:
        for chunk, a, b in plan:
            data = await read_chunk(client, chunk)
            piece = data if (a == 0 and b == len(data)) else data[a:b]
            if hasher:
                hasher.update(piece)
            total += len(piece)
            yield piece
    if expected_size is not None and total != expected_size:
        raise RuntimeError("Reconstructed object size mismatch")
    if hasher and hasher.hexdigest() != expected_sha256:
        raise RuntimeError("Reconstructed object SHA-256 mismatch")


# ---------------------------------------------------------------------------
# Deletes and reference release
# ---------------------------------------------------------------------------

def release_chunks(conn, hashes: list[str], policy: str) -> list[dict]:
    """Drop one reference per occurrence; return catalog rows now unused."""
    for digest in hashes:
        conn.execute(
            "UPDATE chunk_catalog SET ref_count = ref_count - 1 "
            "WHERE chunk_hash = %s AND policy = %s AND ref_count > 0",
            (digest, policy))
    if not hashes:
        return []
    return conn.execute(
        "DELETE FROM chunk_catalog WHERE ref_count = 0 "
        "AND chunk_hash = ANY(%s) AND policy = %s RETURNING *",
        (list(set(hashes)), policy)).fetchall()


async def collect_garbage(garbage_rows: list[dict]) -> int:
    """Queue the blobs of unreferenced chunks for deletion (a "gc" job run
    by the workers). Returns the number of blobs scheduled."""
    pairs = sorted({p for row in garbage_rows for p in blobs_of(row)})
    if not pairs:
        return 0
    await jobs.create_job("gc", "unreferenced chunks", [list(p) for p in pairs],
                          batch_size=200)
    return len(pairs)


async def delete_versions(where_sql: str, params: tuple) -> dict:
    """Delete the object versions matching WHERE and garbage-collect."""
    garbage = []
    with connect_db() as conn:
        rows = conn.execute(
            f"DELETE FROM object_versions WHERE {where_sql} "
            "RETURNING bucket, object_key, version, chunk_hashes, policy",
            params).fetchall()
        for row in rows:
            garbage += release_chunks(conn, row["chunk_hashes"], row["policy"])
    for row in rows:
        search_index.mark_dirty(row["bucket"], row["object_key"])
    scheduled = await collect_garbage(garbage)
    return {"versions_deleted": len(rows),
            "deleted": [{"bucket": r["bucket"], "key": r["object_key"],
                         "version": r["version"]} for r in rows],
            "chunks_garbage_collected": len(garbage),
            "blobs_scheduled_for_gc": scheduled}
