"""Write and read paths for the two redundancy schemes.

Replication (N copies, quorum W / R)
  write: send the chunk to the first N live nodes of its preference list
         (falling back to further nodes if one refuses); succeed once W
         copies are acknowledged. Fewer than N copies = "degraded", topped
         up later by the repair job.
  read:  fetch one verified copy and confirm the chunk on R replicas in
         total. Because W + R > N, the R replicas always include at least
         one replica from the write quorum.

Erasure coding (Reed-Solomon k + m)
  write: split the chunk into k data shards, compute m parity shards, store
         every shard (content-addressed) on the nodes chosen by
         cluster.ec_assignment. Succeed once at least k + 1 shards are
         stored (still tolerant to one more loss); missing shards are
         rebuilt by repair.
  read:  fetch any k valid shards and decode.
"""

import asyncio
import hashlib
import json

import httpx
from fastapi import HTTPException

from coordinator import cluster, config, erasure
from coordinator.db import connect_db


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------

async def store_replicated(client: httpx.AsyncClient, digest: str,
                           block: bytes) -> dict:
    n, w = config.REPLICATION_FACTOR, config.WRITE_QUORUM
    candidates = cluster.preference_list(digest)
    if len(candidates) < w:
        raise HTTPException(
            status_code=503,
            detail=f"Only {len(candidates)} live nodes, write quorum W={w}")

    targets, spares = candidates[:n], candidates[n:]
    results = await asyncio.gather(
        *(cluster.put_blob(client, node, digest, block) for node in targets))
    stored = [node for node, ok in zip(targets, results) if ok]

    # A target refused the write: try the next nodes on the ring.
    for node in spares:
        if len(stored) >= n:
            break
        if await cluster.put_blob(client, node, digest, block):
            stored.append(node)

    if len(stored) < w:
        raise HTTPException(
            status_code=503,
            detail=f"Write quorum not met: {len(stored)}/{w} acknowledgements")

    return {"scheme": "replication", "size": len(block), "nodes": stored,
            "shards": None, "ec_k": None, "ec_m": None, "codec": None,
            "degraded": len(stored) < n}


async def store_erasure_coded(client: httpx.AsyncClient, digest: str,
                              block: bytes, k: int, m: int) -> dict:
    codec = erasure.get_codec()
    shards = await asyncio.to_thread(codec.encode, block, k, m)
    live = cluster.preference_list(digest)
    assignment = cluster.ec_assignment(digest, k, m, candidates=live)

    shard_hashes = [hashlib.sha256(s).hexdigest() for s in shards]
    results = await asyncio.gather(
        *(cluster.put_blob(client, assignment[i], shard_hashes[i], shards[i])
          for i in range(k + m)))

    stored = [{"index": i, "hash": shard_hashes[i], "size": len(shards[i]),
               "node": assignment[i]}
              for i, ok in enumerate(results) if ok]
    if len(stored) < min(k + 1, k + m):
        raise HTTPException(
            status_code=503,
            detail=f"Only {len(stored)} of {k + m} shards stored")

    return {"scheme": "ec", "size": len(block),
            "nodes": cluster.shard_nodes(stored), "shards": stored,
            "ec_k": k, "ec_m": m, "codec": codec.name,
            "degraded": len(stored) < k + m}


async def store_chunk(client, digest, block, policy: dict) -> dict:
    if policy["redundancy"] == "ec":
        return await store_erasure_coded(client, digest, block,
                                         policy["ec_k"], policy["ec_m"])
    return await store_replicated(client, digest, block)


def policy_key(policy: dict) -> str:
    """Identifier of a redundancy policy: 'replication' or 'ec:4+2'."""
    if policy["redundancy"] == "ec":
        return f"ec:{policy['ec_k']}+{policy['ec_m']}"
    return "replication"


def insert_catalog_entry(conn, digest: str, policy: str, entry: dict) -> None:
    """Insert a new chunk (ref_count = 1) or add a reference to it."""
    conn.execute(
        """
        INSERT INTO chunk_catalog
            (chunk_hash, policy, size, ref_count, nodes, scheme, ec_k, ec_m,
             codec, shards)
        VALUES (%s, %s, %s, 1, %s::jsonb, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (chunk_hash, policy) DO UPDATE
        SET ref_count = chunk_catalog.ref_count + 1
        """,
        (digest, policy, entry["size"], json.dumps(entry["nodes"]), entry["scheme"],
         entry["ec_k"], entry["ec_m"], entry["codec"],
         json.dumps(entry["shards"]) if entry["shards"] is not None else None))


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

def forget_copy(digest: str, node: str) -> None:
    """A replica turned out to be missing/corrupt: drop it from the catalog
    and wake the repair job (this is "read repair")."""
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE chunk_catalog
            SET nodes = COALESCE((SELECT jsonb_agg(n) FROM jsonb_array_elements(nodes) n
                                  WHERE n <> to_jsonb(%s::text)), '[]'::jsonb)
            WHERE chunk_hash = %s AND scheme = 'replication'
            """,
            (node, digest))
    cluster.request_repair()


def forget_shard(shard_hash: str, node: str) -> None:
    """Drop every catalog reference to shard `shard_hash` on `node`."""
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT chunk_hash, policy, shards FROM chunk_catalog "
            "WHERE scheme = 'ec' AND shards @> %s::jsonb",
            (json.dumps([{"hash": shard_hash, "node": node}]),)).fetchall()
        for row in rows:
            kept = [s for s in row["shards"]
                    if not (s["hash"] == shard_hash and s["node"] == node)]
            conn.execute(
                "UPDATE chunk_catalog SET shards = %s::jsonb, nodes = %s::jsonb "
                "WHERE chunk_hash = %s AND policy = %s",
                (json.dumps(kept), json.dumps(cluster.shard_nodes(kept)),
                 row["chunk_hash"], row["policy"]))
    cluster.request_repair()


async def read_replicated(client, chunk: dict) -> bytes:
    digest, size = chunk["sha256"], chunk["size"]
    failed = cluster.failed_nodes()
    live = [n for n in chunk["nodes"] if n not in failed]

    data, confirmations = None, 0
    for node in live:
        if data is None:
            blob, status = await cluster.get_blob(client, node, digest, size)
            if status == "ok":
                data, confirmations = blob, 1
            elif status in ("missing", "corrupt"):
                forget_copy(digest, node)
        elif confirmations < config.READ_QUORUM:
            status = await cluster.head_blob(client, node, digest, size)
            if status == "ok":
                confirmations += 1
            elif status in ("missing", "corrupt"):
                forget_copy(digest, node)
        else:
            break

    if data is None:
        raise RuntimeError(f"No valid replica available for chunk {digest}")
    if confirmations < config.READ_QUORUM:
        raise RuntimeError(
            f"Read quorum not met for chunk {digest}: "
            f"{confirmations}/{config.READ_QUORUM} replicas")
    return data


async def read_erasure_coded(client, chunk: dict) -> bytes:
    k, m = chunk["ec_k"], chunk["ec_m"]
    failed = cluster.failed_nodes()
    shards = sorted((s for s in chunk["shards"] if s["node"] not in failed),
                    key=lambda s: s["index"])   # data shards first: fast path

    available: dict[int, bytes] = {}

    async def fetch(s):
        blob, status = await cluster.get_blob(client, s["node"], s["hash"],
                                              s["size"])
        if status in ("missing", "corrupt"):
            forget_shard(s["hash"], s["node"])
        return s["index"], blob

    # First try exactly k shards in parallel, then fall back to the others.
    first, rest = shards[:k], shards[k:]
    for index, blob in await asyncio.gather(*(fetch(s) for s in first)):
        if blob is not None:
            available[index] = blob
    for s in rest:
        if len(available) >= k:
            break
        index, blob = await fetch(s)
        if blob is not None:
            available[index] = blob

    if len(available) < k:
        raise RuntimeError(
            f"Chunk {chunk['sha256']} unrecoverable: "
            f"{len(available)} of {k} required shards")

    data = await asyncio.to_thread(erasure.decode, available, k, m,
                                   chunk["size"], chunk["codec"])
    if hashlib.sha256(data).hexdigest() != chunk["sha256"]:
        raise RuntimeError(f"Decoded chunk {chunk['sha256']} has wrong hash")
    return data


async def read_chunk(client, chunk: dict) -> bytes:
    if chunk.get("scheme") == "ec":
        return await read_erasure_coded(client, chunk)
    return await read_replicated(client, chunk)


# ---------------------------------------------------------------------------
# Garbage collection
# ---------------------------------------------------------------------------

def still_referenced(conn, blob_hash: str, node: str) -> bool:
    """Is (blob_hash on node) used by any catalog entry?

    Needed because shards are content-addressed too: two chunks (or two
    shards of one chunk, e.g. all-zero data) can share the same blob.
    """
    row = conn.execute(
        """
        SELECT 1 FROM chunk_catalog
        WHERE (chunk_hash = %s AND scheme = 'replication' AND nodes ? %s)
           OR (scheme = 'ec' AND shards @> %s::jsonb)
        LIMIT 1
        """,
        (blob_hash, node, json.dumps([{"hash": blob_hash, "node": node}]))
    ).fetchone()
    return row is not None


def blobs_of(row: dict) -> list[tuple[str, str]]:
    """(blob hash, node) pairs physically stored for a catalog row."""
    if row.get("scheme") == "ec":
        return [(s["hash"], s["node"]) for s in (row["shards"] or [])]
    return [(row["chunk_hash"], n) for n in row["nodes"]]


async def delete_unreferenced(client, pairs) -> int:
    removed = 0
    for blob_hash, node in set(pairs):
        with connect_db() as conn:
            if still_referenced(conn, blob_hash, node):
                continue
        if await cluster.delete_blob(client, node, blob_hash):
            removed += 1
    return removed
