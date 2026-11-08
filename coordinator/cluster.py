"""Cluster view: ring membership, node liveness, node I/O and placement.

Two separate notions are kept apart on purpose:
  * membership (ring_members): where data SHOULD live. Changed only by an
    operator (add / remove node), followed by a rebalance.
  * liveness (heartbeats): which nodes are reachable NOW. A node that misses
    heartbeats is "failed": it stays in the ring (a short outage must not
    reshuffle 1/N of the data) but the repair job re-creates its data
    elsewhere and new writes skip it.
"""

import asyncio
import hashlib
import math
import threading
import time

import httpx
from fastapi import HTTPException

from coordinator import config
from coordinator.db import connect_db
from coordinator.ring import ConsistentHashRing

NODES: dict[str, str] = dict(config.SEED_NODES)       # ring members: id -> url
KNOWN_URLS: dict[str, str] = dict(config.SEED_NODES)  # every node ever seen
RING = ConsistentHashRing(NODES, vnodes=config.VNODES)
_LOCK = threading.Lock()

# Set whenever something needs repairing (failed node, missing or corrupted
# copy found by a read or by the scrubber); wakes the repair loop early.
REPAIR_WAKEUP = asyncio.Event()


def request_repair() -> None:
    REPAIR_WAKEUP.set()


def load_membership() -> None:
    """Rebuild NODES / KNOWN_URLS / RING from the database."""
    global NODES, RING
    with connect_db() as conn:
        members = conn.execute(
            "SELECT node_id, url FROM ring_members ORDER BY node_id").fetchall()
        seen = conn.execute(
            "SELECT node_id, url FROM node_heartbeats WHERE url IS NOT NULL"
        ).fetchall()
    with _LOCK:
        for row in seen:
            KNOWN_URLS[row["node_id"]] = row["url"]
        NODES = {row["node_id"]: row["url"] for row in members}
        KNOWN_URLS.update(NODES)
        RING = ConsistentHashRing(NODES, vnodes=config.VNODES)


def node_url(node: str) -> str:
    url = NODES.get(node) or KNOWN_URLS.get(node)
    if url is None:
        raise HTTPException(status_code=500, detail=f"Unknown node id {node}")
    return url


def node_headers() -> dict:
    return {"X-Cluster-Token": config.CLUSTER_TOKEN}


# ---------------------------------------------------------------------------
# Liveness (heartbeat-based failure detector)
# ---------------------------------------------------------------------------

_LIVENESS = {"at": 0.0, "failed": frozenset()}


def failed_nodes(force: bool = False) -> frozenset:
    """Nodes whose last heartbeat is older than HEARTBEAT_TIMEOUT.

    Ages are computed with the database clock (NOW() - last_seen), so the
    coordinator's own clock does not matter. Cached for one second.
    """
    now = time.monotonic()
    if not force and now - _LIVENESS["at"] < 1.0:
        return _LIVENESS["failed"]
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT node_id, EXTRACT(EPOCH FROM (NOW() - last_seen)) AS age "
            "FROM node_heartbeats").fetchall()
    failed = frozenset(r["node_id"] for r in rows
                       if float(r["age"]) > config.HEARTBEAT_TIMEOUT)
    _LIVENESS.update(at=now, failed=failed)
    return failed


# ---------------------------------------------------------------------------
# Node I/O helpers
# ---------------------------------------------------------------------------

async def put_blob(client: httpx.AsyncClient, node: str, digest: str,
                   data: bytes) -> bool:
    try:
        r = await client.put(
            f"{node_url(node)}/v1/chunks/{digest}", content=data,
            headers={**node_headers(),
                     "Content-Type": "application/octet-stream"})
        return r.status_code in (200, 201)
    except httpx.HTTPError:
        return False


async def get_blob(client: httpx.AsyncClient, node: str, digest: str,
                   size: int | None = None) -> tuple[bytes | None, str]:
    """Fetch and verify a blob. Returns (data or None, status).

    status: "ok" | "missing" (404) | "corrupt" (409 or bad hash) | "down"
    """
    try:
        r = await client.get(f"{node_url(node)}/v1/chunks/{digest}",
                             headers=node_headers())
    except httpx.HTTPError:
        return None, "down"
    if r.status_code == 404:
        return None, "missing"
    if r.status_code == 409:
        return None, "corrupt"
    if r.status_code != 200:
        return None, "down"
    data = r.content
    if hashlib.sha256(data).hexdigest() != digest or (
            size is not None and len(data) != size):
        return None, "corrupt"
    return data, "ok"


async def head_blob(client: httpx.AsyncClient, node: str, digest: str,
                    size: int | None = None) -> str:
    try:
        r = await client.head(f"{node_url(node)}/v1/chunks/{digest}",
                              headers=node_headers())
    except httpx.HTTPError:
        return "down"
    if r.status_code == 404:
        return "missing"
    if r.status_code != 200:
        return "down"
    if size is not None and int(r.headers.get("content-length", -1)) != size:
        return "corrupt"
    return "ok"


async def delete_blob(client: httpx.AsyncClient, node: str,
                      digest: str) -> bool:
    try:
        r = await client.delete(f"{node_url(node)}/v1/chunks/{digest}",
                                headers=node_headers())
        return r.status_code in (200, 404)
    except httpx.HTTPError:
        return False


# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------

def preference_list(digest: str, skip_failed: bool = True) -> list[str]:
    """All ring members, in clockwise order from the chunk's position."""
    order = RING.get_nodes(digest, len(NODES))
    if skip_failed:
        failed = failed_nodes()
        order = [n for n in order if n not in failed]
    return order


def replication_targets(digest: str) -> list[str]:
    """Ideal replica set when every node is alive (used by rebalance)."""
    return RING.get_nodes(digest, config.REPLICATION_FACTOR)


def ec_assignment(digest: str, k: int, m: int,
                  candidates: list[str] | None = None) -> list[str]:
    """Node for each shard index 0..k+m-1 (round-robin on the pref. list).

    With fewer than k+m nodes some nodes hold several shards. The chunk
    survives the loss of a whole node only if no node holds more than m
    shards, so that is enforced.
    """
    cands = candidates if candidates is not None else RING.get_nodes(
        digest, len(NODES))
    if not cands:
        raise HTTPException(status_code=503, detail="No storage nodes available")
    if math.ceil((k + m) / len(cands)) > m:
        raise HTTPException(
            status_code=503,
            detail=f"EC {k}+{m} needs at least {math.ceil((k + m) / m)} "
                   f"available nodes, only {len(cands)}")
    return [cands[i % len(cands)] for i in range(k + m)]


def shard_nodes(shards: list[dict]) -> list[str]:
    seen: list[str] = []
    for s in shards:
        if s["node"] not in seen:
            seen.append(s["node"])
    return seen
