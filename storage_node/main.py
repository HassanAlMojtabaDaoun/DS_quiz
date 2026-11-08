"""Storage node: stores immutable, content-addressed chunks on local disk.

Responsibilities
  * PUT / GET / HEAD / DELETE chunks addressed by their SHA-256.
  * Verify the hash on every write and every read (reject corrupted data).
  * Send a heartbeat (id, url, free space, chunk count) to the coordinator.
  * Scrub: periodically re-read every chunk, detect bit rot, quarantine the
    bad copy and report it so the coordinator can repair it.
"""

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response

from observability.metrics import REGISTRY, instrument_app

NODE_ID = os.getenv("NODE_ID", "node1")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "/data")).resolve()
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "dev-cluster-token-change-me")
MAX_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
METADATA_URL = os.getenv("METADATA_URL",
                         "http://storage-coordinator:8000").rstrip("/")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))
# Address other services use to reach this node (sent with each heartbeat,
# so a new node can be added to the ring without editing configuration).
NODE_URL = os.getenv("NODE_URL", "").rstrip("/") or None
# Seconds between two full scrub passes (0 disables the background scrubber).
SCRUB_INTERVAL = int(os.getenv("SCRUB_INTERVAL", "300"))
# Pause after each chunk while scrubbing, so scrubbing does not starve reads.
SCRUB_THROTTLE = float(os.getenv("SCRUB_THROTTLE", "0.002"))
# Demo / testing only: enables POST /v1/debug/corrupt/{digest}.
ALLOW_FAULT_INJECTION = os.getenv("ALLOW_FAULT_INJECTION", "false").lower() == "true"

HASH_RE = re.compile(r"[0-9a-f]{64}")
QUARANTINE_DIR = STORAGE_DIR / "quarantine"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

SCRUB_STATUS: dict = {"passes": 0, "last_pass": None}
LAST_STATS: dict = {}          # refreshed with every heartbeat
FAULTS = {"unhealthy": False}  # demo: make the liveness probe fail

SCRUB_PASSES = REGISTRY.counter("ds_scrub_passes_total", "Completed scrub passes",
                                ["node"])
SCRUB_CHECKED = REGISTRY.counter("ds_scrub_chunks_checked_total",
                                 "Chunks re-read by the scrubber", ["node"])
CORRUPTION = REGISTRY.counter("ds_corrupted_chunks_total",
                              "Corrupted chunks detected", ["node", "detected_by"])
for _name, _field, _help in (
        ("ds_storage_chunks", "chunk_count", "Blobs stored on this node"),
        ("ds_storage_stored_bytes", "stored_bytes", "Bytes stored on this node"),
        ("ds_storage_free_bytes", "free_bytes", "Free bytes on the data disk"),
        ("ds_storage_total_bytes", "total_bytes", "Size of the data disk")):
    REGISTRY.gauge(_name, _help, ["node"],
                   fn=(lambda f=_field: [({"node": NODE_ID}, LAST_STATS.get(f))]))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def authorize(token: str | None) -> None:
    if not token or token != CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def validate_hash(digest: str) -> None:
    if not HASH_RE.fullmatch(digest):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")


def chunk_path(digest: str, create_dir: bool = False) -> Path:
    validate_hash(digest)
    directory = STORAGE_DIR / digest[:2]
    if create_dir:
        directory.mkdir(parents=True, exist_ok=True)
    return directory / digest


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def list_chunks() -> list[Path]:
    return [p for p in STORAGE_DIR.glob("??/*")
            if p.is_file() and HASH_RE.fullmatch(p.name)]


def collect_node_stats():
    disk = shutil.disk_usage(STORAGE_DIR)
    chunks = list_chunks()
    return {
        "node_id": NODE_ID,
        "url": NODE_URL,
        "free_bytes": disk.free,
        "total_bytes": disk.total,
        "chunk_count": len(chunks),
        "stored_bytes": sum(p.stat().st_size for p in chunks),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def quarantine(path: Path) -> None:
    """Move a corrupted copy aside (kept for inspection, never served)."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    try:
        os.replace(path, QUARANTINE_DIR / f"{path.name}.{stamp}.corrupt")
    except FileNotFoundError:
        pass


async def report_corruption(digests: list[str]) -> None:
    """Tell the coordinator which chunks were corrupted so it can repair them."""
    if not digests:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{METADATA_URL}/v1/scrub/report",
                json={"node_id": NODE_ID, "corrupted": digests},
                headers={"X-Cluster-Token": CLUSTER_TOKEN},
            )
    except httpx.HTTPError as exc:
        print(f"[{NODE_ID}] Could not report corruption: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Background tasks: heartbeat and scrubber
# ---------------------------------------------------------------------------

async def heartbeat_loop():
    headers = {"X-Cluster-Token": CLUSTER_TOKEN}
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                stats = await asyncio.to_thread(collect_node_stats)
                LAST_STATS.update(stats)
                response = await client.post(
                    f"{METADATA_URL}/v1/nodes/heartbeat",
                    json=stats, headers=headers)
                response.raise_for_status()
            except (httpx.HTTPError, OSError) as exc:
                print(f"[{NODE_ID}] Heartbeat failed: {exc}", flush=True)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


def _verify(path: Path) -> bool:
    try:
        return file_sha256(path) == path.name
    except FileNotFoundError:
        return True       # deleted meanwhile: nothing to scrub


async def scrub_once() -> dict:
    """Re-read every chunk and check it still hashes to its name."""
    started = datetime.now(timezone.utc)
    checked, corrupted, scanned_bytes = 0, [], 0
    for path in await asyncio.to_thread(list_chunks):
        ok = await asyncio.to_thread(_verify, path)
        checked += 1
        if ok:
            try:
                scanned_bytes += path.stat().st_size
            except FileNotFoundError:
                pass
        else:
            print(f"[{NODE_ID}] Scrubber: bit rot detected in {path.name}",
                  flush=True)
            quarantine(path)
            corrupted.append(path.name)
            CORRUPTION.inc(node=NODE_ID, detected_by="scrubber")
        if SCRUB_THROTTLE:
            await asyncio.sleep(SCRUB_THROTTLE)

    await report_corruption(corrupted)
    SCRUB_PASSES.inc(node=NODE_ID)
    SCRUB_CHECKED.inc(checked, node=NODE_ID)
    SCRUB_STATUS.update({
        "passes": SCRUB_STATUS["passes"] + 1,
        "last_pass": {
            "started_at": started.isoformat(),
            "duration_seconds": round(
                (datetime.now(timezone.utc) - started).total_seconds(), 2),
            "chunks_checked": checked,
            "bytes_scanned": scanned_bytes,
            "corrupted": corrupted,
        },
    })
    return SCRUB_STATUS["last_pass"]


async def scrub_loop():
    while True:
        await asyncio.sleep(SCRUB_INTERVAL)
        try:
            await scrub_once()
        except Exception as exc:          # never let the scrubber die
            print(f"[{NODE_ID}] Scrub pass failed: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(heartbeat_loop())]
    if SCRUB_INTERVAL > 0:
        tasks.append(asyncio.create_task(scrub_loop()))
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


app = FastAPI(title=f"Storage Node {NODE_ID}", lifespan=lifespan)
instrument_app(app, "storage-node")


# ---------------------------------------------------------------------------
# Chunk API
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Liveness: the process is up and serving."""
    if FAULTS["unhealthy"]:
        raise HTTPException(status_code=503, detail="unhealthy (fault injection)")
    return {"status": "ok", "node_id": NODE_ID}


@app.get("/ready")
def ready():
    """Readiness: the data directory is writable (used by Kubernetes)."""
    probe = STORAGE_DIR / ".ready-probe"
    try:
        probe.write_bytes(b"ok")
        probe.unlink()
    except OSError as exc:
        raise HTTPException(status_code=503, detail=f"data dir not writable: {exc}")
    return {"status": "ready", "node_id": NODE_ID}


@app.get("/metrics")
def metrics():
    return Response(REGISTRY.render(), media_type="text/plain; version=0.0.4")


@app.put("/v1/chunks/{digest}")
async def put_chunk(
    digest: str,
    request: Request,
    x_cluster_token: str | None = Header(default=None),
):
    authorize(x_cluster_token)
    target = chunk_path(digest, create_dir=True)

    # Content-addressed data is immutable: a valid copy is never rewritten.
    # A corrupted copy, however, is replaced (this is how repair works).
    if target.is_file() and file_sha256(target) == digest:
        async for _ in request.stream():      # drain the body
            pass
        return {"hash": digest, "stored": True, "already_existed": True}

    temp_path = None
    hasher = hashlib.sha256()
    size = 0
    try:
        # Temporary file on the same filesystem, so the rename is atomic.
        with tempfile.NamedTemporaryFile(mode="wb", dir=target.parent,
                                         prefix=".upload-",
                                         delete=False) as output:
            temp_path = Path(output.name)
            async for block in request.stream():
                if not block:
                    continue
                size += len(block)
                if size > MAX_CHUNK_SIZE:
                    raise HTTPException(status_code=413,
                                        detail="Chunk exceeds maximum size")
                hasher.update(block)
                output.write(block)
            output.flush()
            os.fsync(output.fileno())

        if hasher.hexdigest() != digest:
            raise HTTPException(status_code=422,
                                detail="SHA-256 does not match chunk contents")

        os.replace(temp_path, target)          # atomic publish
        temp_path = None
        return {"hash": digest, "size": size, "stored": True,
                "already_existed": False}
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@app.get("/v1/chunks/{digest}")
async def get_chunk(
    digest: str,
    x_cluster_token: str | None = Header(default=None),
):
    authorize(x_cluster_token)
    path = chunk_path(digest)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    # Verify integrity on every read: the content must hash to its address.
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        print(f"[{NODE_ID}] Corrupted chunk detected on read: {digest}",
              flush=True)
        quarantine(path)
        CORRUPTION.inc(node=NODE_ID, detected_by="read")
        asyncio.create_task(report_corruption([digest]))
        raise HTTPException(status_code=409, detail="Chunk is corrupted")

    return Response(content=data, media_type="application/octet-stream")


@app.head("/v1/chunks/{digest}")
def head_chunk(
    digest: str,
    x_cluster_token: str | None = Header(default=None),
):
    authorize(x_cluster_token)
    path = chunk_path(digest)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    return Response(status_code=200,
                    headers={"Content-Length": str(path.stat().st_size)})


@app.delete("/v1/chunks/{digest}")
def delete_chunk(
    digest: str,
    x_cluster_token: str | None = Header(default=None),
):
    authorize(x_cluster_token)
    path = chunk_path(digest)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    path.unlink()
    return {"hash": digest, "deleted": True}


@app.get("/v1/chunks")
def list_stored_chunks(x_cluster_token: str | None = Header(default=None)):
    """Every blob on this node (used by the orphan garbage collector)."""
    authorize(x_cluster_token)
    out = []
    for p in list_chunks():
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        out.append({"hash": p.name, "size": st.st_size, "mtime": st.st_mtime})
    return {"node_id": NODE_ID, "chunks": out}


@app.get("/v1/stats")
def stats(x_cluster_token: str | None = Header(default=None)):
    authorize(x_cluster_token)
    files = list_chunks()
    return {"node_id": NODE_ID, "chunk_count": len(files),
            "stored_bytes": sum(p.stat().st_size for p in files),
            "scrub": SCRUB_STATUS}


# ---------------------------------------------------------------------------
# Scrubbing and fault injection
# ---------------------------------------------------------------------------

@app.post("/v1/scrub")
async def scrub_now(x_cluster_token: str | None = Header(default=None)):
    """Run a full scrub pass immediately and return its result."""
    authorize(x_cluster_token)
    return await scrub_once()


@app.post("/v1/debug/unhealthy")
def make_unhealthy(x_cluster_token: str | None = Header(default=None)):
    """Demo: /health starts failing, so Kubernetes' liveness probe restarts
    the container (only if fault injection is enabled)."""
    authorize(x_cluster_token)
    if not ALLOW_FAULT_INJECTION:
        raise HTTPException(status_code=403, detail="Fault injection disabled")
    FAULTS["unhealthy"] = True
    return {"node_id": NODE_ID, "unhealthy": True}


@app.post("/v1/debug/corrupt/{digest}")
def corrupt_chunk(
    digest: str,
    x_cluster_token: str | None = Header(default=None),
):
    """Simulate bit rot by flipping one byte (only if explicitly enabled)."""
    authorize(x_cluster_token)
    if not ALLOW_FAULT_INJECTION:
        raise HTTPException(status_code=403, detail="Fault injection disabled")
    path = chunk_path(digest)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    with path.open("r+b") as f:
        first = f.read(1)
        f.seek(0)
        f.write(bytes([(first[0] if first else 0) ^ 0xFF]))
    return {"hash": digest, "corrupted": True, "node_id": NODE_ID}
