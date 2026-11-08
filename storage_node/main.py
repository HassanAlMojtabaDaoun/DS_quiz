import hashlib
import os
import re
import tempfile
from pathlib import Path
import asyncio
import shutil
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response

NODE_ID = os.getenv("NODE_ID", "node1")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "/data")).resolve()
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "dev-cluster-token-change-me")
MAX_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))

STORAGE_DIR.mkdir(parents=True, exist_ok=True)
METADATA_URL = os.getenv(
    "METADATA_URL",
    "http://storage-coordinator:8000",
).rstrip("/")

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))
# Address other services use to reach this node (sent with each heartbeat,
# so a new node can be added to the ring without editing configuration).
NODE_URL = os.getenv("NODE_URL", "").rstrip("/") or None


def collect_node_stats():
    disk = shutil.disk_usage(STORAGE_DIR)

    chunks = [
        path
        for path in STORAGE_DIR.glob("*/*")
        if path.is_file()
        and re.fullmatch(r"[0-9a-f]{64}", path.name)
    ]

    return {
        "node_id": NODE_ID,
        "url": NODE_URL,
        "free_bytes": disk.free,
        "total_bytes": disk.total,
        "chunk_count": len(chunks),
        "stored_bytes": sum(path.stat().st_size for path in chunks),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def heartbeat_loop():
    headers = {"X-Cluster-Token": CLUSTER_TOKEN}

    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                stats = collect_node_stats()

                response = await client.post(
                    f"{METADATA_URL}/v1/nodes/heartbeat",
                    json=stats,
                    headers=headers,
                )
                response.raise_for_status()

            except (httpx.HTTPError, OSError) as exc:
                print(f"[{NODE_ID}] Heartbeat failed: {exc}", flush=True)

            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
app = FastAPI(
    title=f"Storage Node {NODE_ID}",
    lifespan=lifespan,
)


def authorize(token: str | None) -> None:
    if not token or token != CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def validate_hash(digest: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
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


@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID}


@app.put("/v1/chunks/{digest}")
async def put_chunk(
    digest: str,
    request: Request,
    x_cluster_token: str | None = Header(default=None),
):
    authorize(x_cluster_token)
    validate_hash(digest)

    target = chunk_path(digest, create_dir=True)

    # Content-addressed data is immutable: a valid copy is never rewritten.
    # A corrupted copy, however, is replaced (this is how repair works).
    if target.is_file() and file_sha256(target) == digest:
        # Drain the request body so the connection can be reused.
        async for _ in request.stream():
            pass
        return {"hash": digest, "stored": True, "already_existed": True}

    temp_path = None
    hasher = hashlib.sha256()
    size = 0

    try:
        # Temporary file is on the same filesystem as the destination,
        # so the final rename is atomic.
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target.parent,
            prefix=".upload-",
            delete=False,
        ) as output:
            temp_path = Path(output.name)

            async for block in request.stream():
                if not block:
                    continue

                size += len(block)
                if size > MAX_CHUNK_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail="Chunk exceeds maximum size",
                    )

                hasher.update(block)
                output.write(block)

            output.flush()
            os.fsync(output.fileno())

        if hasher.hexdigest() != digest:
            raise HTTPException(
                status_code=422,
                detail="SHA-256 does not match chunk contents",
            )

        # Atomic publish (also replaces a corrupted copy, if any).
        os.replace(temp_path, target)
        temp_path = None

        return {
            "hash": digest,
            "size": size,
            "stored": True,
            "already_existed": False,
        }

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@app.get("/v1/chunks/{digest}")
def get_chunk(
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

    return Response(
        status_code=200,
        headers={"Content-Length": str(path.stat().st_size)},
    )


@app.get("/v1/stats")
def stats(x_cluster_token: str | None = Header(default=None)):
    authorize(x_cluster_token)

    files = [
        p for p in STORAGE_DIR.glob("*/*")
        if p.is_file() and re.fullmatch(r"[0-9a-f]{64}", p.name)
    ]

    return {
        "node_id": NODE_ID,
        "chunk_count": len(files),
        "stored_bytes": sum(p.stat().st_size for p in files),
    }
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

    return {
        "hash": digest,
        "deleted": True,
    }