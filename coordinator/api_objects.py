"""S3-style object API (internal; exposed to users through the gateway).

  GET    /v1/buckets/{bucket}                      bucket details
  DELETE /v1/buckets/{bucket}[?force=true]         delete (empty) bucket
  GET    /v1/objects/{bucket}?prefix=&after=&limit= list latest versions
  PUT    /v1/objects/{bucket}/{key}                streaming upload
  GET    /v1/objects/{bucket}/{key}[?version=]     download (Range supported)
  HEAD   /v1/objects/{bucket}/{key}                headers only
  DELETE /v1/objects/{bucket}/{key}[?version=]     delete one / all versions
  GET    /v1/meta/{bucket}/{key}                   metadata, versions, chunks
  PUT    /v1/tags/{bucket}/{key}                   replace user-defined tags
  GET    /v1/search?...                            search by prefix/size/date/tags
  POST   /v1/uploads                               start a multipart upload
  PUT    /v1/uploads/{id}/parts/{n}                upload part n (streaming)
  GET    /v1/uploads/{id}                          parts received so far
  POST   /v1/uploads/{id}/complete                 assemble the object
  DELETE /v1/uploads/{id}                          abort
"""

import hashlib
import json
from datetime import datetime, timezone
from email.utils import format_datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from coordinator import cluster, config, objects
from coordinator.db import connect_db
from coordinator.ranges import RangeNotSatisfiable, parse_range
from coordinator.redundancy import policy_key

router = APIRouter()

LATEST_SQL = """
    SELECT DISTINCT ON (bucket, object_key) *
    FROM object_versions {where}
    ORDER BY bucket, object_key, version DESC
"""


def _like_prefix(prefix: str) -> str:
    return (prefix.replace("\\", "\\\\").replace("%", "\\%")
            .replace("_", "\\_")) + "%"


def _parse_dt(value: str | None, name: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400,
                            detail=f"{name} must be an ISO-8601 date/time")


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------

@router.get("/v1/buckets/{bucket}")
def get_bucket(bucket: str):
    policy = objects.require_bucket(bucket)
    with connect_db() as conn:
        c = conn.execute(
            "SELECT COUNT(DISTINCT object_key) AS objects, COUNT(*) AS versions, "
            "COALESCE(SUM(size), 0) AS bytes FROM object_versions "
            "WHERE bucket = %s", (bucket,)).fetchone()
    return {**objects.describe_bucket(policy), "objects": c["objects"],
            "versions": c["versions"], "logical_bytes": int(c["bytes"])}


@router.delete("/v1/buckets/{bucket}")
async def delete_bucket(bucket: str, force: bool = False):
    objects.require_bucket(bucket)
    if bucket == "default":
        raise HTTPException(status_code=400, detail="The default bucket is kept")
    with connect_db() as conn:
        n = conn.execute("SELECT COUNT(*) AS n FROM object_versions "
                         "WHERE bucket = %s", (bucket,)).fetchone()["n"]
    if n and not force:
        raise HTTPException(status_code=409,
                            detail=f"Bucket has {n} object versions "
                                   "(use force=true to delete them)")
    result = await objects.delete_versions("bucket = %s", (bucket,))
    with connect_db() as conn:
        conn.execute("DELETE FROM buckets WHERE name = %s", (bucket,))
    return {"bucket": bucket, "deleted": True, **result}


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------

@router.get("/v1/objects/{bucket}")
def list_objects(bucket: str, prefix: str = "", after: str = "",
                 limit: int = 100):
    """Latest version of every key, sorted by key, paginated with `after`."""
    objects.require_bucket(bucket)
    limit = max(1, min(limit, 1000))
    where = "WHERE bucket = %s AND object_key LIKE %s ESCAPE '\\'"
    params = [bucket, _like_prefix(prefix)]
    if after:
        where += " AND object_key > %s"
        params.append(after)
    with connect_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM ({LATEST_SQL.format(where=where)}) v "
            "ORDER BY object_key LIMIT %s", (*params, limit + 1)).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    return {"bucket": bucket, "prefix": prefix, "count": len(rows),
            "is_truncated": more,
            "next_after": rows[-1]["object_key"] if more else None,
            "objects": [objects.describe_version(r) for r in rows]}


async def _request_body(request: Request):
    async for block in request.stream():
        yield block


@router.put("/v1/objects/{bucket}/{key:path}", status_code=201)
async def put_object(bucket: str, key: str, request: Request,
                     content_type: str | None = Header(default=None),
                     x_chunking: str | None = Header(default=None),
                     x_tags: str | None = Header(default=None)):
    """Streaming upload: the body is chunked while it arrives."""
    return await objects.put_object(
        bucket, key, _request_body(request),
        chunking=x_chunking or config.DEFAULT_CHUNKING,
        content_type=content_type, tags=objects.parse_tags(x_tags))


def _object_headers(obj: dict) -> dict:
    etag = obj.get("etag") or obj["sha256"] or ""
    return {
        "ETag": f'"{etag}"',
        "Last-Modified": format_datetime(obj["created_at"].astimezone(timezone.utc),
                                         usegmt=True),
        "Accept-Ranges": "bytes",
        "X-Object-Version": str(obj["version"]),
        "X-Object-Id": str(obj["object_id"]),
        "X-Object-SHA256": obj["sha256"] or "",
        "X-Object-Tags": ",".join(f"{k}={v}" for k, v in
                                  (obj.get("tags") or {}).items()),
    }


@router.get("/v1/objects/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request,
                     version: int | None = None):
    obj = objects.get_version(bucket, key, version)
    headers = _object_headers(obj)
    media_type = obj.get("content_type") or "application/octet-stream"

    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    try:
        byte_range = parse_range(request.headers.get("range"), obj["size"])
    except RangeNotSatisfiable:
        return Response(status_code=416,
                        headers={"Content-Range": f"bytes */{obj['size']}"})

    chunks = objects.chunks_of(obj)
    plan = objects.plan_read(chunks, byte_range)
    objects.check_available([c for c, _, _ in plan])

    if byte_range is None:
        headers["Content-Length"] = str(obj["size"])
        return StreamingResponse(
            objects.stream_plan(plan, obj["sha256"], obj["size"]),
            media_type=media_type, headers=headers)

    start, end = byte_range
    headers["Content-Length"] = str(end - start + 1)
    headers["Content-Range"] = f"bytes {start}-{end}/{obj['size']}"
    return StreamingResponse(objects.stream_plan(plan), status_code=206,
                             media_type=media_type, headers=headers)


@router.head("/v1/objects/{bucket}/{key:path}")
def head_object(bucket: str, key: str, version: int | None = None):
    obj = objects.get_version(bucket, key, version)
    headers = _object_headers(obj)
    headers["Content-Length"] = str(obj["size"])
    headers["Content-Type"] = obj.get("content_type") or "application/octet-stream"
    return Response(status_code=200, headers=headers)


@router.delete("/v1/objects/{bucket}/{key:path}")
async def delete_object(bucket: str, key: str, version: int | None = None):
    objects.require_bucket(bucket)
    if version is None:
        result = await objects.delete_versions(
            "bucket = %s AND object_key = %s", (bucket, key))
    else:
        result = await objects.delete_versions(
            "bucket = %s AND object_key = %s AND version = %s",
            (bucket, key, version))
    if not result["versions_deleted"]:
        raise HTTPException(status_code=404,
                            detail=f"Object '{bucket}/{key}' not found")
    return result


@router.get("/v1/meta/{bucket}/{key:path}")
def object_metadata(bucket: str, key: str, version: int | None = None):
    obj = objects.get_version(bucket, key, version)
    with connect_db() as conn:
        versions = conn.execute(
            "SELECT version, size, sha256, etag, created_at, object_id "
            "FROM object_versions WHERE bucket = %s AND object_key = %s "
            "ORDER BY version DESC", (bucket, key)).fetchall()
    chunks = objects.chunks_of(obj)
    failed = cluster.failed_nodes()
    degraded = 0
    for c in chunks:
        if c["scheme"] == "ec":
            live = {s["index"] for s in c["shards"] if s["node"] not in failed}
            degraded += len(live) < c["ec_k"] + c["ec_m"]
        else:
            degraded += sum(n not in failed for n in c["nodes"]) \
                < config.REPLICATION_FACTOR
    return {
        **objects.describe_version(obj),
        "redundancy": objects.describe_bucket(objects.require_bucket(bucket)),
        "chunk_count": len(chunks),
        "degraded_chunks": degraded,
        "versions": [{"version": v["version"], "size": v["size"],
                      "etag": v["etag"] or v["sha256"],
                      "file_id": str(v["object_id"]),
                      "created_at": v["created_at"].isoformat()}
                     for v in versions],
        "chunks": chunks,
    }


class TagsIn(BaseModel):
    tags: dict


@router.put("/v1/tags/{bucket}/{key:path}")
def put_tags(bucket: str, key: str, body: TagsIn, version: int | None = None):
    obj = objects.get_version(bucket, key, version)
    tags = objects.parse_tags(body.tags)
    with connect_db() as conn:
        conn.execute("UPDATE object_versions SET tags = %s::jsonb "
                     "WHERE object_id = %s", (json.dumps(tags), obj["object_id"]))
    return {"bucket": bucket, "key": key, "version": obj["version"],
            "tags": tags}


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@router.get("/v1/search")
def search(bucket: str | None = None, prefix: str | None = None,
           min_size: int | None = None, max_size: int | None = None,
           created_after: str | None = None, created_before: str | None = None,
           tags: str | None = None, tag_keys: str | None = None,
           content_type: str | None = None, all_versions: bool = False,
           sort: str = "key", order: str = "asc", limit: int = 100,
           offset: int = 0):
    """Search objects by key prefix, size, creation date and tags.

    tags=env=prod,team=ds   objects having ALL these key=value tags
    tag_keys=owner,project  objects having ALL these tag keys
    """
    inner, params = [], []
    if bucket:
        inner.append("bucket = %s")
        params.append(bucket)
    if prefix:
        inner.append("object_key LIKE %s ESCAPE '\\'")
        params.append(_like_prefix(prefix))
    inner_where = ("WHERE " + " AND ".join(inner)) if inner else ""

    outer, oparams = [], []
    if min_size is not None:
        outer.append("size >= %s")
        oparams.append(min_size)
    if max_size is not None:
        outer.append("size <= %s")
        oparams.append(max_size)
    after = _parse_dt(created_after, "created_after")
    before = _parse_dt(created_before, "created_before")
    if after:
        outer.append("created_at >= %s")
        oparams.append(after.isoformat())
    if before:
        outer.append("created_at <= %s")
        oparams.append(before.isoformat())
    wanted = objects.parse_tags(tags)
    if wanted:
        outer.append("tags @> %s::jsonb")
        oparams.append(json.dumps(wanted))
    for k in [t.strip() for t in (tag_keys or "").split(",") if t.strip()]:
        outer.append("tags ? %s")
        oparams.append(k)
    if content_type:
        outer.append("content_type = %s")
        oparams.append(content_type)
    outer_where = ("WHERE " + " AND ".join(outer)) if outer else ""

    sort_col = {"key": "bucket, object_key, version", "size": "size",
                "created_at": "created_at"}.get(sort)
    if sort_col is None or order not in ("asc", "desc"):
        raise HTTPException(status_code=400,
                            detail="sort: key|size|created_at, order: asc|desc")
    limit = max(1, min(limit, 1000))
    source = (f"SELECT * FROM object_versions {inner_where}" if all_versions
              else LATEST_SQL.format(where=inner_where))

    with connect_db() as conn:
        rows = conn.execute(
            f"SELECT v.*, COUNT(*) OVER () AS total FROM ({source}) v "
            f"{outer_where} ORDER BY {sort_col} {order} LIMIT %s OFFSET %s",
            (*params, *oparams, limit, max(0, offset))).fetchall()
    return {"total": rows[0]["total"] if rows else 0,
            "count": len(rows), "offset": offset,
            "results": [objects.describe_version(r) for r in rows]}


# ---------------------------------------------------------------------------
# Multipart uploads (large files in independent, retryable parts)
# ---------------------------------------------------------------------------

class UploadIn(BaseModel):
    bucket: str
    key: str
    chunking: str = config.DEFAULT_CHUNKING
    content_type: str | None = None
    tags: dict | str | None = None


def _get_upload(upload_id: str) -> dict:
    try:
        uid = UUID(upload_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid upload id")
    with connect_db() as conn:
        row = conn.execute("SELECT * FROM multipart_uploads WHERE upload_id = %s",
                           (uid,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    return row


def _describe_upload(row: dict) -> dict:
    parts = row["parts"] or {}
    return {"upload_id": str(row["upload_id"]), "bucket": row["bucket"],
            "key": row["object_key"], "chunking": row["chunking"],
            "created_at": row["created_at"].isoformat(),
            "parts": sorted(({"part_number": int(n), "size": p["size"],
                              "sha256": p["sha256"],
                              "chunks": len(p["hashes"])}
                             for n, p in parts.items()),
                            key=lambda p: p["part_number"]),
            "bytes_received": sum(p["size"] for p in parts.values())}


@router.post("/v1/uploads", status_code=201)
def start_upload(body: UploadIn):
    policy = objects.require_bucket(body.bucket)
    if not body.key or len(body.key) > 1024:
        raise HTTPException(status_code=400, detail="Invalid object key")
    if body.chunking not in ("fixed", "cdc"):
        raise HTTPException(status_code=400, detail="chunking: fixed|cdc")
    tags = objects.parse_tags(body.tags)
    with connect_db() as conn:
        row = conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket, object_key, "
            "policy, chunking, content_type, tags) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) RETURNING *",
            (uuid4(), body.bucket, body.key, policy_key(policy),
             body.chunking, body.content_type, json.dumps(tags))).fetchone()
    return _describe_upload(row)


@router.get("/v1/uploads")
def list_uploads(bucket: str | None = None):
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT * FROM multipart_uploads WHERE %s::text IS NULL "
            "OR bucket = %s ORDER BY created_at", (bucket, bucket)).fetchall()
    return {"uploads": [_describe_upload(r) for r in rows]}


@router.get("/v1/uploads/{upload_id}")
def get_upload(upload_id: str):
    return _describe_upload(_get_upload(upload_id))


@router.put("/v1/uploads/{upload_id}/parts/{part_number}")
async def upload_part(upload_id: str, part_number: int, request: Request):
    """Each part is chunked and stored immediately; its chunk references
    are held by the upload until it is completed or aborted. Re-sending a
    part replaces it (parts can be retried independently)."""
    if not 1 <= part_number <= 10000:
        raise HTTPException(status_code=400, detail="part_number: 1..10000")
    upload = _get_upload(upload_id)
    policy = objects.require_bucket(upload["bucket"])
    state = await objects.ingest(_request_body(request), policy,
                                 upload["chunking"], config.MAX_FILE_SIZE)
    part = {"hashes": state["hashes"], "size": state["size"],
            "sha256": state["sha256"]}
    garbage = []
    with connect_db() as conn:
        objects.commit_chunk_refs(conn, state)
        row = conn.execute(
            "SELECT parts FROM multipart_uploads WHERE upload_id = %s "
            "FOR UPDATE", (upload["upload_id"],)).fetchone()
        if row is None:
            garbage = objects.release_chunks(conn, state["hashes"],
                                             state["policy"])
            raise HTTPException(status_code=404, detail="Upload was aborted")
        old = (row["parts"] or {}).get(str(part_number))
        if old:
            garbage = objects.release_chunks(conn, old["hashes"],
                                             upload["policy"])
        conn.execute(
            "UPDATE multipart_uploads SET parts = parts || %s::jsonb "
            "WHERE upload_id = %s",
            (json.dumps({str(part_number): part}), upload["upload_id"]))
    await objects.collect_garbage(garbage)
    return {"upload_id": upload_id, "part_number": part_number,
            "size": state["size"], "sha256": state["sha256"],
            "etag": state["sha256"], "chunks": len(state["hashes"]),
            "new_chunks": state["new"],
            "deduplicated_chunks": state["dedup"]}


class CompleteIn(BaseModel):
    parts: list[int] | None = None


@router.post("/v1/uploads/{upload_id}/complete", status_code=201)
async def complete_upload(upload_id: str, body: CompleteIn | None = None):
    upload = _get_upload(upload_id)
    parts = upload["parts"] or {}
    wanted = sorted(body.parts) if body and body.parts else \
        sorted(int(n) for n in parts)
    if not wanted:
        raise HTTPException(status_code=400, detail="No parts uploaded")
    missing = [n for n in wanted if str(n) not in parts]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing parts {missing}")
    chosen = [parts[str(n)] for n in wanted]
    size = sum(p["size"] for p in chosen)
    if size > config.MAX_OBJECT_SIZE:
        raise HTTPException(status_code=413, detail="Object exceeds maximum size")

    hashes = [h for p in chosen for h in p["hashes"]]
    # S3-style multipart ETag: hash of the part hashes, plus the part count.
    etag = hashlib.sha256(b"".join(bytes.fromhex(p["sha256"])
                                   for p in chosen)).hexdigest() + f"-{len(chosen)}"
    garbage = []
    with connect_db() as conn:
        for n, p in parts.items():                 # parts not used: release
            if int(n) not in wanted:
                garbage += objects.release_chunks(conn, p["hashes"],
                                                  upload["policy"])
        row = objects.insert_version(
            conn, upload["bucket"], upload["object_key"], size=size,
            sha256=None, etag=etag, hashes=hashes, chunking=upload["chunking"],
            policy=upload["policy"], content_type=upload["content_type"],
            tags=upload["tags"] or {})
        conn.execute("DELETE FROM multipart_uploads WHERE upload_id = %s",
                     (upload["upload_id"],))
    await objects.collect_garbage(garbage)
    return {**objects.describe_version(row), "parts": len(chosen),
            "chunk_count": len(hashes)}


@router.delete("/v1/uploads/{upload_id}")
async def abort_upload(upload_id: str):
    upload = _get_upload(upload_id)
    garbage = []
    with connect_db() as conn:
        conn.execute("DELETE FROM multipart_uploads WHERE upload_id = %s",
                     (upload["upload_id"],))
        for p in (upload["parts"] or {}).values():
            garbage += objects.release_chunks(conn, p["hashes"],
                                              upload["policy"])
    removed = await objects.collect_garbage(garbage)
    return {"upload_id": upload_id, "aborted": True, "blobs_removed": removed}
