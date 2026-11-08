"""Elasticsearch index over object metadata (full-text and faceted search).

PostgreSQL stays the source of truth; Elasticsearch holds a DERIVED copy of
the metadata of the latest version of every object, optimised for search:
  * the key is tokenised on / . _ - so "logs/2026/app-error.json" matches
    "error", "2026" or "app"; fuzzy matching tolerates typos;
  * tags are stored both as exact key/value pairs (filters) and as text;
  * size, date, bucket and content type are filterable and aggregatable
    (facets for the UI).

Keeping the index up to date is asynchronous: every write only records the
(bucket, key) as "dirty" and a background task re-reads the latest version
from PostgreSQL and updates the index in bulk. Writes therefore never fail
or slow down because Elasticsearch is down; the index catches up when it
is back (eventual consistency). A full rebuild is available too.
"""

import asyncio
import json
import os

import httpx

from coordinator.db import connect_db

ES_URL = os.getenv("ES_URL", "").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "objects")
SYNC_INTERVAL = float(os.getenv("ES_SYNC_INTERVAL", "2"))

INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "tokenizer": {
                "key_tokenizer": {"type": "pattern", "pattern": r"[/._\-\s]+"}
            },
            "analyzer": {
                "key_analyzer": {"type": "custom", "tokenizer": "key_tokenizer",
                                 "filter": ["lowercase"]}
            },
        },
    },
    "mappings": {
        "properties": {
            "bucket": {"type": "keyword"},
            "key": {"type": "text", "analyzer": "key_analyzer",
                    "fields": {"raw": {"type": "keyword"}}},
            "version": {"type": "integer"},
            "size": {"type": "long"},
            "created_at": {"type": "date"},
            "content_type": {"type": "keyword"},
            "etag": {"type": "keyword"},
            "file_id": {"type": "keyword"},
            "chunking": {"type": "keyword"},
            "tags": {"type": "flattened"},
            "tags_text": {"type": "text"},
        }
    },
}

STATE = {"enabled": bool(ES_URL), "ready": False, "last_error": None,
         "indexed": 0, "deleted": 0, "last_sync": None}
_dirty: set[tuple[str, str]] = set()
_wakeup = asyncio.Event()


def doc_id(bucket: str, key: str) -> str:
    return f"{bucket}/{key}"


def to_document(row: dict) -> dict:
    tags = row.get("tags") or {}
    return {
        "bucket": row["bucket"],
        "key": row["object_key"],
        "version": row["version"],
        "size": row["size"],
        "created_at": row["created_at"].isoformat(),
        "content_type": row.get("content_type"),
        "etag": row.get("etag") or row.get("sha256"),
        "file_id": str(row["object_id"]),
        "chunking": row.get("chunking"),
        "tags": tags,
        "tags_text": " ".join(f"{k} {v}" for k, v in tags.items()),
    }


# ---------------------------------------------------------------------------
# Change notification (called by the write paths, never blocks)
# ---------------------------------------------------------------------------

def mark_dirty(bucket: str, key: str) -> None:
    if STATE["enabled"]:
        _dirty.add((bucket, key))
        _wakeup.set()


def mark_bucket_dirty(bucket: str, keys) -> None:
    for key in keys:
        mark_dirty(bucket, key)


# ---------------------------------------------------------------------------
# Index management and background sync
# ---------------------------------------------------------------------------

async def ensure_index(client: httpx.AsyncClient) -> bool:
    """Create the index if needed. Returns True if it was just created."""
    r = await client.head(f"{ES_URL}/{ES_INDEX}")
    if r.status_code == 200:
        return False
    r = await client.put(f"{ES_URL}/{ES_INDEX}", json=INDEX_BODY)
    if r.status_code >= 400 and "resource_already_exists" not in r.text:
        raise RuntimeError(f"create index: {r.status_code} {r.text[:200]}")
    return True


def _latest_versions(keys: list[tuple[str, str]]) -> dict:
    out = {}
    with connect_db() as conn:
        for bucket, key in keys:
            row = conn.execute(
                "SELECT * FROM object_versions WHERE bucket = %s AND "
                "object_key = %s ORDER BY version DESC LIMIT 1",
                (bucket, key)).fetchone()
            out[(bucket, key)] = row
    return out


async def _bulk(client: httpx.AsyncClient, lines: list[str]) -> None:
    if not lines:
        return
    r = await client.post(f"{ES_URL}/_bulk", content="\n".join(lines) + "\n",
                          headers={"Content-Type": "application/x-ndjson"})
    if r.status_code >= 400:
        raise RuntimeError(f"bulk: {r.status_code} {r.text[:200]}")
    body = r.json()
    if body.get("errors"):
        first = next((i for i in body["items"]
                      if list(i.values())[0].get("error")), None)
        raise RuntimeError(f"bulk item error: {json.dumps(first)[:300]}")


async def sync_keys(client: httpx.AsyncClient,
                    keys: list[tuple[str, str]]) -> None:
    latest = await asyncio.to_thread(_latest_versions, keys)
    lines = []
    for (bucket, key), row in latest.items():
        meta = {"_index": ES_INDEX, "_id": doc_id(bucket, key)}
        if row is None:
            lines.append(json.dumps({"delete": meta}))
            STATE["deleted"] += 1
        else:
            lines.append(json.dumps({"index": meta}))
            lines.append(json.dumps(to_document(row)))
            STATE["indexed"] += 1
    await _bulk(client, lines)


async def reindex_all(client: httpx.AsyncClient | None = None,
                      recreate: bool = True) -> dict:
    """Rebuild the index from PostgreSQL (latest version of every key)."""
    own = client is None
    client = client or httpx.AsyncClient(timeout=60)
    try:
        if recreate:
            await client.delete(f"{ES_URL}/{ES_INDEX}")
        await ensure_index(client)

        def load():
            with connect_db() as conn:
                return conn.execute(
                    "SELECT DISTINCT ON (bucket, object_key) * "
                    "FROM object_versions "
                    "ORDER BY bucket, object_key, version DESC").fetchall()
        rows = await asyncio.to_thread(load)
        for i in range(0, len(rows), 500):
            lines = []
            for row in rows[i:i + 500]:
                lines.append(json.dumps({"index": {
                    "_index": ES_INDEX,
                    "_id": doc_id(row["bucket"], row["object_key"])}}))
                lines.append(json.dumps(to_document(row)))
            await _bulk(client, lines)
        await client.post(f"{ES_URL}/{ES_INDEX}/_refresh")
        STATE["indexed"] += len(rows)
        return {"reindexed": len(rows)}
    finally:
        if own:
            await client.aclose()


async def sync_loop():
    """Background task: create the index, then keep it in sync."""
    if not STATE["enabled"]:
        return
    async with httpx.AsyncClient(timeout=30) as client:
        # Wait for Elasticsearch (it can take a while to start).
        while not STATE["ready"]:
            try:
                created = await ensure_index(client)
                if created:                   # first start: load everything
                    await reindex_all(client, recreate=False)
                STATE.update(ready=True, last_error=None)
            except (httpx.HTTPError, RuntimeError) as exc:
                STATE["last_error"] = f"waiting for Elasticsearch: {exc}"
                await asyncio.sleep(5)

        while True:
            try:
                await asyncio.wait_for(_wakeup.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            _wakeup.clear()
            await asyncio.sleep(SYNC_INTERVAL)        # batch nearby writes
            if not _dirty:
                continue
            batch = list(_dirty)[:1000]
            for item in batch:
                _dirty.discard(item)
            try:
                await sync_keys(client, batch)
                STATE.update(last_error=None,
                             last_sync=asyncio.get_running_loop().time())
            except (httpx.HTTPError, RuntimeError) as exc:
                _dirty.update(batch)                  # retry later
                STATE["last_error"] = str(exc)
                await asyncio.sleep(5)
            if _dirty:
                _wakeup.set()


def status() -> dict:
    return {"enabled": STATE["enabled"], "url": ES_URL or None,
            "index": ES_INDEX, "ready": STATE["ready"],
            "pending_changes": len(_dirty), "documents_indexed": STATE["indexed"],
            "documents_deleted": STATE["deleted"],
            "last_error": STATE["last_error"]}


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def build_query(q: str | None = None, bucket: str | None = None,
                prefix: str | None = None, min_size: int | None = None,
                max_size: int | None = None, created_after: str | None = None,
                created_before: str | None = None, tags: dict | None = None,
                content_type: str | None = None, sort: str = "relevance",
                order: str = "desc", limit: int = 50, offset: int = 0) -> dict:
    must = []
    if q:
        must.append({"multi_match": {
            "query": q,
            "fields": ["key^3", "tags_text^2", "content_type", "bucket"],
            "fuzziness": "AUTO",
            "operator": "and",
        }})
    filters = []
    if bucket:
        filters.append({"term": {"bucket": bucket}})
    if prefix:
        filters.append({"prefix": {"key.raw": prefix}})
    if min_size is not None or max_size is not None:
        rng = {}
        if min_size is not None:
            rng["gte"] = min_size
        if max_size is not None:
            rng["lte"] = max_size
        filters.append({"range": {"size": rng}})
    if created_after or created_before:
        rng = {}
        if created_after:
            rng["gte"] = created_after
        if created_before:
            rng["lte"] = created_before
        filters.append({"range": {"created_at": rng}})
    for k, v in (tags or {}).items():
        filters.append({"term": {f"tags.{k}": v}})
    if content_type:
        filters.append({"term": {"content_type": content_type}})

    sort_field = {"relevance": "_score", "size": "size",
                  "created_at": "created_at", "key": "key.raw"}.get(sort, "_score")
    return {
        "query": {"bool": {"must": must or [{"match_all": {}}],
                           "filter": filters}},
        "sort": [{sort_field: {"order": order}}, {"key.raw": "asc"}],
        "from": max(0, offset),
        "size": max(1, min(limit, 500)),
        "track_total_hits": True,
        # encoder=html escapes the text, so highlights are safe to render.
        "highlight": {"fields": {"key": {}, "tags_text": {}}, "encoder": "html",
                      "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
        "aggs": {
            "buckets": {"terms": {"field": "bucket", "size": 20}},
            "content_types": {"terms": {"field": "content_type", "size": 10}},
            "sizes": {"range": {"field": "size", "ranges": [
                {"key": "< 64 KiB", "to": 65536},
                {"key": "64 KiB - 1 MiB", "from": 65536, "to": 1048576},
                {"key": "1 - 100 MiB", "from": 1048576, "to": 104857600},
                {"key": "> 100 MiB", "from": 104857600}]}},
        },
    }


def format_results(body: dict) -> dict:
    hits = body.get("hits", {})
    aggs = body.get("aggregations", {})
    return {
        "engine": "elasticsearch",
        "total": hits.get("total", {}).get("value", 0),
        "took_ms": body.get("took"),
        "results": [{**h["_source"], "score": h.get("_score"),
                     "highlight": h.get("highlight", {})}
                    for h in hits.get("hits", [])],
        "facets": {
            name: [{"value": b["key"], "count": b["doc_count"]}
                   for b in aggs.get(name, {}).get("buckets", [])]
            for name in ("buckets", "content_types", "sizes")
        },
    }


async def search(**params) -> dict:
    query = build_query(**params)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{ES_URL}/{ES_INDEX}/_search", json=query)
    if r.status_code >= 400:
        raise RuntimeError(f"search: {r.status_code} {r.text[:300]}")
    return format_results(r.json())
