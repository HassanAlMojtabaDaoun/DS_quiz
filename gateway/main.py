"""API gateway: the public, S3-style entry point of the storage system.

  client --HTTPS--> gateway (auth, rate limit, routing) --> coordinator

The gateway is stateless (no database, no disk): any number of copies can
run behind a load balancer. It authenticates every request with an API key
(X-API-Key header or "Authorization: Bearer <key>"), checks the key's role,
applies a per-key rate limit, and forwards the request to the coordinator,
streaming request and response bodies so large objects are never buffered.

Public API
  GET    /health                                 (no key needed)
  GET    /me                                     who am I (role, limits)
  GET    /buckets                     POST /buckets
  GET    /buckets/{b}                 DELETE /buckets/{b}[?force=true]
  GET    /buckets/{b}/stats
  GET    /buckets/{b}/objects?prefix=&after=&limit=
  PUT    /buckets/{b}/objects/{key}   streaming upload (X-Tags, X-Chunking)
  POST   /buckets/{b}/objects         multipart/form-data upload (file, key, tags)
  GET    /buckets/{b}/objects/{key}   download (Range: bytes=a-b -> 206)
  HEAD   /buckets/{b}/objects/{key}
  DELETE /buckets/{b}/objects/{key}[?version=n]
  GET    /buckets/{b}/metadata/{key}  metadata, versions, chunk placement
  PUT    /buckets/{b}/tags/{key}      {"tags": {...}}
  GET    /search?bucket=&prefix=&min_size=&max_size=&created_after=
                 &created_before=&tags=k=v,k2=v2&tag_keys=&sort=&order=
  POST   /buckets/{b}/uploads         multipart upload: start
  PUT    /uploads/{id}/parts/{n}      upload one part
  GET    /uploads/{id}                POST /uploads/{id}/complete
  DELETE /uploads/{id}                abort
  GET    /cluster/status  /cluster/nodes  /cluster/repair  /stats
  admin: /admin/api-keys, /admin/repair, /admin/scrub, /admin/rebalance,
         /admin/gc, /admin/nodes
"""

import hashlib
import hmac
import json
import os
import socket
import time
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, quote, urlencode

import httpx
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.middleware.cors import CORSMiddleware

from gateway.ratelimit import RateLimiter

COORDINATOR_URL = os.getenv("COORDINATOR_URL",
                            "http://storage-coordinator:8000").rstrip("/")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "dev-cluster-token-change-me")
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "20"))
RATE_LIMIT_BURST_FACTOR = float(os.getenv("RATE_LIMIT_BURST_FACTOR", "2"))
AUTH_CACHE_SECONDS = float(os.getenv("AUTH_CACHE_SECONDS", "30"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
GATEWAY_ID = os.getenv("GATEWAY_ID") or socket.gethostname()
# Shared by all gateway instances: signs presigned download URLs.
PRESIGN_SECRET = os.getenv("PRESIGN_SECRET", "dev-presign-secret-change-me")
PRESIGN_MAX_SECONDS = 3600
SIGNATURE_PARAMS = ("X-Expires", "X-KeyId", "X-Signature")

OPEN_PATHS = {"/health", "/docs", "/openapi.json"}
REQUEST_HEADERS = ("range", "if-none-match", "content-type", "x-chunking",
                   "x-tags", "content-length")
RESPONSE_HEADERS = ("content-type", "content-length", "content-range",
                    "accept-ranges", "etag", "last-modified",
                    "content-disposition", "retry-after", "x-object-version",
                    "x-object-id", "x-object-sha256", "x-object-tags",
                    "x-file-sha256")
ROLE_RANK = {"readonly": 0, "readwrite": 1, "admin": 2}

limiter = RateLimiter(RATE_LIMIT_RPS, RATE_LIMIT_BURST_FACTOR)
failed_auth = RateLimiter(5.0, 4.0)          # per client IP, bad keys only
_auth_cache: dict[str, tuple[float, dict | None]] = {}
_http: dict[str, httpx.AsyncClient] = {}


def http() -> httpx.AsyncClient:
    client = _http.get("c")
    if client is None or client.is_closed:
        client = _http["c"] = httpx.AsyncClient(
            base_url=COORDINATOR_URL,
            timeout=httpx.Timeout(30.0, read=None, write=None))
    return client


# ---------------------------------------------------------------------------
# Authentication, authorisation and rate limiting (ASGI middleware)
# ---------------------------------------------------------------------------

async def resolve_key(api_key: str) -> dict | None:
    """API key -> identity, via the coordinator, cached for a short time."""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    cached = _auth_cache.get(key_hash)
    now = time.monotonic()
    if cached and cached[0] > now:
        return cached[1]
    r = await http().post("/v1/internal/auth", json={"key_hash": key_hash},
                          headers={"X-Cluster-Token": CLUSTER_TOKEN})
    if r.status_code == 200:
        ident = r.json()
        _auth_cache[key_hash] = (now + AUTH_CACHE_SECONDS, ident)
        return ident
    if r.status_code == 401:
        _auth_cache[key_hash] = (now + 5, None)
        return None
    raise RuntimeError(f"auth backend returned {r.status_code}")


def presign_signature(method: str, path: str, version: str, expires: int,
                      key_id: str) -> str:
    message = f"{method}\n{path}\n{version}\n{expires}\n{key_id}"
    return hmac.new(PRESIGN_SECRET.encode(), message.encode(),
                    hashlib.sha256).hexdigest()


def check_presigned(method: str, path: str, query: str) -> dict | None:
    """Identity for a valid presigned URL (download only), else None.

    Like an S3 presigned URL: anyone holding the link can download that one
    object until it expires, without an API key. The HMAC prevents changing
    the path, version or expiry."""
    if method not in ("GET", "HEAD") or "/objects/" not in path:
        return None
    params = {k: v[0] for k, v in parse_qs(query).items()}
    try:
        expires = int(params["X-Expires"])
        key_id, signature = params["X-KeyId"], params["X-Signature"]
    except (KeyError, ValueError):
        return None
    if expires < time.time():
        return None
    expected = presign_signature("GET", quote(path, safe="/"),
                                 params.get("version", ""), expires, key_id)
    if not hmac.compare_digest(expected, signature):
        return None
    return {"key_id": key_id, "role": "readonly", "presigned": True}


def required_role(method: str, path: str) -> str:
    if path.startswith("/admin"):
        return "admin"
    if method in ("GET", "HEAD") or path == "/presign":
        return "readonly"
    return "readwrite"


class AuthMiddleware:
    """Pure ASGI middleware, so streamed bodies pass through untouched."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        extra = [(b"x-served-by", GATEWAY_ID.encode())]
        path, method = scope["path"], scope["method"]

        async def reply(status, detail, headers=()):
            body = json.dumps({"detail": detail}).encode()
            await send({"type": "http.response.start", "status": status,
                        "headers": [(b"content-type", b"application/json"),
                                    (b"content-length", str(len(body)).encode()),
                                    *extra, *headers]})
            await send({"type": "http.response.body", "body": body})

        if path not in OPEN_PATHS and method != "OPTIONS":
            headers = {k.decode().lower(): v.decode()
                       for k, v in scope["headers"]}
            api_key = headers.get("x-api-key", "")
            auth = headers.get("authorization", "")
            if not api_key and auth.lower().startswith("bearer "):
                api_key = auth[7:].strip()
            client_ip = (scope.get("client") or ("?", 0))[0]

            query = scope.get("query_string", b"").decode()
            if not api_key and "X-Signature=" in query:
                ident = check_presigned(method, path, query)
                if ident is None:
                    return await reply(403, "Invalid or expired presigned URL")
            elif not api_key:
                return await reply(401, "Missing API key (X-API-Key header)",
                                   [(b"www-authenticate", b"Bearer")])
            else:
                try:
                    ident = await resolve_key(api_key)
                except (httpx.HTTPError, RuntimeError):
                    return await reply(503, "Authentication service unavailable")
            if ident is None:
                allowed, _, retry, _ = failed_auth.take(f"ip:{client_ip}")
                if not allowed:
                    return await reply(429, "Too many failed authentications",
                                       [(b"retry-after",
                                         str(max(1, round(retry))).encode())])
                return await reply(401, "Invalid API key",
                                   [(b"www-authenticate", b"Bearer")])

            need = required_role(method, path)
            if ROLE_RANK[ident["role"]] < ROLE_RANK[need]:
                return await reply(403, f"Role '{ident['role']}' cannot "
                                        f"perform this operation "
                                        f"(needs '{need}')")

            allowed, left, retry, limit = limiter.take(
                ident["key_id"], ident.get("rate_limit_rps"))
            extra += [(b"x-ratelimit-limit", str(limit).encode()),
                      (b"x-ratelimit-remaining", str(left).encode())]
            if not allowed:
                return await reply(429, "Rate limit exceeded",
                                   [(b"retry-after",
                                     str(max(1, round(retry + 0.5))).encode())])
            scope["gateway.identity"] = ident

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                message = {**message,
                           "headers": list(message.get("headers", [])) + extra}
            await send(message)

        await self.app(scope, receive, send_with_headers)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    if "c" in _http:
        await _http["c"].aclose()


app = FastAPI(title="Distributed Storage Gateway", version="1.0.0",
              lifespan=lifespan)
app.add_middleware(AuthMiddleware)
app.add_middleware(
    CORSMiddleware, allow_origins=CORS_ORIGINS, allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Content-Length", "ETag", "Accept-Ranges",
                    "X-Object-Version", "X-Object-Id", "X-Object-Tags",
                    "X-RateLimit-Limit", "X-RateLimit-Remaining",
                    "Retry-After", "X-Served-By"])


# ---------------------------------------------------------------------------
# Forwarding to the coordinator
# ---------------------------------------------------------------------------

def _q(key: str) -> str:
    return quote(key, safe="/")


def _pass_headers(request: Request, admin: bool = False) -> dict:
    h = {k: v for k, v in request.headers.items() if k.lower() in REQUEST_HEADERS}
    if admin:
        h["X-Cluster-Token"] = CLUSTER_TOKEN
    return h


def _response_headers(r: httpx.Response) -> dict:
    return {k: v for k, v in r.headers.items() if k.lower() in RESPONSE_HEADERS}


async def forward(request: Request, method: str, path: str, *,
                  json_body=None, raw_body: bool = False,
                  stream_response: bool = False, admin: bool = False,
                  params=None):
    headers = _pass_headers(request, admin)
    if params is None:
        params = {k: v for k, v in request.query_params.items()
                  if k not in SIGNATURE_PARAMS and k != "download"}
    kwargs = {"headers": headers, "params": params}
    if json_body is not None:
        kwargs["json"] = json_body
        headers.pop("content-length", None)
        headers.pop("content-type", None)
    elif raw_body:
        kwargs["content"] = request.stream()      # streamed, never buffered
    else:
        headers.pop("content-length", None)
    try:
        req = http().build_request(method, path, **kwargs)
        r = await http().send(req, stream=True)
    except httpx.HTTPError as exc:
        return JSONResponse({"detail": f"Coordinator unreachable: {exc}"}, 502)

    if stream_response and r.status_code in (200, 206):
        out_headers = _response_headers(r)
        if request.query_params.get("download"):
            name = path.rsplit("/", 1)[-1] or "download"
            out_headers["Content-Disposition"] = f'attachment; filename="{name}"'
        return StreamingResponse(r.aiter_raw(), status_code=r.status_code,
                                 headers=out_headers,
                                 background=BackgroundTask(r.aclose))
    body = await r.aread()
    await r.aclose()
    headers = _response_headers(r)
    if method == "HEAD":                 # no body, keep the real length
        return Response(status_code=r.status_code, headers=headers)
    headers.pop("content-length", None)
    return Response(content=body, status_code=r.status_code, headers=headers)


async def _json(request: Request):
    raw = await request.body()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    try:
        r = await http().get("/health", timeout=5)
        coordinator = r.json()
    except (httpx.HTTPError, ValueError) as exc:
        coordinator = {"status": "unreachable", "error": str(exc)}
    return {"status": "ok", "gateway": GATEWAY_ID,
            "default_rate_limit_rps": RATE_LIMIT_RPS,
            "coordinator": coordinator}


@app.get("/me")
async def whoami(request: Request):
    return request.scope.get("gateway.identity")


# ---- buckets ----------------------------------------------------------------

@app.get("/buckets")
async def list_buckets(request: Request):
    return await forward(request, "GET", "/v1/buckets")


@app.post("/buckets")
async def create_bucket(request: Request):
    body = await _json(request)
    if body is None:
        return JSONResponse({"detail": "Body must be JSON"}, 400)
    return await forward(request, "POST", "/v1/buckets", json_body=body)


@app.get("/buckets/{bucket}")
async def get_bucket(bucket: str, request: Request):
    return await forward(request, "GET", f"/v1/buckets/{_q(bucket)}")


@app.delete("/buckets/{bucket}")
async def delete_bucket(bucket: str, request: Request):
    return await forward(request, "DELETE", f"/v1/buckets/{_q(bucket)}")


@app.get("/buckets/{bucket}/stats")
async def bucket_stats(bucket: str, request: Request):
    return await forward(request, "GET", f"/v1/buckets/{_q(bucket)}/stats")


# ---- objects ----------------------------------------------------------------

@app.get("/buckets/{bucket}/objects")
async def list_objects(bucket: str, request: Request):
    return await forward(request, "GET", f"/v1/objects/{_q(bucket)}")


@app.post("/buckets/{bucket}/objects")
async def upload_form(bucket: str, request: Request,
                      file: UploadFile = File(...),
                      key: str | None = Form(None),
                      tags: str | None = Form(None),
                      chunking: str | None = Form(None)):
    """Browser/Postman friendly upload (multipart/form-data)."""
    object_key = key or file.filename
    if not object_key:
        return JSONResponse({"detail": "key or filename required"}, 400)

    async def body():
        while True:
            block = await file.read(1024 * 1024)
            if not block:
                break
            yield block

    headers = {"content-type": file.content_type or "application/octet-stream"}
    if tags:
        headers["x-tags"] = tags
    if chunking:
        headers["x-chunking"] = chunking
    try:
        r = await http().put(f"/v1/objects/{_q(bucket)}/{_q(object_key)}",
                             content=body(), headers=headers)
    except httpx.HTTPError as exc:
        return JSONResponse({"detail": f"Coordinator unreachable: {exc}"}, 502)
    finally:
        await file.close()
    return Response(content=r.content, status_code=r.status_code,
                    media_type="application/json")


@app.put("/buckets/{bucket}/objects/{key:path}")
async def put_object(bucket: str, key: str, request: Request):
    return await forward(request, "PUT", f"/v1/objects/{_q(bucket)}/{_q(key)}",
                         raw_body=True)


@app.get("/buckets/{bucket}/objects/{key:path}")
async def get_object(bucket: str, key: str, request: Request):
    return await forward(request, "GET", f"/v1/objects/{_q(bucket)}/{_q(key)}",
                         stream_response=True)


@app.head("/buckets/{bucket}/objects/{key:path}")
async def head_object(bucket: str, key: str, request: Request):
    return await forward(request, "HEAD", f"/v1/objects/{_q(bucket)}/{_q(key)}")


@app.delete("/buckets/{bucket}/objects/{key:path}")
async def delete_object(bucket: str, key: str, request: Request):
    return await forward(request, "DELETE",
                         f"/v1/objects/{_q(bucket)}/{_q(key)}")


@app.get("/buckets/{bucket}/metadata/{key:path}")
async def object_metadata(bucket: str, key: str, request: Request):
    return await forward(request, "GET", f"/v1/meta/{_q(bucket)}/{_q(key)}")


@app.put("/buckets/{bucket}/tags/{key:path}")
async def put_tags(bucket: str, key: str, request: Request):
    body = await _json(request)
    if body is None:
        return JSONResponse({"detail": "Body must be JSON"}, 400)
    return await forward(request, "PUT", f"/v1/tags/{_q(bucket)}/{_q(key)}",
                         json_body=body)


@app.get("/search")
async def search(request: Request):
    """Structured search in PostgreSQL (exact filters)."""
    return await forward(request, "GET", "/v1/search")


@app.get("/search/text")
async def text_search(request: Request):
    """Full-text search in Elasticsearch (fuzzy, facets, highlighting)."""
    return await forward(request, "GET", "/v1/search/text")


@app.get("/search/index")
async def search_index_status(request: Request):
    return await forward(request, "GET", "/v1/search/index")


@app.post("/presign")
async def presign(request: Request):
    """Create a time-limited download URL for one object (no key needed
    to use it). Body: {"bucket", "key", "version"?, "expires_in"?}"""
    body = await _json(request) or {}
    bucket, key = body.get("bucket"), body.get("key")
    if not bucket or not key:
        return JSONResponse({"detail": "bucket and key are required"}, 400)
    try:
        ttl = int(body.get("expires_in", 300))
    except (TypeError, ValueError):
        return JSONResponse({"detail": "expires_in must be an integer"}, 400)
    ttl = max(1, min(ttl, PRESIGN_MAX_SECONDS))
    ident = request.scope.get("gateway.identity") or {}
    version = str(body.get("version") or "")
    expires = int(time.time()) + ttl
    path = f"/buckets/{_q(bucket)}/objects/{_q(key)}"
    params = {"X-Expires": expires, "X-KeyId": ident.get("key_id", ""),
              "X-Signature": presign_signature("GET", path, version, expires,
                                               ident.get("key_id", ""))}
    if version:
        params["version"] = version
    if body.get("download"):
        params["download"] = "1"
    return {"url": f"{path}?{urlencode(params)}", "expires_at": expires,
            "expires_in": ttl}


# ---- multipart uploads --------------------------------------------------------

@app.post("/buckets/{bucket}/uploads")
async def start_upload(bucket: str, request: Request):
    body = await _json(request)
    if body is None:
        return JSONResponse({"detail": "Body must be JSON"}, 400)
    return await forward(request, "POST", "/v1/uploads",
                         json_body={**body, "bucket": bucket})


@app.get("/uploads")
async def list_uploads(request: Request):
    return await forward(request, "GET", "/v1/uploads")


@app.get("/uploads/{upload_id}")
async def get_upload(upload_id: str, request: Request):
    return await forward(request, "GET", f"/v1/uploads/{_q(upload_id)}")


@app.put("/uploads/{upload_id}/parts/{part_number}")
async def upload_part(upload_id: str, part_number: int, request: Request):
    return await forward(request, "PUT",
                         f"/v1/uploads/{_q(upload_id)}/parts/{part_number}",
                         raw_body=True)


@app.post("/uploads/{upload_id}/complete")
async def complete_upload(upload_id: str, request: Request):
    body = await _json(request)
    return await forward(request, "POST",
                         f"/v1/uploads/{_q(upload_id)}/complete",
                         json_body=body or {})


@app.delete("/uploads/{upload_id}")
async def abort_upload(upload_id: str, request: Request):
    return await forward(request, "DELETE", f"/v1/uploads/{_q(upload_id)}")


# ---- cluster ------------------------------------------------------------------

@app.get("/cluster/status")
async def cluster_status(request: Request):
    return await forward(request, "GET", "/v1/cluster/status")


@app.get("/cluster/nodes")
async def cluster_nodes(request: Request):
    return await forward(request, "GET", "/v1/nodes")


@app.get("/cluster/repair")
async def cluster_repair(request: Request):
    return await forward(request, "GET", "/v1/repair/status")


@app.get("/stats")
async def stats(request: Request):
    return await forward(request, "GET", "/v1/stats")


# ---- admin (role: admin) ------------------------------------------------------

@app.get("/admin/api-keys")
async def list_keys(request: Request):
    return await forward(request, "GET", "/v1/api-keys", admin=True)


@app.post("/admin/api-keys")
async def create_key(request: Request):
    body = await _json(request)
    if body is None:
        return JSONResponse({"detail": "Body must be JSON"}, 400)
    return await forward(request, "POST", "/v1/api-keys", json_body=body,
                         admin=True)


@app.delete("/admin/api-keys/{key_id}")
async def revoke_key(key_id: str, request: Request):
    return await forward(request, "DELETE", f"/v1/api-keys/{_q(key_id)}",
                         admin=True)


@app.post("/admin/{action}")
async def admin_action(action: str, request: Request):
    paths = {"repair": "/v1/admin/repair", "scrub": "/v1/admin/scrub",
             "rebalance": "/v1/admin/rebalance", "gc": "/v1/admin/gc",
             "nodes": "/v1/admin/nodes", "reindex": "/v1/admin/reindex"}
    if action not in paths:
        return JSONResponse({"detail": "Unknown admin action"}, 404)
    body = await _json(request) if action == "nodes" else None
    return await forward(request, "POST", paths[action], json_body=body,
                         admin=True)


@app.delete("/admin/nodes/{node_id}")
async def remove_node(node_id: str, request: Request):
    return await forward(request, "DELETE", f"/v1/admin/nodes/{_q(node_id)}",
                         admin=True)
