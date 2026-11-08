"""End-to-end API tests against a RUNNING cluster, through the gateway.

    docker compose up -d
    python -m pytest -v tests/integration

Settings (environment variables):
    GATEWAY_URL     default http://localhost:8080
    ADMIN_API_KEY   default dev-admin-key-change-me
The whole module is skipped if the gateway is not reachable.
"""

import hashlib
import os
import time
import uuid

import httpx
import pytest

URL = os.getenv("GATEWAY_URL", "http://localhost:8080").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "dev-admin-key-change-me")

try:
    httpx.get(f"{URL}/health", timeout=3)
except httpx.HTTPError:
    pytest.skip(f"gateway not reachable at {URL}", allow_module_level=True)


def client(key=ADMIN_KEY):
    headers = {"X-API-Key": key} if key else {}
    return httpx.Client(base_url=URL, headers=headers, timeout=120)


@pytest.fixture(scope="module")
def admin():
    with client() as c:
        yield c


@pytest.fixture(scope="module")
def bucket(admin):
    name = f"test-{uuid.uuid4().hex[:8]}"
    r = admin.post("/buckets", json={"name": name, "redundancy": "replication"})
    assert r.status_code == 201, r.text
    yield name
    admin.delete(f"/buckets/{name}", params={"force": "true"})


@pytest.fixture(scope="module")
def ec_bucket(admin):
    name = f"test-ec-{uuid.uuid4().hex[:8]}"
    r = admin.post("/buckets", json={"name": name, "redundancy": "ec",
                                     "k": 4, "m": 2})
    assert r.status_code == 201, r.text
    yield name
    admin.delete(f"/buckets/{name}", params={"force": "true"})


def make_key(admin, role, rps=None):
    body = {"name": f"pytest-{role}", "role": role}
    if rps:
        body["rate_limit_rps"] = rps
    r = admin.post("/admin/api-keys", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ---- authentication and authorisation ----------------------------------------

def test_health_is_public():
    r = httpx.get(f"{URL}/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_missing_key_is_401():
    with client(None) as c:
        assert c.get("/buckets").status_code == 401


def test_invalid_key_is_401():
    with client("dsk_not-a-real-key") as c:
        assert c.get("/buckets").status_code == 401


def test_bearer_token_header_works(admin):
    r = httpx.get(f"{URL}/me", headers={"Authorization": f"Bearer {ADMIN_KEY}"})
    assert r.status_code == 200 and r.json()["role"] == "admin"


def test_readonly_key_cannot_write(admin, bucket):
    key = make_key(admin, "readonly")
    with client(key["api_key"]) as c:
        assert c.get(f"/buckets/{bucket}/objects").status_code == 200
        r = c.put(f"/buckets/{bucket}/objects/ro.txt", content=b"x")
        assert r.status_code == 403
        assert c.get("/admin/api-keys").status_code == 403
    admin.delete(f"/admin/api-keys/{key['key_id']}")


def test_revoked_key_is_rejected(admin):
    key = make_key(admin, "readwrite")
    admin.delete(f"/admin/api-keys/{key['key_id']}")
    time.sleep(0.1)
    with client(key["api_key"]) as c:
        # the gateway caches identities briefly, so allow for the cache TTL
        deadline = time.time() + 35
        while c.get("/buckets").status_code != 401 and time.time() < deadline:
            time.sleep(2)
        assert c.get("/buckets").status_code == 401


def test_rate_limit_returns_429(admin):
    key = make_key(admin, "readonly", rps=2)      # burst = 4 requests
    with client(key["api_key"]) as c:
        codes = [c.get("/buckets").status_code for _ in range(12)]
        assert 429 in codes
        r = c.get("/buckets")
        if r.status_code == 429:
            assert int(r.headers["retry-after"]) >= 1
        time.sleep(1.5)                            # tokens refill
        assert c.get("/buckets").status_code == 200
    admin.delete(f"/admin/api-keys/{key['key_id']}")


# ---- buckets -----------------------------------------------------------------

def test_create_and_list_buckets(admin, bucket, ec_bucket):
    names = {b["name"]: b for b in admin.get("/buckets").json()["buckets"]}
    assert names[bucket]["redundancy"] == "replication"
    assert names[ec_bucket]["storage_overhead"] == 1.5
    assert admin.post("/buckets", json={"name": bucket}).status_code == 409
    assert admin.post("/buckets", json={"name": "Bad_Name!"}).status_code == 400


# ---- objects -----------------------------------------------------------------

DATA = os.urandom(9 * 1024 * 1024 + 777)        # > 2 chunks of 4 MiB


@pytest.mark.parametrize("bucket_fixture", ["bucket", "ec_bucket"])
def test_put_get_head_object(admin, request, bucket_fixture):
    b = request.getfixturevalue(bucket_fixture)
    r = admin.put(f"/buckets/{b}/objects/dir/data.bin", content=DATA,
                  headers={"Content-Type": "application/x-test",
                           "X-Tags": "env=test,team=ds"})
    assert r.status_code == 201, r.text
    meta = r.json()
    assert meta["size"] == len(DATA)
    assert meta["sha256"] == hashlib.sha256(DATA).hexdigest()
    assert meta["tags"] == {"env": "test", "team": "ds"}

    r = admin.get(f"/buckets/{b}/objects/dir/data.bin")
    assert r.status_code == 200 and r.content == DATA
    assert r.headers["content-type"].startswith("application/x-test")
    assert r.headers["accept-ranges"] == "bytes"

    h = admin.head(f"/buckets/{b}/objects/dir/data.bin")
    assert h.status_code == 200
    assert int(h.headers["content-length"]) == len(DATA)


def test_byte_ranges(admin, ec_bucket):
    admin.put(f"/buckets/{ec_bucket}/objects/range.bin", content=DATA)
    path = f"/buckets/{ec_bucket}/objects/range.bin"
    cases = {"bytes=0-99": DATA[:100],
             # crosses the boundary between chunk 1 and chunk 2 (4 MiB)
             "bytes=4194000-4194999": DATA[4194000:4195000],
             "bytes=-500": DATA[-500:],
             f"bytes={len(DATA) - 10}-": DATA[-10:]}
    for header, expected in cases.items():
        r = admin.get(path, headers={"Range": header})
        assert r.status_code == 206, (header, r.text)
        assert r.content == expected, header
        assert r.headers["content-range"].endswith(f"/{len(DATA)}")
    r = admin.get(path, headers={"Range": f"bytes={len(DATA)}-"})
    assert r.status_code == 416


def test_versions_metadata_and_delete(admin, bucket):
    path = f"/buckets/{bucket}/objects/notes.txt"
    admin.put(path, content=b"v1")
    admin.put(path, content=b"version two")
    assert admin.get(path).content == b"version two"
    assert admin.get(path, params={"version": 1}).content == b"v1"

    meta = admin.get(f"/buckets/{bucket}/metadata/notes.txt").json()
    assert meta["version"] == 2 and len(meta["versions"]) == 2
    assert meta["chunks"][0]["scheme"] == "replication"

    r = admin.delete(path, params={"version": 1})
    assert r.status_code == 200 and r.json()["versions_deleted"] == 1
    r = admin.delete(path)
    assert r.status_code == 200
    assert admin.get(path).status_code == 404


def test_list_with_prefix_and_pagination(admin, bucket):
    for i in range(5):
        admin.put(f"/buckets/{bucket}/objects/logs/2026/{i}.log", content=b"x" * i)
    admin.put(f"/buckets/{bucket}/objects/other.txt", content=b"y")
    r = admin.get(f"/buckets/{bucket}/objects",
                  params={"prefix": "logs/", "limit": 3}).json()
    assert r["count"] == 3 and r["is_truncated"]
    r2 = admin.get(f"/buckets/{bucket}/objects",
                   params={"prefix": "logs/", "after": r["next_after"]}).json()
    keys = [o["key"] for o in r["objects"] + r2["objects"]]
    assert keys == [f"logs/2026/{i}.log" for i in range(5)]


def test_form_upload(admin, bucket):
    r = admin.post(f"/buckets/{bucket}/objects",
                   files={"file": ("photo.jpg", b"\xff\xd8 fake jpeg",
                                   "image/jpeg")},
                   data={"tags": "kind=photo"})
    assert r.status_code == 201, r.text
    assert r.json()["key"] == "photo.jpg"
    assert admin.get(f"/buckets/{bucket}/objects/photo.jpg").content == \
        b"\xff\xd8 fake jpeg"


# ---- search ------------------------------------------------------------------

def test_search_by_prefix_size_date_and_tags(admin, bucket):
    admin.put(f"/buckets/{bucket}/objects/search/a.json", content=b"a" * 100,
              headers={"X-Tags": "project=alpha,owner=sara"})
    admin.put(f"/buckets/{bucket}/objects/search/b.json", content=b"b" * 5000,
              headers={"X-Tags": "project=beta"})
    admin.put(f"/buckets/{bucket}/objects/search/c.json", content=b"c" * 50,
              headers={"X-Tags": "project=alpha"})

    def keys(**params):
        r = admin.get("/search", params={"bucket": bucket, **params})
        assert r.status_code == 200, r.text
        return sorted(o["key"] for o in r.json()["results"])

    assert keys(prefix="search/") == ["search/a.json", "search/b.json",
                                      "search/c.json"]
    assert keys(prefix="search/", min_size=1000) == ["search/b.json"]
    assert keys(prefix="search/", max_size=100) == ["search/a.json",
                                                    "search/c.json"]
    assert keys(tags="project=alpha") == ["search/a.json", "search/c.json"]
    assert keys(tags="project=alpha,owner=sara") == ["search/a.json"]
    assert keys(tag_keys="owner") == ["search/a.json"]
    assert keys(prefix="search/", created_after="2000-01-01T00:00:00Z") \
        == ["search/a.json", "search/b.json", "search/c.json"]
    assert keys(prefix="search/", created_before="2000-01-01T00:00:00Z") == []

    admin.put(f"/buckets/{bucket}/tags/search/b.json",
              json={"tags": {"project": "alpha"}})
    assert keys(tags="project=alpha") == ["search/a.json", "search/b.json",
                                          "search/c.json"]
    r = admin.get("/search", params={"bucket": bucket, "prefix": "search/",
                                     "sort": "size", "order": "desc"}).json()
    assert [o["key"] for o in r["results"]][0] == "search/b.json"


# ---- multipart upload ----------------------------------------------------------

def test_multipart_upload(admin, ec_bucket):
    parts = [os.urandom(5 * 1024 * 1024), os.urandom(5 * 1024 * 1024),
             os.urandom(1234)]
    up = admin.post(f"/buckets/{ec_bucket}/uploads",
                    json={"key": "big/multi.bin", "tags": "src=multipart"})
    assert up.status_code == 201, up.text
    uid = up.json()["upload_id"]
    for n in (3, 1, 2):                              # any order
        r = admin.put(f"/uploads/{uid}/parts/{n}", content=parts[n - 1])
        assert r.status_code == 200, r.text
    # retry part 2 (replaces it)
    assert admin.put(f"/uploads/{uid}/parts/2", content=parts[1]).status_code == 200
    assert len(admin.get(f"/uploads/{uid}").json()["parts"]) == 3

    r = admin.post(f"/uploads/{uid}/complete")
    assert r.status_code == 201, r.text
    assert r.json()["etag"].endswith("-3")
    got = admin.get(f"/buckets/{ec_bucket}/objects/big/multi.bin")
    assert got.status_code == 200 and got.content == b"".join(parts)
    assert admin.get(f"/uploads/{uid}").status_code == 404


def test_abort_multipart_upload(admin, bucket):
    uid = admin.post(f"/buckets/{bucket}/uploads",
                     json={"key": "aborted.bin"}).json()["upload_id"]
    admin.put(f"/uploads/{uid}/parts/1", content=os.urandom(100_000))
    r = admin.delete(f"/uploads/{uid}")
    assert r.status_code == 200 and r.json()["aborted"]
    assert admin.get(f"/buckets/{bucket}/objects/aborted.bin").status_code == 404


# ---- cluster -------------------------------------------------------------------

def test_cluster_status(admin):
    s = admin.get("/cluster/status").json()
    assert len(s["nodes"]) >= 4
    for key in ("total_free_bytes", "blobs_per_node", "objects_degraded",
                "chunks_degraded", "repair"):
        assert key in s


def test_delete_bucket_requires_empty(admin):
    name = f"test-del-{uuid.uuid4().hex[:6]}"
    admin.post("/buckets", json={"name": name})
    admin.put(f"/buckets/{name}/objects/x", content=b"x")
    assert admin.delete(f"/buckets/{name}").status_code == 409
    assert admin.delete(f"/buckets/{name}",
                        params={"force": "true"}).status_code == 200
    assert admin.get(f"/buckets/{name}").status_code == 404
