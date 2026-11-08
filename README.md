# Distributed Object Storage Framework (DS Quiz)

Content-addressed, chunked object storage over 4+ storage nodes. A
coordinator (metadata, placement, redundancy, repair) is backed by
PostgreSQL. Data is protected per bucket by replication with quorums or by
Reed-Solomon erasure coding; failed nodes are detected by heartbeats and
their data is rebuilt automatically; a scrubber detects bit rot.

## Layout
| Path | Role |
|---|---|
| `storage_node/main.py` | Chunk server: PUT/GET/HEAD/DELETE by SHA-256, verify on read, heartbeat, scrubber, quarantine |
| `coordinator/config.py` | All settings (environment variables) |
| `coordinator/db.py` | PostgreSQL schema: buckets, object_versions, chunk_catalog, ring_members, node_heartbeats |
| `coordinator/ring.py` | Consistent hashing ring with virtual nodes |
| `coordinator/chunking.py` | Fixed-size and FastCDC content-defined chunking |
| `coordinator/erasure.py` | Reed-Solomon codec (zfec, or built-in pure Python) |
| `coordinator/cluster.py` | Membership, heartbeat failure detector, node I/O, placement |
| `coordinator/redundancy.py` | Write/read paths: replication with W/R quorums, erasure coding |
| `coordinator/repair.py` | Repair job, scrub reports, degraded objects, rebalance, orphan GC |
| `coordinator/objects.py` | Object operations shared by all APIs: streaming ingest, versions, range reads, deletes |
| `coordinator/api_objects.py` | S3-style internal object API, search, multipart uploads |
| `coordinator/api_keys.py` | API keys (hashed), roles |
| `coordinator/ranges.py` | HTTP Range parsing |
| `coordinator/main.py` | Core HTTP API (nodes, buckets, repair, admin) |
| `gateway/main.py` | **Public API gateway**: API-key auth, roles, rate limiting, streaming proxy |
| `gateway/ratelimit.py` | Token-bucket rate limiter |
| `docs/postman_collection.json` | Postman collection (36 requests with tests) |
| `client/main.py` | Command-line client |
| `scripts/` | Workload generator and experiments |
| `tests/` | pytest unit tests |
| `results/` | Benchmark outputs (JSON + Markdown) |

## 1. Local Python environment (Windows PowerShell)
```powershell
cd C:\DS-quiz
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pytest -q tests                 # unit tests (+ integration if the cluster runs)
```

## 2. Run the cluster
```powershell
docker compose up --build -d
docker compose ps
python -m client.main nodes
```
Users talk to the **gateway** on port 8080 with an API key; port 8000
(coordinator) is the internal service API used by the CLI and scripts.

## 3. Public API (gateway, port 8080)
Every request except `/health` needs `X-API-Key: <key>` (or
`Authorization: Bearer <key>`). The development admin key is
`dev-admin-key-change-me` (variable `ADMIN_API_KEY`).

| Method & path | Purpose |
|---|---|
| `GET /buckets`, `POST /buckets` `{"name","redundancy":"replication"\|"ec","k","m"}` | list / create buckets |
| `GET /buckets/{b}`, `DELETE /buckets/{b}[?force=true]`, `GET /buckets/{b}/stats` | bucket details, delete, overhead |
| `PUT /buckets/{b}/objects/{key}` (headers `Content-Type`, `X-Tags: k=v,k2=v2`, `X-Chunking: fixed\|cdc`) | streaming upload |
| `POST /buckets/{b}/objects` (multipart/form-data `file`, `key`, `tags`) | form upload |
| `GET /buckets/{b}/objects/{key}[?version=n]` (header `Range: bytes=a-b`) | download, 206 for ranges |
| `HEAD /buckets/{b}/objects/{key}` | metadata in headers |
| `DELETE /buckets/{b}/objects/{key}[?version=n]` | delete one / all versions |
| `GET /buckets/{b}/objects?prefix=&after=&limit=` | list (paginated) |
| `GET /buckets/{b}/metadata/{key}` | versions, tags, chunks, shard placement |
| `PUT /buckets/{b}/tags/{key}` `{"tags":{...}}` | replace tags |
| `GET /search?bucket=&prefix=&min_size=&max_size=&created_after=&created_before=&tags=k=v&tag_keys=&sort=key\|size\|created_at&order=` | search |
| `POST /buckets/{b}/uploads` `{"key"}` -> `PUT /uploads/{id}/parts/{n}` -> `POST /uploads/{id}/complete` (`DELETE` aborts) | multipart upload |
| `GET /cluster/status`, `/cluster/nodes`, `/cluster/repair`, `/stats` | cluster state |
| `POST /admin/api-keys` `{"name","role":"admin\|readwrite\|readonly","rate_limit_rps"}`, `GET /admin/api-keys`, `DELETE /admin/api-keys/{id}` | key management |
| `POST /admin/repair\|scrub\|rebalance\|gc` | operations |

Rate limit: token bucket per key (`RATE_LIMIT_RPS`, default 20/s, burst
2x, or the key's own `rate_limit_rps`). Exceeding it returns **429** with
`Retry-After`; every response carries `X-RateLimit-Limit/Remaining`.

### Testing the API
```powershell
python -m pytest -v tests/integration      # 19 end-to-end tests via the gateway
.\scripts\api_demo.ps1                      # curl walkthrough (good for screenshots)
```
Postman: *Import* `docs/postman_collection.json`, then *Run collection*.

## 4. Objects, buckets and redundancy policies (CLI, internal API)
```powershell
python -m client.main bucket-create photos  --redundancy replication
python -m client.main bucket-create archive --redundancy ec --k 4 --m 2
python -m client.main buckets
python -m client.main upload test.txt --bucket archive
python -m client.main info <file_id>          # chunks, shards and their nodes
python -m client.main download <file_id> out.txt
python -m client.main bucket-stats archive    # storage overhead
python -m client.main status                  # nodes, blobs per node, degraded objects
```

## 5. Placement and chunking experiments (Task 3)
```powershell
python scripts/placement_experiment.py
python scripts/gen_workloads.py small --out datasets/small
python scripts/gen_workloads.py large --out datasets/large --min-mb 100 --max-mb 300
python scripts/compare_chunking.py datasets/small datasets/large

docker compose --profile scale up -d storage-node-5
$h = @{ "X-Cluster-Token" = "dev-cluster-token-change-me" }
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/nodes -Headers $h -ContentType "application/json" -Body '{"node_id":"node5"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/rebalance -Headers $h
```

## 6. Failures, repair and scrubbing (Task 4)
```powershell
docker compose stop storage-node-4              # node failure
python -m client.main nodes                     # node4 -> failed after ~15 s
python -m client.main repair-status             # repair job and traffic
python -m client.main status                    # degraded objects -> 0
docker compose start storage-node-4

# Bit rot: corrupt one stored blob, then scrub
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/fault/corrupt -Headers $h -ContentType "application/json" -Body '{"node_id":"node1","hash":"<blob hash from info>"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/scrub -Headers $h

# Orphans left on a node that came back after its data was rebuilt elsewhere
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/v1/admin/gc?min_age_seconds=60" -Headers $h

# Replication vs erasure coding: overhead and repair traffic, both workloads
python scripts/redundancy_benchmark.py --small datasets/small --large datasets/large
```

## Main settings (docker-compose.yml)
| Variable | Default | Meaning |
|---|---|---|
| `REPLICATION_FACTOR` / `WRITE_QUORUM` / `READ_QUORUM` | 3 / 2 / 2 | N, W, R (W + R > N) |
| `EC_K` / `EC_M` | 4 / 2 | default Reed-Solomon parameters for EC buckets |
| `HEARTBEAT_INTERVAL` (nodes) / `HEARTBEAT_TIMEOUT` | 5 s / 15 s | failure detector |
| `REPAIR_INTERVAL` | 10 s | repair loop period |
| `SCRUB_INTERVAL` (nodes) | 300 s | full scrub period |
| `VNODES` | 128 | virtual nodes per storage node |
| `ALLOW_FAULT_INJECTION` (nodes) | true | enables the bit-rot demo endpoint (disable in production) |

## 7. Kubernetes (Docker Desktop)
```powershell
kubectl config use-context docker-desktop
kubectl get nodes
kubectl apply -f k8s/00-namespace.yaml
```
