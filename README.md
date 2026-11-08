# Distributed Object Storage Framework (DS Quiz)

Content-addressed, chunked object storage over 4+ storage nodes. A
coordinator (metadata, placement, redundancy, repair) is backed by
PostgreSQL. Data is protected per bucket by replication with quorums or by
Reed-Solomon erasure coding; failed nodes are detected by heartbeats and
their data is rebuilt automatically; a scrubber detects bit rot.

## Architecture
```
 browser / clients
        |                      :3001 ui (nginx, React)   :3000 grafana  :9090 prometheus
        v
   :8080 lb (nginx, least_conn) ----> gateway-1, gateway-2   (API keys, rate limit, S3 API)
                                          |
                                          v
                               storage-coordinator (metadata, placement, repair planning)
                                 |         |          |                 \
                          postgres   elasticsearch   kafka (job queue)   storage-node-1..4 (+5,6)
                                                        |                       ^
                                                     worker x2 -----------------+
                                         (repair / rebalance / gc batches)
```

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
| `coordinator/jobs.py` | Job tracking + queue (Kafka, PostgreSQL fallback), at-least-once |
| `coordinator/worker.py` | Worker service executing repair / rebalance / gc batches |
| `observability/metrics.py` | Prometheus metrics (no dependency) used by every service |
| `docker/*.Dockerfile` | One Dockerfile per service |
| `lb/nginx.conf` | Load balancer in front of the 2 gateways |
| `monitoring/` | Prometheus config, Grafana provisioning and dashboard |
| `k8s/` | Kubernetes manifests (StatefulSet, CloudNativePG, HPA, ...), `kubectl apply -k k8s/` |
| `docs/kubernetes.md`, `docs/cloud-deployment.md` | Kubernetes and cloud deployment guides |
| `coordinator/search_index.py` | Elasticsearch index over metadata (async sync, full-text query, facets) |
| `ui/` | React web UI (source in `ui/src`, prebuilt bundle in `ui/dist`, served by nginx) |
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
Users talk to the **load balancer / gateways** on port 8080 with an API key;
port 8000 (coordinator) is the internal service API used by the CLI and scripts.

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
| `POST /presign` `{"bucket","key","version"?,"expires_in"?,"download"?}` | time-limited download URL (HMAC-signed, no key needed to use it) |
| `GET /search/text?q=&bucket=&prefix=&min_size=&max_size=&created_after=&created_before=&tags=&sort=relevance` | Elasticsearch full-text search with facets |
| `POST /admin/repair\|scrub\|rebalance\|gc\|reindex` | operations |

Rate limit: token bucket per key (`RATE_LIMIT_RPS`, default 20/s, burst
2x, or the key's own `rate_limit_rps`). Exceeding it returns **429** with
`Retry-After`; every response carries `X-RateLimit-Limit/Remaining`.

### Testing the API
```powershell
python -m pytest -v tests/integration      # 19 end-to-end tests via the gateway
.\scripts\api_demo.ps1                      # curl walkthrough (good for screenshots)
```
Postman: *Import* `docs/postman_collection.json`, then *Run collection*.

## 4. Web UI and Elasticsearch search (Task 6)
Open **http://localhost:3001** and sign in with an API key
(`dev-admin-key-change-me`).

| Page | What it shows / does |
|---|---|
| Cluster | node health (heartbeats), free space, blobs per node (chunk distribution), degraded/unavailable objects, repair progress and traffic; admin buttons for scrub / repair / rebalance |
| Browse | buckets (create with replication or EC k+m), folders, objects, download (presigned URL), delete, details: versions, tags, and the node of every replica / shard |
| Upload | drag and drop, progress bars; files >= 64 MiB use multipart upload (16 MiB parts) |
| Search | Elasticsearch full-text search (fuzzy key/tag matching, highlighting, facets) or exact PostgreSQL search |
| Settings | API keys (create / revoke), search-index status and rebuild |

Elasticsearch is a **derived index**: PostgreSQL remains the source of truth,
writes only mark keys as changed and a background task updates the index
in bulk (eventual consistency). `POST /admin/reindex` rebuilds it.

Rebuild the UI after changing `ui/src` (needs Node.js):
```powershell
cd ui; npm install; npm run build; cd ..
docker compose up -d --build ui
```

## 5. Objects, buckets and redundancy policies (CLI, internal API)
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

## 6. Placement and chunking experiments (Task 3)
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

## 7. Failures, repair and scrubbing (Task 4)
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

## 8. Load balancing, job queue, monitoring (Bonus 7)
| | |
|---|---|
| Load balancer | `lb` (nginx) on :8080, `least_conn` over `gateway-1` and `gateway-2`, passive health checks, retry on the other gateway. Every response has `X-Served-By`. |
| Job queue | repair / rebalance / gc jobs are split into batches on the Kafka topic `storage-jobs` (6 partitions); the 2 `worker` containers form the consumer group `storage-workers`. PostgreSQL table `job_queue` is the fallback. `GET /v1/jobs` shows jobs and progress. |
| Monitoring | every service exposes `/metrics`; Prometheus :9090, Grafana :3000 (admin / ChangeMe-123!) dashboard "Distributed Storage - Overview": throughput, latency, disk per node, degraded objects, repair rate, workers. |
| Elastic membership | a new storage node joins the ring by itself (`AUTO_JOIN`) and a rebalance job follows 15 s later. |

```powershell
docker compose up --build -d --remove-orphans
python scripts/load_test.py --duration 30        # requests split over gateway-1 / gateway-2
docker compose stop gateway-1                     # all traffic continues on gateway-2
docker compose --profile scale up -d storage-node-5   # auto-join + rebalance
curl.exe http://localhost:8000/v1/jobs
```

## 9. Kubernetes and cloud (Bonus 8)
StatefulSet with persistent volumes and liveness/readiness probes for the
storage nodes, a 3-instance replicated PostgreSQL (CloudNativePG, automatic
failover), Kafka, workers, an autoscaled gateway (HPA 2-6 pods), UI,
Prometheus and Grafana. See **docs/kubernetes.md** (Docker Desktop) and
**docs/cloud-deployment.md** (GKE / EKS / AKS).
```powershell
docker compose build
kubectl apply -k k8s/
```
