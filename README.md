# Distributed Object Storage Framework (DS Quiz)

Content-addressed, chunked object storage over 4+ storage nodes, with a
coordinator (metadata + placement) backed by PostgreSQL.

## Layout
| Path | Role |
|---|---|
| `storage_node/` | FastAPI chunk server: PUT/GET/HEAD/DELETE by SHA-256, verify on read, heartbeat |
| `coordinator/` | Metadata & placement service: chunking, dedup with ref counts, node membership |
| `coordinator/ring.py` | Consistent hashing ring with virtual nodes |
| `coordinator/chunking.py` | Fixed-size and FastCDC content-defined chunking |
| `client/` | Command-line client (upload / download / info / delete / nodes / ring / stats) |
| `scripts/` | Workload generator, chunking comparison, placement experiment |
| `tests/` | pytest unit tests |
| `results/` | Benchmark outputs (JSON + Markdown) |
| `k8s/` | Kubernetes manifests |
| `monitoring/` | Prometheus config |

## 1. Local Python environment (Windows PowerShell)
```powershell
cd C:\DS-quiz
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Run the cluster with Docker Compose
```powershell
docker compose up --build -d
docker compose ps
curl.exe http://localhost:8000/v1/nodes
```

## 3. Try it
```powershell
python -m client.main upload test.txt
python -m client.main download <file_id> out.txt
python -m client.main nodes
```

## 4. Placement and chunking experiments (Task 3)
```powershell
python -m pytest -q tests
python scripts/placement_experiment.py                 # synthetic, 100k chunks
python scripts/gen_workloads.py small --out datasets/small
python scripts/gen_workloads.py large --out datasets/large --min-mb 100 --max-mb 300
python scripts/compare_chunking.py datasets/small datasets/large

# Add a 5th node to the real cluster and rebalance
docker compose --profile scale up -d storage-node-5
$h = @{ "X-Cluster-Token" = "dev-cluster-token-change-me" }
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/nodes -Headers $h -ContentType "application/json" -Body '{"node_id":"node5"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/admin/rebalance -Headers $h
```

## 5. Kubernetes (Docker Desktop)
Enable it in Docker Desktop -> Settings -> Kubernetes, then:
```powershell
kubectl config use-context docker-desktop
kubectl get nodes
kubectl apply -f k8s/00-namespace.yaml
kubectl get namespaces
```
