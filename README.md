# Distributed Object Storage Framework (DS Quiz)

Content-addressed, chunked object storage over 4+ storage nodes, with a
coordinator (metadata + placement) backed by PostgreSQL.

## Layout
| Path | Role |
|---|---|
| `storage_node/` | FastAPI chunk server: PUT/GET/HEAD/DELETE by SHA-256, verify on read, heartbeat |
| `coordinator/` | Metadata & placement service: chunking, dedup with ref counts, node membership |
| `client/` | Command-line client (upload / download / nodes) |
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

## 4. Kubernetes (Docker Desktop)
Enable it in Docker Desktop -> Settings -> Kubernetes, then:
```powershell
kubectl config use-context docker-desktop
kubectl get nodes
kubectl apply -f k8s/00-namespace.yaml
kubectl get namespaces
```
