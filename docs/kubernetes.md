# Kubernetes deployment (Docker Desktop, then any cloud)

Everything runs in the namespace `ds-storage`:

| Kind | Name | Why |
|---|---|---|
| StatefulSet + headless Service | `storage-node` (4 pods, 5 Gi PVC each) | stable identity (`storage-node-0..3`) and own disk per node; liveness `/health`, readiness `/ready` |
| CloudNativePG `Cluster` | `storage-db` (3 instances) | replicated PostgreSQL, automatic failover - no single point of failure for metadata |
| StatefulSet | `kafka` | job queue (repair / rebalance / gc batches) |
| StatefulSet | `elasticsearch` | metadata search index |
| Deployment | `storage-coordinator` (1) | metadata service, leader loops (failure detection, planning) |
| Deployment | `worker` (2) | Kafka consumer group executing the jobs |
| Deployment + LoadBalancer + HPA | `gateway` (2..6) | public API, autoscaled on CPU |
| Deployment + LoadBalancer | `ui`, `prometheus`, `grafana` | web UI and monitoring |

## 1. Prerequisites (once)

1. Docker Desktop -> Settings -> Kubernetes -> **Enable Kubernetes**
   (cluster type *kubeadm*, so the cluster sees the images built locally).
2. Build the images (tags `ds-storage/*:1.0`):
   ```powershell
   docker compose build
   ```
3. Install the **CloudNativePG** operator (replicated PostgreSQL):
   ```powershell
   kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.24/releases/cnpg-1.24.1.yaml
   kubectl -n cnpg-system rollout status deployment cnpg-controller-manager
   ```
4. Install **metrics-server** (needed by the autoscaler). Docker Desktop's
   kubelet uses a self-signed certificate, hence the extra flag:
   ```powershell
   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
   kubectl -n kube-system patch deployment metrics-server --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
   ```
5. Stop the Docker Compose stack first (same ports): `docker compose down`

## 2. Deploy
```powershell
kubectl apply -k k8s/
kubectl -n ds-storage get pods -w          # wait until everything is Running
kubectl -n ds-storage get svc              # LoadBalancer services -> localhost
```
The PostgreSQL cluster takes 1-2 minutes; the coordinator and workers
restart until its secret `storage-db-app` exists - that is expected.

| URL | |
|---|---|
| http://localhost:8080 | API gateway (Service `gateway`, load-balanced over the pods) |
| http://localhost:3001 | web UI |
| http://localhost:3000 | Grafana (admin / ChangeMe-123!) |
| http://localhost:9090 | Prometheus |

## 3. Demonstrations for the report

**StatefulSet + persistent volumes**
```powershell
kubectl -n ds-storage get statefulset,pvc
kubectl -n ds-storage delete pod storage-node-2        # it comes back as storage-node-2 with the same PVC
```

**Liveness / readiness probes** - make a node report itself unhealthy
(fault injection); after 3 failed liveness probes (~30 s) Kubernetes kills
and restarts the container, with its data volume intact:
```powershell
kubectl -n ds-storage exec storage-node-1 -- python -c "import urllib.request as u; print(u.urlopen(u.Request('http://127.0.0.1:8000/v1/debug/unhealthy', method='POST', headers={'X-Cluster-Token':'dev-cluster-token-change-me'})).read())"
kubectl -n ds-storage get pods -w                      # RESTARTS 0 -> 1
kubectl -n ds-storage describe pod storage-node-1      # events: "Liveness probe failed ... will be restarted"
```

**Adding a storage node triggers rebalancing**
```powershell
kubectl -n ds-storage scale statefulset storage-node --replicas=5
kubectl -n ds-storage logs deploy/storage-coordinator | Select-String membership
curl.exe http://localhost:8080/cluster/nodes -H "X-API-Key: dev-admin-key-change-me"
```
`storage-node-4` sends its first heartbeat, joins the ring automatically and
15 s later a rebalance job moves ~1/5 of the chunk copies onto it (see
`/cluster/status` blobs per node, or the Grafana "Data stored per node" panel).

**Replicated metadata store (no single point of failure)**
```powershell
kubectl -n ds-storage get cluster storage-db           # 3 instances, primary shown
kubectl -n ds-storage get pods -l cnpg.io/cluster=storage-db -L role
kubectl -n ds-storage delete pod <the pod with role=primary>
kubectl -n ds-storage get pods -l cnpg.io/cluster=storage-db -L role -w
```
A replica is promoted within seconds; `storage-db-rw` now points to it and
the API keeps working.

**Gateway autoscaling (HPA)**
```powershell
kubectl -n ds-storage get hpa gateway -w               # in a second window
python scripts/load_test.py --url http://localhost:8080 --duration 180 --concurrency 32
```
When average CPU of the gateway pods exceeds 60 % of their request, the HPA
adds pods (up to 6); the load test output shows the new pod names in
"served by". After the load stops, it scales back to 2 (after ~1 minute).

**Workers share the jobs** - stop a node to create repair work:
```powershell
kubectl -n ds-storage scale statefulset storage-node --replicas=3   # removes storage-node-3 (and -4)
kubectl -n ds-storage logs deploy/worker --all-containers --prefix | Select-String batch
```
(scale back to 4 afterwards).

## 4. Clean up
```powershell
kubectl delete -k k8s/
kubectl -n ds-storage delete pvc --all
```
