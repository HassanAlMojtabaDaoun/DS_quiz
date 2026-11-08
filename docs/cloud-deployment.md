# Deploying on a cloud Kubernetes service

The manifests in `k8s/` are cloud-neutral: the only differences from
Docker Desktop are that the images must be in a registry the cluster can
pull from, and that `LoadBalancer` services get a public IP. Below: Google
Kubernetes Engine (GKE). Amazon EKS and Azure AKS differ only in the
cluster/registry commands (shown at the end).

## 1. Push the images to a registry
GitHub Container Registry works with every cloud:
```powershell
docker compose build
echo $env:GITHUB_TOKEN | docker login ghcr.io -u <github-user> --password-stdin
foreach ($s in "storage-node","coordinator","gateway","worker","ui") {
  docker tag ds-storage/${s}:1.0 ghcr.io/<github-user>/ds-storage-${s}:1.0
  docker push ghcr.io/<github-user>/ds-storage-${s}:1.0
}
```
Make the packages public (GitHub -> Packages -> settings) or add an
`imagePullSecret`. Then add to `k8s/kustomization.yaml`:
```yaml
images:
  - { name: ds-storage/storage-node, newName: ghcr.io/<github-user>/ds-storage-storage-node }
  - { name: ds-storage/coordinator,  newName: ghcr.io/<github-user>/ds-storage-coordinator }
  - { name: ds-storage/gateway,      newName: ghcr.io/<github-user>/ds-storage-gateway }
  - { name: ds-storage/worker,       newName: ghcr.io/<github-user>/ds-storage-worker }
  - { name: ds-storage/ui,           newName: ghcr.io/<github-user>/ds-storage-ui }
```

## 2. Create the cluster (GKE)
```powershell
gcloud auth login
gcloud config set project <project-id>
gcloud container clusters create ds-storage --zone europe-west1-b `
    --num-nodes 3 --machine-type e2-standard-4 --enable-autoscaling --min-nodes 3 --max-nodes 5
gcloud container clusters get-credentials ds-storage --zone europe-west1-b
```
GKE ships metrics-server, so the HPA works directly. PersistentVolumeClaims
use the default `standard-rwo` storage class (Persistent Disk).

## 3. Install the operator and deploy
```powershell
kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.24/releases/cnpg-1.24.1.yaml
kubectl apply -k k8s/
kubectl -n ds-storage get svc gateway ui grafana      # EXTERNAL-IP columns
```
Then use `http://<gateway EXTERNAL-IP>:8080` instead of localhost.

**Before exposing it publicly**: change the secrets in `k8s/01-config.yaml`
(cluster token, admin key, presign secret), set `ALLOW_FAULT_INJECTION` to
"false" on the storage nodes, and put the gateway behind HTTPS (an Ingress
with a managed certificate).

## Other clouds
| | Cluster | Registry |
|---|---|---|
| Amazon EKS | `eksctl create cluster --name ds-storage --nodes 3 --node-type t3.large` (install metrics-server and the EBS CSI driver) | ECR, or GHCR as above |
| Azure AKS | `az aks create -g rg -n ds-storage --node-count 3 --node-vm-size Standard_D4s_v5 --generate-ssh-keys` then `az aks get-credentials -g rg -n ds-storage` | ACR (`az aks update --attach-acr`), or GHCR |

## Cost control
Delete the cluster when done: `gcloud container clusters delete ds-storage --zone europe-west1-b`
(also deletes the disks of the PVCs only if you delete the namespace first:
`kubectl delete -k k8s/` then `kubectl -n ds-storage delete pvc --all`).
