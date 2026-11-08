"""Coordinator configuration (all values come from environment variables)."""

import os


def parse_nodes(raw: str) -> dict[str, str]:
    """Parse NODE_URLS into {node_id: url}.

    Accepted formats (comma separated):
      node1=http://storage-node-1:8000   (explicit id)
      http://storage-node-1:8000         (id defaults to node<position>)
    """
    nodes: dict[str, str] = {}
    entries = [e.strip() for e in raw.split(",") if e.strip()]
    for position, entry in enumerate(entries, start=1):
        if "=" in entry:
            node_id, url = entry.split("=", 1)
        else:
            node_id, url = f"node{position}", entry
        nodes[node_id.strip()] = url.strip().rstrip("/")
    return nodes


DATABASE_URL = os.environ.get("DATABASE_URL", "")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "dev-cluster-token-change-me")
SEED_NODES = parse_nodes(os.getenv("NODE_URLS", ""))
VNODES = int(os.getenv("VNODES", "128"))

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(1024 * 1024 * 1024)))
# Objects assembled from a multipart upload may be larger than one request.
MAX_OBJECT_SIZE = int(os.getenv("MAX_OBJECT_SIZE", str(10 * 1024 ** 3)))
# Created at start-up as an admin key if set (development bootstrap).
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
DEFAULT_CHUNKING = os.getenv("DEFAULT_CHUNKING", "fixed")

# --- Replication with quorums -------------------------------------------
# N copies of each chunk; a write succeeds once W copies are acknowledged,
# a read needs R replicas to confirm the chunk. W + R > N makes every read
# quorum overlap every write quorum.
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "2"))
READ_QUORUM = int(os.getenv("READ_QUORUM", "2"))

# --- Erasure coding defaults (per bucket, overridable) ---------------------
EC_K = int(os.getenv("EC_K", "4"))
EC_M = int(os.getenv("EC_M", "2"))
DEFAULT_REDUNDANCY = os.getenv("DEFAULT_REDUNDANCY", "replication")

# --- Failure detection and repair ------------------------------------------
# A node is failed after this many seconds without a heartbeat
# (default = 3 missed heartbeats at a 5 s interval).
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "15"))
REPAIR_INTERVAL = float(os.getenv("REPAIR_INTERVAL", "10"))
REPAIR_ENABLED = os.getenv("REPAIR_ENABLED", "true").lower() == "true"
REPAIR_CONCURRENCY = int(os.getenv("REPAIR_CONCURRENCY", "8"))


# --- Background jobs (repair, rebalance, garbage collection) ---------------
# Jobs are split into batches and put on a queue; workers (separate
# containers, or LOCAL_WORKERS tasks inside the coordinator) execute them.
JOB_BACKEND = os.getenv("JOB_BACKEND", "postgres")        # postgres | kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "storage-jobs")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "storage-workers")
LOCAL_WORKERS = int(os.getenv("LOCAL_WORKERS", "2"))
JOB_BATCH_SIZE = int(os.getenv("JOB_BATCH_SIZE", "50"))
JOB_TIMEOUT = float(os.getenv("JOB_TIMEOUT", "900"))
JOB_LEASE_SECONDS = int(os.getenv("JOB_LEASE_SECONDS", "300"))

# --- Elastic membership -------------------------------------------------------
# A node that starts sending heartbeats joins the ring automatically and a
# rebalance job is scheduled REBALANCE_DELAY seconds later.
AUTO_JOIN = os.getenv("AUTO_JOIN", "false").lower() == "true"
REBALANCE_DELAY = float(os.getenv("REBALANCE_DELAY", "15"))


def validate() -> list[str]:
    """Raise on impossible settings, return warnings for weak ones."""
    warnings = []
    if CHUNK_SIZE <= 0 or MAX_FILE_SIZE < 0:
        raise RuntimeError("Invalid size configuration")
    if REPLICATION_FACTOR < 1:
        raise RuntimeError("Invalid replication factor")
    if len(SEED_NODES) < REPLICATION_FACTOR and not AUTO_JOIN:
        raise RuntimeError("Fewer seed nodes than the replication factor "
                           "(set NODE_URLS or AUTO_JOIN=true)")
    if not (1 <= WRITE_QUORUM <= REPLICATION_FACTOR):
        raise RuntimeError("WRITE_QUORUM must be between 1 and N")
    if not (1 <= READ_QUORUM <= REPLICATION_FACTOR):
        raise RuntimeError("READ_QUORUM must be between 1 and N")
    if WRITE_QUORUM + READ_QUORUM <= REPLICATION_FACTOR:
        warnings.append(
            f"W + R = {WRITE_QUORUM + READ_QUORUM} <= N = {REPLICATION_FACTOR}: "
            "read and write quorums may not overlap")
    if EC_K < 1 or EC_M < 1 or EC_K + EC_M > 255:
        raise RuntimeError("Invalid EC_K / EC_M")
    return warnings
