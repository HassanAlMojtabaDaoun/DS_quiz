"""Background jobs: tracking table + work queue (Kafka or PostgreSQL).

A job (repair, rebalance, gc) is split into batches of items (chunks or
blobs). Every batch is a message on the queue; any number of workers take
batches, execute them and report progress into the `jobs` row:

    coordinator --enqueue--> [ Kafka topic | job_queue table ] --> workers
                                                                  |
    jobs table  <-------------- progress, counters ---------------+

Delivery is AT-LEAST-ONCE: a batch is acknowledged only after it has been
executed (Kafka offset commit / DELETE from job_queue), so a worker crash
means the batch is redelivered to another worker. Every handler is
idempotent (it re-reads the current state before acting), so running a
batch twice is harmless.

  * Kafka: topic `storage-jobs` with several partitions, consumer group
    `storage-workers`: Kafka assigns partitions to workers, which spreads
    the batches over them.
  * PostgreSQL: rows of `job_queue` are claimed with
    UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) and a lease;
    an expired lease (crashed worker) makes the row visible again. It is
    also the automatic fallback when Kafka cannot be reached.
"""

import asyncio
import json
import threading
from datetime import datetime, timezone
from uuid import UUID, uuid4

from coordinator import config
from coordinator.db import connect_db

_progress_lock = threading.Lock()
STATE = {"backend": config.JOB_BACKEND, "kafka_ok": None, "kafka_error": None,
         "published": 0, "fallbacks": 0}


# ---------------------------------------------------------------------------
# Kafka producer (optional)
# ---------------------------------------------------------------------------

class _KafkaProducer:
    def __init__(self):
        self._producer = None
        self._lock = asyncio.Lock()

    async def get(self):
        async with self._lock:
            if self._producer is None:
                from aiokafka import AIOKafkaProducer        # optional dependency
                producer = AIOKafkaProducer(
                    bootstrap_servers=config.KAFKA_BOOTSTRAP, acks="all",
                    request_timeout_ms=10000)
                await asyncio.wait_for(producer.start(), timeout=15)
                self._producer = producer
            return self._producer

    async def send(self, key: str, payload: dict):
        producer = await self.get()
        await asyncio.wait_for(
            producer.send_and_wait(config.KAFKA_TOPIC,
                                   value=json.dumps(payload).encode(),
                                   key=key.encode()), timeout=15)

    async def reset(self):
        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception:
                pass
        self._producer = None


_kafka = _KafkaProducer()


# ---------------------------------------------------------------------------
# Creating jobs
# ---------------------------------------------------------------------------

def _pg_enqueue(conn, job_id, job_type, payload):
    conn.execute(
        "INSERT INTO job_queue (job_id, type, payload) VALUES (%s, %s, %s::jsonb)",
        (job_id, job_type, json.dumps(payload)))


async def create_job(job_type: str, reason: str, items: list, meta: dict | None = None,
                     batch_size: int | None = None) -> dict:
    """Record a job and enqueue its items in batches."""
    batch_size = batch_size or config.JOB_BATCH_SIZE
    job_id = uuid4()
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    with connect_db() as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, type, reason, total, meta, status) "
            "VALUES (%s, %s, %s, %s, %s::jsonb, %s)",
            (job_id, job_type, reason, len(items),
             json.dumps({**(meta or {}), "batches": len(batches)}),
             "running" if items else "done"))

    backend = "postgres"
    if config.JOB_BACKEND == "kafka" and batches:
        try:
            for n, batch in enumerate(batches):
                # key = job:batch -> batches spread over the partitions,
                # hence over the workers of the consumer group.
                await _kafka.send(f"{job_id}:{n}", {
                    "job_id": str(job_id), "type": job_type, "batch": n,
                    "items": batch, "reason": reason})
            backend = "kafka"
            STATE.update(kafka_ok=True, kafka_error=None)
        except Exception as exc:                 # Kafka down -> fallback
            STATE.update(kafka_ok=False, kafka_error=str(exc)[:300])
            STATE["fallbacks"] += 1
            await _kafka.reset()
            backend = "postgres"
    if backend == "postgres" and batches:
        with connect_db() as conn:
            for n, batch in enumerate(batches):
                _pg_enqueue(conn, job_id, job_type, {
                    "job_id": str(job_id), "type": job_type, "batch": n,
                    "items": batch, "reason": reason})
    STATE["published"] += len(batches)
    with connect_db() as conn:
        conn.execute("UPDATE jobs SET meta = meta || %s::jsonb WHERE job_id = %s",
                     (json.dumps({"queue": backend}), job_id))
    return describe(get_job(job_id))


# ---------------------------------------------------------------------------
# PostgreSQL queue (consumer side)
# ---------------------------------------------------------------------------

def claim(worker: str) -> dict | None:
    """Take one batch: a lease of JOB_LEASE_SECONDS; SKIP LOCKED lets many
    workers poll the same table without blocking each other."""
    with connect_db() as conn:
        row = conn.execute(
            """
            UPDATE job_queue
            SET locked_by = %s,
                locked_until = NOW() + make_interval(secs => %s),
                attempts = attempts + 1
            WHERE id = (
                SELECT id FROM job_queue
                WHERE locked_until IS NULL OR locked_until < NOW()
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1)
            RETURNING id, job_id, type, payload, attempts
            """, (worker, config.JOB_LEASE_SECONDS)).fetchone()
    return row


def ack(queue_id: int) -> None:
    with connect_db() as conn:
        conn.execute("DELETE FROM job_queue WHERE id = %s", (queue_id,))


def queue_length() -> int:
    with connect_db() as conn:
        return conn.execute("SELECT COUNT(*) AS n FROM job_queue").fetchone()["n"]


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------

def _add(acc, delta):
    """Recursively add numeric counters (dicts of dicts of numbers)."""
    for k, v in delta.items():
        if isinstance(v, dict):
            _add(acc.setdefault(k, {}), v)
        elif isinstance(v, (int, float)):
            acc[k] = acc.get(k, 0) + v
    return acc


def record_progress(job_id, items_done: int, counters: dict,
                    lost: list | None = None) -> None:
    """Add a finished batch's results to its job (row-locked update)."""
    with _progress_lock, connect_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = %s FOR UPDATE",
                           (job_id,)).fetchone()
        if row is None:
            return
        merged = _add(dict(row["counters"] or {}), counters)
        done = min(row["total"], row["done"] + items_done)
        all_lost = (list(row["lost"] or []) + list(lost or []))[:50]
        finished = done >= row["total"]
        conn.execute(
            "UPDATE jobs SET done = %s, counters = %s::jsonb, lost = %s::jsonb, "
            "status = %s, finished_at = CASE WHEN %s THEN NOW() ELSE NULL END "
            "WHERE job_id = %s",
            (done, json.dumps(merged), json.dumps(all_lost),
             "done" if finished else "running", finished, job_id))


def fail_job(job_id, reason: str) -> None:
    with connect_db() as conn:
        conn.execute("UPDATE jobs SET status = 'failed', finished_at = NOW(), "
                     "meta = meta || %s::jsonb WHERE job_id = %s",
                     (json.dumps({"error": reason}), job_id))


def expire_stale_jobs() -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE jobs SET status = 'expired', finished_at = NOW() "
            "WHERE status = 'running' AND created_at < NOW() - make_interval(secs => %s)",
            (config.JOB_TIMEOUT,))


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_job(job_id) -> dict | None:
    if isinstance(job_id, str):
        job_id = UUID(job_id)
    with connect_db() as conn:
        return conn.execute("SELECT * FROM jobs WHERE job_id = %s",
                            (job_id,)).fetchone()


def active_job(job_type: str) -> dict | None:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM jobs WHERE type = %s AND status = 'running' "
            "ORDER BY created_at DESC LIMIT 1", (job_type,)).fetchone()


def recent_jobs(job_type: str | None = None, limit: int = 20) -> list[dict]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM jobs WHERE (%s::text IS NULL OR type = %s) "
            "ORDER BY created_at DESC LIMIT %s",
            (job_type, job_type, limit)).fetchall()


def describe(row: dict | None) -> dict | None:
    if row is None:
        return None
    finished = row["finished_at"]
    created = row["created_at"]
    end = finished or datetime.now(timezone.utc)
    meta = row["meta"] or {}
    return {
        "job_id": str(row["job_id"]),
        "type": row["type"],
        "reason": row["reason"],
        "status": row["status"],
        "queue": meta.get("queue"),
        "failed_nodes": meta.get("failed_nodes", []),
        "started_at": created.isoformat(),
        "finished_at": finished.isoformat() if finished else None,
        "duration_seconds": round((end - created).total_seconds(), 2),
        "chunks_to_check": row["total"],
        "chunks_done": row["done"],
        "batches": meta.get("batches"),
        "by_scheme": row["counters"] if row["type"] == "repair" else None,
        "counters": row["counters"],
        "lost": row["lost"],
        "meta": meta,
    }
