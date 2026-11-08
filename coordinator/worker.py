"""Job workers: execute repair / rebalance / gc batches from the queue.

Run as a separate service (docker compose service `worker`, Kubernetes
Deployment `worker`, any number of replicas):

    uvicorn coordinator.worker:app --port 8000

Each worker process runs WORKER_CONCURRENCY PostgreSQL-queue pollers and,
when JOB_BACKEND=kafka, one Kafka consumer in the `storage-workers` group.
The coordinator can also run LOCAL_WORKERS of them in-process.
"""

import asyncio
import json
import os
import socket
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response

from coordinator import cluster, config, jobs, repair
from coordinator.db import init_db
from observability.metrics import REGISTRY, instrument_app

WORKER_ID = os.getenv("WORKER_ID") or socket.gethostname()
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "2"))

BATCHES = REGISTRY.counter("ds_worker_batches_total",
                           "Job batches executed by this worker",
                           ["type", "result", "queue"])
ITEMS = REGISTRY.counter("ds_worker_items_total", "Items (chunks/blobs) processed",
                         ["type"])
DURATION = REGISTRY.histogram("ds_worker_batch_seconds", "Batch execution time",
                              ["type"])
STATUS = {"started_at": time.time(), "batches": 0, "last_batch": None,
          "kafka": None}


async def run_batch(payload: dict, queue: str) -> None:
    job_type = payload["type"]
    handler = repair.HANDLERS[job_type]
    await asyncio.to_thread(cluster.load_membership)   # ring may have changed
    t0 = time.perf_counter()
    try:
        await handler(payload["items"], payload["job_id"])
        BATCHES.inc(type=job_type, result="ok", queue=queue)
    except Exception:
        BATCHES.inc(type=job_type, result="error", queue=queue)
        raise
    finally:
        DURATION.observe(time.perf_counter() - t0, type=job_type)
        ITEMS.inc(len(payload["items"]), type=job_type)
        STATUS["batches"] += 1
        STATUS["last_batch"] = {"type": job_type, "job_id": payload["job_id"],
                                "items": len(payload["items"]), "queue": queue,
                                "at": time.time()}


async def postgres_worker(name: str) -> None:
    """Poll the job_queue table (SKIP LOCKED + lease)."""
    while True:
        try:
            row = await asyncio.to_thread(jobs.claim, name)
        except Exception as exc:
            print(f"[{name}] queue unavailable: {exc}", flush=True)
            await asyncio.sleep(5)
            continue
        if row is None:
            await asyncio.sleep(1)
            continue
        try:
            await run_batch(row["payload"], "postgres")
            await asyncio.to_thread(jobs.ack, row["id"])
        except Exception as exc:
            print(f"[{name}] batch {row['id']} failed (attempt "
                  f"{row['attempts']}): {exc}", flush=True)
            if row["attempts"] >= 5:              # poison message: give up
                await asyncio.to_thread(jobs.ack, row["id"])
                await asyncio.to_thread(jobs.fail_job, row["job_id"], str(exc))
            # otherwise the lease expires and another worker retries it


async def kafka_worker(name: str) -> None:
    """Consume the Kafka topic as a member of the consumer group. Offsets
    are committed only after a batch has been executed (at-least-once)."""
    from aiokafka import AIOKafkaConsumer          # optional dependency
    while True:
        consumer = AIOKafkaConsumer(
            config.KAFKA_TOPIC, bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id=config.KAFKA_GROUP, client_id=name,
            enable_auto_commit=False, auto_offset_reset="earliest",
            max_poll_interval_ms=900000)
        try:
            await consumer.start()
            STATUS["kafka"] = "connected"
            print(f"[{name}] consuming {config.KAFKA_TOPIC} as part of "
                  f"group {config.KAFKA_GROUP}", flush=True)
            async for msg in consumer:
                payload = json.loads(msg.value)
                try:
                    await run_batch(payload, "kafka")
                except Exception as exc:
                    # Hand the batch to the PostgreSQL queue so it is retried.
                    print(f"[{name}] batch failed, re-queued: {exc}", flush=True)
                    await asyncio.to_thread(_requeue, payload)
                await consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            STATUS["kafka"] = f"error: {exc}"
            print(f"[{name}] Kafka unavailable ({exc}); retrying in 10 s",
                  flush=True)
            await asyncio.sleep(10)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


def _requeue(payload: dict) -> None:
    from coordinator.db import connect_db
    with connect_db() as conn:
        jobs._pg_enqueue(conn, payload["job_id"], payload["type"], payload)


def start_workers(count: int, prefix: str) -> list[asyncio.Task]:
    tasks = [asyncio.create_task(postgres_worker(f"{prefix}-pg{i}"))
             for i in range(count)]
    if count and config.JOB_BACKEND == "kafka":
        tasks.append(asyncio.create_task(kafka_worker(f"{prefix}-kafka")))
    return tasks


# ---------------------------------------------------------------------------
# Stand-alone worker service
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(init_db)
    await asyncio.to_thread(cluster.load_membership)
    tasks = start_workers(WORKER_CONCURRENCY, WORKER_ID)
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title="Storage job worker", lifespan=lifespan)
instrument_app(app, "worker")


@app.get("/health")
def health():
    return {"status": "ok", "worker": WORKER_ID, "backend": config.JOB_BACKEND,
            "concurrency": WORKER_CONCURRENCY, **STATUS}


@app.get("/metrics")
def metrics():
    return Response(REGISTRY.render(), media_type="text/plain; version=0.0.4")
