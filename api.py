import json

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

import config
import metrics
from dead_letter_queue import DeadLetterQueue
from job import Job, JobPriority
from job_store import JobStore
from priority_queue import RedisPriorityQueue

app = FastAPI(title="Orion Queue", version="1.0.0")

r = config.redis_client()
queue = RedisPriorityQueue(client=r, queue_key=config.QUEUE_KEY)
dlq = DeadLetterQueue(client=r, dlq_key=config.DLQ_KEY)
job_store = JobStore(r)


class SubmitRequest(BaseModel):
    task_name: str
    payload: dict
    priority: int = JobPriority.NORMAL.value
    max_retries: int = 3


@app.post("/submit")
def submit_job(req: SubmitRequest):
    job = Job(
        task_name=req.task_name,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries,
    )
    # write status key BEFORE pushing to queue to avoid race condition
    job_store.save(job)
    queue.push(job)
    metrics.jobs_submitted.labels(task_name=job.task_name).inc()
    return {"job_id": job.id, "status": job.status.value}


@app.get("/status/{job_id}")
def get_status(job_id: str):
    record = job_store.get(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    return record


# --- Probes ----------------------------------------------------------------
#
# Liveness and readiness are deliberately different checks.
#
# /health answers "is this process wedged". It must not touch Redis: a
# liveness probe that fails during a Redis outage restarts every API pod
# simultaneously, converting a dependency problem into a full outage.
#
# /ready answers "can this process serve traffic right now". It does touch
# Redis, because an API that cannot reach Redis cannot accept a job, and
# Kubernetes should take it out of the Service until it can.


@app.get("/health")
def health():
    return {"status": "ok", "role": config.ROLE}


@app.get("/ready")
def ready(response: Response):
    try:
        r.ping()
    except Exception as exc:
        metrics.redis_up.set(0)
        response.status_code = 503
        return {"status": "not ready", "reason": f"redis unreachable: {exc}"}

    metrics.redis_up.set(1)
    return {"status": "ready"}


# --- Metrics ---------------------------------------------------------------


def _refresh_depth_gauges() -> tuple[int, int]:
    """Read the two queue depths, updating the gauges as a side effect.

    Both are O(1) in Redis (ZCARD and LLEN), so doing this per scrape is
    cheaper than maintaining a running count that can drift.
    """
    try:
        depth = queue.size()
        dead = dlq.size()
    except Exception:
        metrics.redis_up.set(0)
        raise

    metrics.redis_up.set(1)
    metrics.queue_depth.set(depth)
    metrics.dead_letter_depth.set(dead)
    return depth, dead


@app.get("/metrics")
def prometheus_metrics():
    """Prometheus text exposition format. Scraped by kube-prometheus-stack."""
    try:
        _refresh_depth_gauges()
    except Exception:
        # Serve whatever the counters already hold rather than failing the
        # scrape outright; orion_redis_up going to 0 is the signal that
        # matters, and losing it here would hide the outage.
        pass

    body, content_type = metrics.render()
    return Response(content=body, media_type=content_type)


@app.get("/stats")
def stats():
    """The original JSON metrics payload.

    Kept because KEDA's metrics-api scaler reads JSON, not the Prometheus
    exposition format. See decision D6 in the Helios repo: the queue is a
    Redis sorted set, so KEDA's redis scaler cannot measure it.
    """
    depth, dead = _refresh_depth_gauges()
    return {"queue_size": depth, "dead_jobs": dead}
