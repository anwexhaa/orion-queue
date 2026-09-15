"""Local development entrypoint: API, workers and scheduler in one process.

This is not what runs in Kubernetes. There the three are separate deployments
— api_main.py, worker_main.py and scheduler_main.py — so that worker count can
follow queue depth without also multiplying API replicas.

Kept because it is still the fastest way to run the whole system on a laptop:

    docker compose up -d redis
    python main.py
"""

import uvicorn

import config
import tasks  # noqa: F401  -- importing registers every task
from dead_letter_queue import DeadLetterQueue
from delayed_queue import DelayedQueue, DelayedQueueScheduler
from job_store import JobStore
from priority_queue import RedisPriorityQueue
from retry_manager import RetryManager
from worker_pool import WorkerPool

r = config.redis_client()
queue = RedisPriorityQueue(client=r, queue_key=config.QUEUE_KEY)
dlq = DeadLetterQueue(client=r, dlq_key=config.DLQ_KEY)
dq = DelayedQueue(client=r, delayed_key=config.DELAYED_KEY)
rm = RetryManager(dq, dlq)
job_store = JobStore(r)

scheduler = DelayedQueueScheduler(dq, queue, poll_interval=config.SCHEDULER_POLL_INTERVAL)
scheduler.start()

pool = WorkerPool(
    n_workers=config.WORKER_COUNT, queue=queue, retry_manager=rm, job_store=job_store
)
pool.start()

print(f"[all-in-one] {config.summary()}", flush=True)
print(f"[all-in-one] {len(pool.workers)} workers started, scheduler running", flush=True)

uvicorn.run("api:app", host=config.HTTP_HOST, port=config.HTTP_PORT, reload=False)
