"""Worker entrypoint.

Runs the worker pool and nothing else. This is the deployment KEDA scales on
queue depth, which is only possible because it is separate from the API.

Exposes /health, /ready and /metrics on HTTP_PORT so Kubernetes can probe it
and Prometheus can scrape it.
"""

import signal
import sys
import threading
import time

import config
import metrics
import probes
import tasks  # noqa: F401  -- importing registers every task
from dead_letter_queue import DeadLetterQueue
from delayed_queue import DelayedQueue
from job_store import JobStore
from priority_queue import RedisPriorityQueue
from retry_manager import RetryManager
from worker_pool import WorkerPool

# How long to let an in-flight job finish after SIGTERM. Kubernetes sends
# SIGKILL after terminationGracePeriodSeconds (30 by default), so this stays
# comfortably under it.
SHUTDOWN_GRACE_SECONDS = 20


def main() -> int:
    print(f"[worker] starting {config.summary()}", flush=True)

    r = config.redis_client()
    queue = RedisPriorityQueue(client=r, queue_key=config.QUEUE_KEY)
    dlq = DeadLetterQueue(client=r, dlq_key=config.DLQ_KEY)
    delayed = DelayedQueue(client=r, delayed_key=config.DELAYED_KEY)
    retry_manager = RetryManager(delayed, dlq)
    job_store = JobStore(r)

    pool = WorkerPool(
        n_workers=config.WORKER_COUNT,
        queue=queue,
        retry_manager=retry_manager,
        job_store=job_store,
    )
    pool.start()
    metrics.workers_alive.set(len(pool.workers))

    def ready() -> bool:
        r.ping()
        metrics.workers_alive.set(len(pool.workers))
        metrics.queue_depth.set(queue.size())
        return True

    probes.serve(config.HTTP_HOST, config.HTTP_PORT, ready)
    print(
        f"[worker] {len(pool.workers)} workers running, "
        f"probes on {config.HTTP_HOST}:{config.HTTP_PORT}",
        flush=True,
    )

    stopping = threading.Event()

    def shutdown(signum, _frame):
        # KEDA scaling down, a rolling deploy, or a node drain all arrive
        # here. Stop pulling new jobs and let in-flight ones finish rather
        # than dropping them back onto the queue.
        print(f"[worker] signal {signum}, draining", flush=True)
        stopping.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown)

    stopping.wait()

    for worker in pool.workers:
        worker.stop()

    # Wait for in-flight jobs, but no longer than the grace period. Polling
    # beats a flat sleep: a pool that drains in two seconds should not hold
    # the pod open for twenty.
    deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
    while time.monotonic() < deadline:
        in_flight = sum(1 for w in pool.workers if w.current_job is not None)
        if in_flight == 0:
            break
        time.sleep(0.2)

    in_flight = sum(1 for w in pool.workers if w.current_job is not None)
    if in_flight:
        print(f"[worker] {in_flight} job(s) still running at cutoff", flush=True)
    print("[worker] stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
