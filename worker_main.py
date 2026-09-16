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
from task_registry import TaskRegistry
from worker_pool import WorkerPool

# How long to let an in-flight job finish after SIGTERM. Kubernetes sends
# SIGKILL after terminationGracePeriodSeconds (30 by default), so this stays
# comfortably under it.
SHUTDOWN_GRACE_SECONDS = 20


def main() -> int:
    print(f"[worker] starting {config.summary()}", flush=True)

    r = config.redis_client()
    metrics.init_job_series(TaskRegistry._registry)
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

    # One round trip, on a client with one-second timeouts. Game day 3 found
    # the original version making two calls on the main five-second client -
    # a ping and a queue-depth read - so 500ms of Redis latency took each
    # probe to about a second, past the kubelet's 1s timeout, and every worker
    # and the scheduler went NotReady together. Harmless for workers, which
    # have no Service; fatal if the same pattern were on the API.
    #
    # Queue depth is not read here at all. Readiness must be cheap and must
    # answer one question, and the API already reports depth on /metrics.
    probe_r = config.redis_client(socket_timeout=1, socket_connect_timeout=1)

    def ready() -> bool:
        probe_r.ping()
        metrics.workers_alive.set(len(pool.workers))
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
