"""Scheduler entrypoint.

Moves jobs whose retry time has arrived from the delayed queue back onto the
main queue.

This is a singleton. Two schedulers polling the same delayed queue will both
see the same due job, and `DelayedQueue.poll` pushes before it removes, so the
job gets requeued twice. Run exactly one replica.
"""

import signal
import sys
import threading

import config
import metrics
import probes
from delayed_queue import DelayedQueue, DelayedQueueScheduler
from priority_queue import RedisPriorityQueue


def main() -> int:
    print(f"[scheduler] starting {config.summary()}", flush=True)

    r = config.redis_client()
    queue = RedisPriorityQueue(client=r, queue_key=config.QUEUE_KEY)
    delayed = DelayedQueue(client=r, delayed_key=config.DELAYED_KEY)

    scheduler = DelayedQueueScheduler(
        delayed, queue, poll_interval=config.SCHEDULER_POLL_INTERVAL
    )
    scheduler.start()

    def ready() -> bool:
        r.ping()
        metrics.queue_depth.set(queue.size())
        return True

    probes.serve(config.HTTP_HOST, config.HTTP_PORT, ready)
    print(
        f"[scheduler] polling every {config.SCHEDULER_POLL_INTERVAL}s, "
        f"probes on {config.HTTP_HOST}:{config.HTTP_PORT}",
        flush=True,
    )

    stopping = threading.Event()

    def shutdown(signum, _frame):
        print(f"[scheduler] signal {signum}, stopping", flush=True)
        stopping.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown)

    stopping.wait()
    print("[scheduler] stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
