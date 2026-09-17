"""Orion Queue benchmark.

    python benchmark.py              # 5 repeats, median reported
    python benchmark.py --repeat 9

Every figure is the median of several repeats, with the min-max range beside
it. An earlier version ran each measurement once over a few hundred jobs that
finished in about a tenth of a second, where thread start-up and scheduler
noise dominated: the same machine reported anywhere from 1,168 to 2,256
jobs/sec for a single worker on consecutive runs. Numbers that move that much
between runs cannot support a claim, so this version runs long enough to
measure the queue rather than the timer.
"""

import argparse
import statistics
import threading
import time

import config
from dead_letter_queue import DeadLetterQueue
from delayed_queue import DelayedQueue
from job import Job, JobPriority
from priority_queue import RedisPriorityQueue
from retry_manager import RetryManager
from task_registry import TaskRegistry
from worker_pool import WorkerPool

PREFIX = "bench"

r = config.redis_client()
queue = RedisPriorityQueue(client=r, queue_key=f"{PREFIX}_queue")
dlq = DeadLetterQueue(client=r, dlq_key=f"{PREFIX}_dlq")
dq = DelayedQueue(client=r, delayed_key=f"{PREFIX}_delayed")
rm = RetryManager(dq, dlq)

completed = 0
lock = threading.Lock()


@TaskRegistry.register("bench_task")
def bench_task(payload: dict):
    global completed
    with lock:
        completed += 1


def reset():
    """Delete every benchmark key, including any the queue derives itself."""
    keys = list(r.scan_iter(match=f"{PREFIX}_*"))
    if keys:
        r.delete(*keys)


def stop(pool, n):
    pool.scale_down(n)
    # Let stopped workers leave their loop, so a job one of them was still
    # holding cannot be counted towards the next measurement.
    time.sleep(0.3)


def summarise(values, unit, fmt="{:.3f}"):
    med = statistics.median(values)
    lo, hi = min(values), max(values)
    return f"{fmt.format(med)} {unit}  (range {fmt.format(lo)}-{fmt.format(hi)})"


# -- latency -------------------------------------------------------------------
def bench_latency(n):
    reset()
    jobs = [Job(task_name="bench_task", payload={"i": i}) for i in range(n)]

    start = time.perf_counter()
    for j in jobs:
        queue.push(j)
    push_ms = (time.perf_counter() - start) / n * 1000

    popped = []
    start = time.perf_counter()
    for _ in range(n):
        popped.append(queue.pop())
    pop_ms = (time.perf_counter() - start) / n * 1000

    # A worker acknowledges every job it finishes, so the full cost of
    # taking one job is pop + ack. Measured separately so the two can be
    # compared with the old destructive pop, which had no ack.
    ack_ms = None
    if hasattr(queue, "ack"):
        start = time.perf_counter()
        for job in popped:
            queue.ack(job)
        ack_ms = (time.perf_counter() - start) / n * 1000

    # What a worker actually does per job: acknowledge the previous one and
    # lease the next, in one round trip.
    take_ms = None
    if hasattr(queue, "take"):
        reset()
        for j in jobs:
            queue.push(j)
        previous = None
        start = time.perf_counter()
        for _ in range(n):
            _, previous = queue.take(ack=previous)
        take_ms = (time.perf_counter() - start) / n * 1000
        queue.take(ack=previous)

    reset()
    return push_ms, pop_ms, ack_ms, take_ms


# -- throughput ----------------------------------------------------------------
def bench_drain(n_jobs, n_workers):
    """Jobs/sec for a pool draining a queue that is already full."""
    global completed
    reset()
    for i in range(n_jobs):
        queue.push(Job(task_name="bench_task", payload={"i": i}))

    completed = 0
    pool = WorkerPool(n_workers=n_workers, queue=queue, retry_manager=rm)
    start = time.perf_counter()
    pool.start()
    while completed < n_jobs:
        time.sleep(0.005)
    elapsed = time.perf_counter() - start
    stop(pool, n_workers)
    reset()
    return n_jobs / elapsed


# -- priority ------------------------------------------------------------------
def bench_priority(n_per_priority):
    reset()
    order = []
    order_lock = threading.Lock()

    @TaskRegistry.register("priority_bench")
    def priority_bench(payload):
        with order_lock:
            order.append(payload["priority"])

    priorities = [p.value for p in (
        JobPriority.LOW, JobPriority.NORMAL, JobPriority.HIGH, JobPriority.CRITICAL
    )]
    for p in priorities:
        for _ in range(n_per_priority):
            queue.push(Job(task_name="priority_bench", payload={"priority": p}, priority=p))

    pool = WorkerPool(n_workers=1, queue=queue, retry_manager=rm)
    pool.start()
    total = n_per_priority * len(priorities)
    while len(order) < total:
        time.sleep(0.01)
    stop(pool, 1)
    reset()

    # With one worker, execution order must be exactly descending priority.
    expected = sorted(order, reverse=True)
    return order == expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--latency-ops", type=int, default=5000)
    parser.add_argument("--drain-jobs", type=int, default=4000)
    args = parser.parse_args()

    print("Orion Queue benchmark")
    print(f"  repeats={args.repeat}  latency_ops={args.latency_ops}  "
          f"drain_jobs={args.drain_jobs}")

    push, pop, ack, take = [], [], [], []
    for _ in range(args.repeat):
        p, q, a, t = bench_latency(args.latency_ops)
        push.append(p)
        pop.append(q)
        if a is not None:
            ack.append(a)
        if t is not None:
            take.append(t)

    print("\nLatency per operation")
    print(f"  push   {summarise(push, 'ms')}")
    print(f"  pop    {summarise(pop, 'ms')}")
    if ack:
        print(f"  ack    {summarise(ack, 'ms')}")
        print(f"  pop+ack {summarise([a + b for a, b in zip(pop, ack)], 'ms')}  (two calls)")
    if take:
        print(f"  take   {summarise(take, 'ms')}  (ack + pop in one call - what a worker does)")

    print("\nDrain throughput")
    for workers in (1, 2, 4, 8):
        rates = [bench_drain(args.drain_jobs, workers) for _ in range(args.repeat)]
        print(f"  {workers} worker(s)  {summarise(rates, 'jobs/sec', '{:,.0f}')}")

    ok = all(bench_priority(50) for _ in range(args.repeat))
    print(f"\nPriority ordering exact on every repeat: {ok}")


if __name__ == "__main__":
    main()
