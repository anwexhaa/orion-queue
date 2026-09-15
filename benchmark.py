import config
import time
import threading
from job import Job, JobPriority
from priority_queue import RedisPriorityQueue
from delayed_queue import DelayedQueue, DelayedQueueScheduler
from dead_letter_queue import DeadLetterQueue
from retry_manager import RetryManager
from task_registry import TaskRegistry
from worker_pool import WorkerPool

# ── setup ──────────────────────────────────────────────────────────────────
r = config.redis_client()
r.delete("bench_queue")
r.delete("bench_dlq")
r.delete("bench_delayed")

queue  = RedisPriorityQueue(client=r, queue_key="bench_queue")
dlq    = DeadLetterQueue(client=r, dlq_key="bench_dlq")
dq     = DelayedQueue(client=r, delayed_key="bench_delayed")
rm     = RetryManager(dq, dlq)

completed = 0
lock = threading.Lock()

@TaskRegistry.register("bench_task")
def bench_task(payload: dict):
    global completed
    with lock:
        completed += 1

# ── benchmark 1: throughput ────────────────────────────────────────────────
def bench_throughput(n_jobs=500, n_workers=4):
    global completed
    completed = 0
    r.delete("bench_queue")

    pool = WorkerPool(n_workers=n_workers, queue=queue, retry_manager=rm)
    pool.start()

    start = time.time()
    for i in range(n_jobs):
        queue.push(Job(task_name="bench_task", payload={"i": i}))

    while completed < n_jobs:
        time.sleep(0.01)

    elapsed = time.time() - start
    pool.scale_down(n_workers)

    print(f"\n── Throughput ──────────────────────────────")
    print(f"  Jobs:     {n_jobs}")
    print(f"  Workers:  {n_workers}")
    print(f"  Time:     {elapsed:.3f}s")
    print(f"  Rate:     {n_jobs / elapsed:.1f} jobs/sec")

# ── benchmark 2: priority correctness under load ───────────────────────────
def bench_priority(n_per_priority=50):
    r.delete("bench_queue")
    order = []
    lock2 = threading.Lock()

    @TaskRegistry.register("priority_bench")
    def priority_bench(payload):
        with lock2:
            order.append(payload["priority"])

    priorities = [
        JobPriority.LOW.value,
        JobPriority.NORMAL.value,
        JobPriority.HIGH.value,
        JobPriority.CRITICAL.value,
    ]

    for p in priorities:
        for _ in range(n_per_priority):
            queue.push(Job(task_name="priority_bench", payload={"priority": p}, priority=p))

    pool = WorkerPool(n_workers=1, queue=queue, retry_manager=rm)
    pool.start()

    total = n_per_priority * len(priorities)
    while len(order) < total:
        time.sleep(0.01)

    pool.scale_down(1)

    # check first quarter is all CRITICAL
    first_quarter = order[:n_per_priority]
    correct = all(p == JobPriority.CRITICAL.value for p in first_quarter)

    print(f"\n── Priority Ordering ───────────────────────")
    print(f"  Jobs per priority: {n_per_priority}")
    print(f"  First {n_per_priority} processed all CRITICAL: {correct}")
    print(f"  Execution order (first 20): {order[:20]}")

# ── benchmark 3: push/pop latency ─────────────────────────────────────────
def bench_latency(n=1000):
    r.delete("bench_queue")

    # push latency
    jobs = [Job(task_name="bench_task", payload={"i": i}) for i in range(n)]
    start = time.time()
    for j in jobs:
        queue.push(j)
    push_time = time.time() - start

    # pop latency
    start = time.time()
    for _ in range(n):
        queue.pop()
    pop_time = time.time() - start

    print(f"\n── Latency ─────────────────────────────────")
    print(f"  {n} pushes: {push_time:.3f}s  ({n/push_time:.0f} ops/sec)")
    print(f"  {n} pops:   {pop_time:.3f}s  ({n/pop_time:.0f} ops/sec)")
    print(f"  Avg push:  {push_time/n*1000:.3f}ms")
    print(f"  Avg pop:   {pop_time/n*1000:.3f}ms")

# ── benchmark 4: worker scaling ────────────────────────────────────────────
def bench_scaling(n_jobs=400):
    print(f"\n── Worker Scaling ──────────────────────────")
    for n_workers in [1, 2, 4, 8]:
        global completed
        completed = 0
        r.delete("bench_queue")

        for i in range(n_jobs):
            queue.push(Job(task_name="bench_task", payload={"i": i}))

        pool = WorkerPool(n_workers=n_workers, queue=queue, retry_manager=rm)
        pool.start()
        start = time.time()

        while completed < n_jobs:
            time.sleep(0.01)

        elapsed = time.time() - start
        pool.scale_down(n_workers)
        print(f"  {n_workers} workers: {elapsed:.3f}s  ({n_jobs/elapsed:.1f} jobs/sec)")

# ── run all ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Orion Queue — Benchmark")
    print("=" * 45)
    bench_latency(1000)
    bench_throughput(500, 4)
    bench_priority(50)
    bench_scaling(400)
    print("\n" + "=" * 45)
    print("Done.")