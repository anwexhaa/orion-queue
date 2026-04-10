# Orion Queue

A distributed task queue built from scratch in Python and Redis — with priority scheduling, exponential backoff retries, a dead letter queue, and a REST API. No Celery. No abstractions I didn't write myself.

---

## Why I built this

Every backend system eventually needs to defer work. Send this email later. Process this image in the background. Retry this payment if it fails.

The standard answer is "just use Celery." But Celery is a black box — you configure it, you don't understand it. I wanted to understand it. So I built the same category of system from scratch: a Redis-backed distributed task queue where I made every design decision myself and can explain why.

---

## How it works

```
HTTP Client
    │
    ▼
FastAPI (/submit)
    │  creates Job with UUID, priority, payload
    ▼
RedisPriorityQueue  ◄──────────────────────────────┐
    │  Redis sorted set, score = priority           │
    │  zpopmax = atomic pop, no duplicate execution │
    ▼                                               │
Worker (one of N threads)                          │
    │  looks up task function by name              │
    │  executes task_fn(payload)                   │
    │                                              │
    ├─ success → job.status = DONE                 │
    │                                              │
    └─ failure → RetryManager                      │
                    │  exponential backoff + jitter │
                    ▼                              │
              DelayedQueue                         │
                    │  Redis sorted set            │
                    │  score = retry timestamp     │
                    ▼                              │
              Scheduler thread ──── polls every 500ms
                    │  moves due jobs back ────────┘
                    │
                    └─ max retries exceeded
                              │
                              ▼
                        DeadLetterQueue
                          Redis list, persistent
```

---

## Design decisions and why

**Redis sorted sets for the priority queue, not an in-memory heap.**

A heap lives in one process. If you want multiple worker processes — on the same machine or across machines — they can't share it. Redis sorted sets are persistent and accessible from anywhere. `ZADD` is O(log n), `ZPOPMAX` is O(log n): same complexity as a heap, but distributed. `ZPOPMAX` is also atomic, which matters — two workers can't pop the same job.

**UUIDs for job IDs, not auto-incrementing integers.**

An atomic counter requires coordination. In a distributed system where multiple processes are submitting jobs simultaneously, you'd need a single source of truth for the counter. UUIDs are generated independently and guaranteed unique across processes without any coordination.

**Task names as strings, not function references.**

You can't serialize a Python function into Redis. You store the name, workers look up the actual callable from a registry at runtime — the same pattern Celery uses internally. The registry is just a dict: `{"send_email": <function>, "resize_image": <function>}`.

**Delayed queue instead of `time.sleep()` for retries.**

The naive approach: when a job fails, sleep for N seconds, then retry. The problem: if 1000 jobs fail simultaneously, you need 1000 threads all blocked sleeping. The delayed queue approach: one scheduler thread polls a Redis sorted set (scored by retry timestamp) every 500ms and moves due jobs back to the main queue. The overhead is constant regardless of how many jobs are waiting to retry.

**Exponential backoff with jitter.**

Delay after attempt N = `0.5 * (2^N) + random(0, 0.5)`. The exponential part means a repeatedly failing job backs off progressively. The jitter (random noise) is what prevents thundering herd — if 500 jobs all fail at the same time and retry at the exact same interval, they all hammer the downstream service simultaneously. Jitter spreads them out.

**Dead letter queue in Redis, not just a log.**

Failed jobs that exhaust retries land in a Redis list. They're inspectable, persistent across restarts, and queryable via the API. A log line disappears when the process dies. A DLQ entry doesn't.

**Worker heartbeat monitoring.**

Each worker updates a `last_heartbeat` timestamp after every operation. A monitor thread checks all workers every second — if any worker hasn't updated in 10 seconds, it's assumed stuck, its current job is requeued, and it's replaced with a fresh worker. The pool size stays constant without manual intervention.

---

## Benchmarks

Run on Windows, Python 3.12, Redis 7 via Docker (localhost).

### Push/Pop latency

| Operation | 1000 ops | Avg per op |
|-----------|----------|------------|
| Push      | 0.489s   | 0.489ms    |
| Pop       | 0.530s   | 0.530ms    |

Sub-millisecond per operation. This is the Redis round trip on localhost — expect 1–5ms in a real network environment depending on Redis proximity.

### Throughput

500 jobs processed end-to-end with 4 workers: **1,710 jobs/sec**

### Priority correctness under load

200 jobs submitted across 4 priority levels simultaneously. First 50 processed were 100% CRITICAL priority. The sorted set ordering holds under concurrent load, not just in unit tests.

### Worker scaling

| Workers | Time   | Throughput   |
|---------|--------|--------------|
| 1       | 0.188s | 2,125 j/s    |
| 2       | 0.134s | 2,981 j/s    |
| 4       | 0.073s | 5,452 j/s    |
| 8       | 0.073s | 5,459 j/s    |

Throughput scales linearly from 1→4 workers, then plateaus at 8. The bottleneck shifts from workers to Redis — a single Redis instance saturates before the worker pool does. The production fix is Redis Cluster or sharding the queue across multiple Redis instances.

---

## Getting started

**Prerequisites:** Python 3.11+, Docker Desktop

```bash
git clone https://github.com/anwexhaa/orion-queue
cd orion-queue

python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

pip install -r requirements.txt

docker-compose up -d

python main.py
```

---

## API

### Submit a job

```
POST /submit
Content-Type: application/json

{
  "task_name": "add",
  "payload": {"a": 10, "b": 25},
  "priority": 10,
  "max_retries": 3
}
```

Response:
```json
{"job_id": "3b35d3dc-950e-4fd7-ac30-362f208914dc", "status": "pending"}
```

### Check job status

```
GET /status/{job_id}
```

### Queue metrics

```
GET /metrics
```

Returns current queue depth and dead letter queue size.

---

## Priority levels

| Name     | Value |
|----------|-------|
| LOW      | 1     |
| NORMAL   | 5     |
| HIGH     | 10    |
| CRITICAL | 20    |

Within the same priority level, jobs are processed FIFO — earlier submissions win via a timestamp tiebreaker in the sort score.

---

## Registering tasks

Add functions to `tasks.py`:

```python
from task_registry import TaskRegistry

@TaskRegistry.register("my_task")
def my_task(payload: dict):
    # payload is whatever dict you passed at submission time
    pass
```

Workers look up tasks by name at runtime. If a task name isn't registered, the job fails immediately and enters the retry cycle.

---

## Running tests

```bash
pytest test_queue.py -v
```

4 tests covering priority ordering, retry-to-dead flow, delayed queue requeuing, and DLQ persistence. All tests use isolated Redis keys so they don't interfere with a running instance.

---

## Project structure

```
orion-queue/
├── job.py                 # Job dataclass, status and priority enums
├── task_registry.py       # Decorator-based task registration
├── priority_queue.py      # Redis sorted set queue with priority + FIFO tiebreaker
├── delayed_queue.py       # Retry scheduling without blocking threads
├── dead_letter_queue.py   # Persistent storage for exhausted jobs
├── retry_manager.py       # Exponential backoff with jitter
├── worker.py              # Single worker thread: pop → lookup → execute
├── worker_pool.py         # N workers + heartbeat monitor + auto-replacement
├── api.py                 # FastAPI: /submit /status /metrics
├── main.py                # Entry point: boots pool, scheduler, and API
├── tasks.py               # Registered task definitions
├── benchmark.py           # Throughput, latency, priority, and scaling benchmarks
├── test_queue.py          # pytest test suite
└── docker-compose.yml     # Redis 7
```
