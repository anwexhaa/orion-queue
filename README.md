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

docker compose up -d redis

python main.py
```

Or run the whole stack in containers, shaped the way it is deployed — API,
worker and scheduler as separate processes from one image:

```bash
docker compose up --build
```

| Service | URL |
|---------|-----|
| API | http://localhost:8000 |
| Worker probes and metrics | http://localhost:8001 |
| Scheduler probes and metrics | http://localhost:8002 |

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

### Metrics

```
GET /metrics
```

Prometheus text exposition format. Exposes queue depth, dead letter depth,
jobs submitted and processed by terminal status, retry counts, job duration,
and dispatch latency.

`orion_dispatch_latency_seconds` — the time between a job being submitted and
a worker starting it — has an explicit bucket boundary at 0.3s, because the
latency objective is "95% of jobs dispatched within 300 ms" and a histogram
can only answer that exactly if the threshold is a bucket edge.

### Queue stats

```
GET /stats
```

```json
{"queue_size": 12, "dead_jobs": 0}
```

The original JSON payload, kept because KEDA's `metrics-api` scaler reads JSON
rather than the Prometheus format.

### Health and readiness

```
GET /health    liveness  — always 200 while the process is responsive
GET /ready     readiness — 200 when Redis is reachable, 503 when it is not
```

These are deliberately different checks. Liveness does not touch Redis: a
liveness probe that fails during a Redis outage would restart every pod at
once, turning a recoverable dependency problem into a full outage. Readiness
does touch Redis, because a process that cannot reach it cannot do any work
and should be taken out of the load balancer until it can.

All three processes — API, worker and scheduler — serve these paths.

---

## Configuration

Everything is read from the environment. Every default is the value that used
to be hardcoded, so nothing needs setting to run locally.

| Variable | Default | Notes |
|----------|---------|-------|
| `REDIS_HOST` | `localhost` | Must be set in Kubernetes, where `localhost` is the pod itself |
| `REDIS_PORT` | `6379` | |
| `REDIS_DB` | `0` | |
| `REDIS_PASSWORD` | unset | |
| `QUEUE_KEY` | `task_queue` | |
| `DLQ_KEY` | `dead_letter_queue` | |
| `DELAYED_KEY` | `delayed_queue` | |
| `WORKER_COUNT` | `4` | Threads per worker process |
| `SCHEDULER_POLL_INTERVAL` | `0.5` | Seconds |
| `HTTP_HOST` | `0.0.0.0` | |
| `HTTP_PORT` | `8000` | |
| `ORION_ROLE` | `all-in-one` | Label for logs and metrics |

---

## Process topology

One image, three entrypoints:

| Entrypoint | Role | Replicas |
|------------|------|----------|
| `api_main.py` | HTTP only | Scale on request volume |
| `worker_main.py` | Worker pool only | Scale on queue depth |
| `scheduler_main.py` | Delayed-queue scheduler | **Exactly one** |

The scheduler is a singleton. `DelayedQueue.poll` pushes a due job onto the
main queue before removing it from the delayed set, so two schedulers polling
together will requeue the same job twice.

Splitting the API from the workers is what makes queue-depth autoscaling
possible at all — while they share a process, scaling for backlog also
multiplies HTTP replicas that were never the bottleneck.

`main.py` still runs all three in one process for local development.

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
├── api.py                 # FastAPI: /submit /status /metrics /stats /health /ready
├── config.py              # Environment-driven configuration and the Redis client
├── metrics.py             # Prometheus collectors, including the SLI histograms
├── probes.py              # Stdlib HTTP server giving the worker and scheduler probes
├── tasks.py               # Registered task definitions
│
├── main.py                # Local development: all three roles in one process
├── api_main.py            # Container entrypoint: API only
├── worker_main.py         # Container entrypoint: worker pool only
├── scheduler_main.py      # Container entrypoint: scheduler only, single replica
│
├── benchmark.py           # Throughput, latency, priority, and scaling benchmarks
├── test_queue.py          # pytest test suite
├── test_pool.py           # Manual worker pool smoke check, not a pytest test
├── Dockerfile             # Multi-stage, non-root, one image for all three roles
└── docker-compose.yml     # Redis plus the three processes
```
