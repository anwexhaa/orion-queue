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
    │  pop = atomic ZPOPMAX + lease, one script     │
    ▼                                               │
Processing set (lease per job) ── reaper requeues ──┤
    │  renewed while the job runs                   │  expired leases
    ▼                                               │
Worker (one of N threads)                          │
    │  looks up task function by name              │
    │  executes task_fn(payload)                   │
    │  acks on the next pop, in the same round trip│
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

**Two layers of recovery: heartbeats inside a process, leases across processes.**

Each worker updates a `last_heartbeat` timestamp after every operation. A monitor thread checks all workers every second — if one hasn't updated in 10 seconds, it's assumed wedged, its current job is requeued, and it's replaced with a fresh thread.

That only works while the process is alive. A chaos experiment that killed a worker pod mid-job lost four jobs: `ZPOPMAX` had already removed them from Redis, and the monitor that would have requeued them died with the pod. Nothing alerted, because a job that never finishes never increments any counter.

So a pop no longer deletes. One Lua script pops the job and records a **lease** — a deadline in a processing set — atomically. The pool renews its workers' leases every third of the lease period while jobs run. If the process dies, renewals stop, the lease expires, and a reaper in the scheduler requeues the job with its original priority. A crashed worker's jobs come back within `LEASE_SECONDS + REAP_INTERVAL`, and every expiry is counted in `orion_leases_expired_total`, so a dying worker is visible.

**At-least-once delivery, not exactly-once.**

A worker that finishes a job just after its lease expired, or dies between finishing and acknowledging, means the job runs twice. That is the price of never losing one, and it is the same trade SQS visibility timeouts and Sidekiq's reliable fetch make. Tasks must be idempotent. Late acknowledgements are counted in `orion_lease_ack_late_total`; a rising rate means the lease is too short for the real tasks.

Three details keep the duplicates rare. A worker acknowledges a job only once its outcome is durably recorded — if recording a failure itself fails, it keeps the lease, so the job is retried rather than dropped. Requeueing a stalled job is conditional on still holding its lease, so the monitor and the reaper can never both put it back. And a graceful stop flushes the last acknowledgement before the process exits.

**The acknowledgement rides on the next pop.**

Leasing alone doubled the Redis round trips per job — pop, then ack — and roughly halved throughput on the benchmark, where the task does nothing and round trips are the whole cost. `take()` acknowledges the previous job and leases the next in one script, which brought throughput back to the pre-lease level. See the benchmark below.

---

## Benchmarks

```bash
python benchmark.py            # median of 5 repeats
python benchmark.py --repeat 9
```

Windows 11, Python 3.12, Redis 7 in Docker Desktop on localhost. Every figure is the **median of 5 repeats**; the range across repeats is shown because it is wide, and a single run on this setup can land anywhere inside it.

An earlier version of this benchmark timed a few hundred jobs finishing in about a tenth of a second, where thread start-up and scheduler noise dominated — the same machine reported 1,168 to 2,256 jobs/sec for one worker on consecutive runs. It now runs long enough to measure the queue rather than the timer, and the older figures that used to be quoted here could not be reproduced by it.

### Latency per operation

| Operation | Median | Range |
|-----------|--------|-------|
| push | 0.42 ms | 0.40–0.69 |
| take — ack the previous job and lease the next | **0.51 ms** | 0.50–0.52 |

For comparison, the old destructive pop was 0.42 ms, and leased pop followed by a separate ack was 0.90 ms. This is the localhost round trip; expect 1–5 ms against Redis on a real network.

### Drain throughput

4,000 no-op jobs already queued, drained by a pool of N worker threads.

| Workers | Before leases | Leases, pop + ack | **Leases, take()** |
|---------|---------------|-------------------|--------------------|
| 1 | 1,594 /s | 1,030 /s | **1,828 /s** |
| 2 | 1,944 /s | 1,584 /s | **2,672 /s** |
| 4 | 4,160 /s | 2,164 /s | **4,386 /s** |
| 8 | 4,423 /s | 2,336 /s | **4,543 /s** |

With `take()`, leasing has no measurable throughput cost — the differences from the pre-lease column are inside run-to-run noise. Throughput rises with workers up to 4 and then flattens: past that point a single Redis instance, not the pool, is the constraint.

Because the task does nothing, these numbers are an upper bound on queue overhead, not a prediction of real throughput. With real work — even the 2-second `slow_task` — the task dominates and the queue's cost is noise.

### Priority ordering

200 jobs across four priorities, drained by one worker: execution order was exactly descending priority on every repeat.

### Crash recovery

`scripts/crash-test.sh` reproduces the chaos experiment without a cluster. It runs the API, scheduler and a 4-thread worker on a private Docker network, submits 40 two-second jobs, kills the worker while it holds a full batch, starts a replacement, and then reads every job record.

| Code | Signal | Runs | Done | Lost | Leases reaped |
|------|--------|------|------|------|---------------|
| before leases | SIGKILL | 4 | 36 of 40 | **4** | — |
| leases, pop + ack | SIGKILL | 4 | 40 of 40 | **0** | 4, every run |
| leases, take() | SIGKILL | 3 | 40 of 40 | **0** | 4, every run |
| leases, take() | SIGTERM | 1 | 40 of 40 | **0** | 0 — drained and acknowledged its own jobs |

The run against the old code matters as much as the new one: had it lost nothing, the harness would not have been killing mid-job, and a clean result on the new code would prove nothing.

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
{"queue_size": 12, "dead_jobs": 0, "in_flight": 4}
```

`in_flight` is the number of jobs currently leased to a worker.

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
| `LEASE_SECONDS` | `30` | How long a job is leased to the worker that took it. Renewed every third of this while it runs |
| `REAP_INTERVAL` | `5` | Seconds between checks for expired leases. Worst-case recovery is `LEASE_SECONDS + REAP_INTERVAL` |
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

27 tests. Beyond priority ordering, retries and the dead letter queue, they cover leased fetch — a job held by a dead worker is recovered, keeps its priority, and is not duplicated when a stalled worker is also requeued — the worker's acknowledgement rules, the heartbeat monitor, and the SLI series being exposed before the first error. They need a local Redis (`docker compose up -d redis`) and use isolated keys, so they don't interfere with a running instance.

For the crash behaviour end to end, against a built image:

```bash
docker build -t orion-queue:local .
./scripts/crash-test.sh orion-queue:local
```

---

## Project structure

```
orion-queue/
├── job.py                 # Job dataclass, status and priority enums
├── task_registry.py       # Decorator-based task registration
├── priority_queue.py      # Priority queue with leased fetch, ack, renew and reap
├── lease_reaper.py        # Requeues jobs whose worker stopped renewing its lease
├── delayed_queue.py       # Retry scheduling without blocking threads
├── dead_letter_queue.py   # Persistent storage for exhausted jobs
├── retry_manager.py       # Exponential backoff with jitter
├── worker.py              # Single worker thread: take → execute → ack on next take
├── worker_pool.py         # N workers, heartbeat monitor, lease renewal
├── api.py                 # FastAPI: /submit /status /metrics /stats /health /ready
├── config.py              # Environment-driven configuration and the Redis client
├── metrics.py             # Prometheus collectors, including the SLI histograms
├── probes.py              # Stdlib HTTP server giving the worker and scheduler probes
├── tasks.py               # Registered task definitions
│
├── main.py                # Local development: all three roles in one process
├── api_main.py            # Container entrypoint: API only
├── worker_main.py         # Container entrypoint: worker pool only
├── scheduler_main.py      # Container entrypoint: delayed-queue scheduler and lease reaper
│
├── benchmark.py           # Latency, throughput and priority, median of repeats
├── scripts/crash-test.sh  # Kill a worker mid-job and count what was lost
├── test_queue.py          # pytest test suite
├── test_pool.py           # Manual worker pool smoke check, not a pytest test
├── Dockerfile             # Multi-stage, non-root, one image for all three roles
└── docker-compose.yml     # Redis plus the three processes
```
