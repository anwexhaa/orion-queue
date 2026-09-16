"""Prometheus instrumentation.

The two service level indicators Helios measures come from here:

  availability — orion_jobs_processed_total split by status
  latency      — orion_dispatch_latency_seconds, the time between a job being
                 submitted and a worker picking it up

DISPATCH_BUCKETS contains 0.3 deliberately. The latency SLO is "95% of jobs
dispatched within 300 ms", and a histogram can only answer that exactly if
300 ms is a bucket boundary. Change the SLO and this list has to change with
it, or the number on the dashboard quietly becomes an interpolation.
"""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

REGISTRY = CollectorRegistry(auto_describe=True)

DISPATCH_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.2,
    0.3,  # the SLO threshold
    0.5, 1.0, 2.5, 5.0, 10.0,
)

DURATION_BUCKETS = (
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
)

# --- HTTP, the availability SLI -------------------------------------------
#
# Labelled by ROUTE TEMPLATE, never the raw path. "/status/{job_id}" is one
# time series; "/status/<uuid>" would be one series per job ever submitted,
# which is how a metrics backend gets taken down by its own instrumentation.

http_requests_total = Counter(
    "orion_http_requests_total",
    "HTTP requests handled by the API",
    ["method", "route", "status"],
    registry=REGISTRY,
)

http_request_duration = Histogram(
    "orion_http_request_duration_seconds",
    "Time to serve an HTTP request",
    ["method", "route"],
    buckets=DISPATCH_BUCKETS,
    registry=REGISTRY,
)

jobs_submitted = Counter(
    "orion_jobs_submitted_total",
    "Jobs accepted by the API",
    ["task_name"],
    registry=REGISTRY,
)

jobs_processed = Counter(
    "orion_jobs_processed_total",
    "Jobs that reached a terminal state",
    ["task_name", "status"],
    registry=REGISTRY,
)

job_retries = Counter(
    "orion_job_retries_total",
    "Retry attempts scheduled",
    ["task_name"],
    registry=REGISTRY,
)

dispatch_latency = Histogram(
    "orion_dispatch_latency_seconds",
    "Time from job submission to a worker starting it",
    buckets=DISPATCH_BUCKETS,
    registry=REGISTRY,
)

job_duration = Histogram(
    "orion_job_duration_seconds",
    "Time spent executing a job",
    ["task_name"],
    buckets=DURATION_BUCKETS,
    registry=REGISTRY,
)

queue_depth = Gauge(
    "orion_queue_depth",
    "Jobs waiting in the priority queue",
    registry=REGISTRY,
)

dead_letter_depth = Gauge(
    "orion_dead_letter_depth",
    "Jobs in the dead letter queue",
    registry=REGISTRY,
)

workers_alive = Gauge(
    "orion_workers_alive",
    "Worker threads currently registered in this pool",
    registry=REGISTRY,
)

workers_replaced_total = Counter(
    "orion_workers_replaced_total",
    "Workers replaced by the heartbeat monitor after stalling",
    registry=REGISTRY,
)

redis_up = Gauge(
    "orion_redis_up",
    "1 when the last Redis round-trip from this process succeeded",
    registry=REGISTRY,
)


# --- Series initialisation ---------------------------------------------------
#
# A labelled counter child does not exist until its first .inc(). If the first
# events for a label combination arrive between two scrapes, Prometheus first
# sees that series already at N, never at 0, and increase()/rate() over it read
# N - N = 0. The events happened; the SLI cannot see them.
#
# Found in game day 2: four real 500s during a Redis outage, and the
# availability SLI recorded none, because the status="500" series was born
# mid-outage at value 4. The first burst of any status code the process had not
# produced before was invisible - which in practice means the first outage.
#
# Calling .labels() without .inc() creates the child at zero, so Prometheus
# observes the 0 -> N transition and counts it.

HTTP_SERIES = (
    ("POST", "/submit", ("200", "422", "500", "503")),
    ("GET", "/status/{job_id}", ("200", "404", "500", "503")),
)


def init_http_series() -> None:
    for method, route, statuses in HTTP_SERIES:
        for status in statuses:
            http_requests_total.labels(method=method, route=route, status=status)


def init_job_series(task_names) -> None:
    for name in task_names:
        for status in ("done", "dead"):
            jobs_processed.labels(task_name=name, status=status)
        job_retries.labels(task_name=name)


def render() -> tuple[bytes, str]:
    """Metrics in the Prometheus text exposition format, with its content type."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
