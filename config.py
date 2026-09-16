"""Runtime configuration, read from the environment.

Every default here is the value that used to be hardcoded, so local
development and the existing tests behave exactly as they did before this
module existed. Nothing needs to be set to run `python main.py` against a
local Redis.

In Kubernetes, `localhost` is the pod itself, so REDIS_HOST is the one value
that must always be supplied there.
"""

import os

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


# --- Redis -----------------------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = _int("REDIS_PORT", 6379)
REDIS_DB = _int("REDIS_DB", 0)
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# --- Keys ------------------------------------------------------------------

QUEUE_KEY = os.getenv("QUEUE_KEY", "task_queue")
DLQ_KEY = os.getenv("DLQ_KEY", "dead_letter_queue")
DELAYED_KEY = os.getenv("DELAYED_KEY", "delayed_queue")

# --- Process shape ---------------------------------------------------------

# How long a job record survives in Redis after it is written. Without a
# TTL, every job ever submitted leaves a permanent key behind.
JOB_TTL_SECONDS = _int("JOB_TTL_SECONDS", 86400)

WORKER_COUNT = _int("WORKER_COUNT", 4)
SCHEDULER_POLL_INTERVAL = _float("SCHEDULER_POLL_INTERVAL", 0.5)

HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = _int("HTTP_PORT", 8000)

# Identifies which deployment a metric came from once all three run in the
# same cluster.
ROLE = os.getenv("ORION_ROLE", "all-in-one")


def redis_client(**overrides) -> redis.Redis:
    """A Redis client configured from the environment.

    The timeouts matter more than they look. Without them a Redis outage
    leaves workers blocked on a socket read forever, so the pods stay Ready,
    the queue stops draining, and nothing reports an error. With them the
    call raises, the readiness probe fails, and the problem is visible.
    """
    params = dict(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        # One attempt, no backoff. redis-py's default is three retries with
        # exponential backoff, which sounds prudent and is not: with Redis
        # down, a single /submit took 48 seconds to return its 500, because
        # each of two Redis calls burned the full retry budget first.
        #
        # Requests that are certainly going to fail should fail immediately.
        # Slow failures fill the worker pool, so a dependency outage becomes
        # a total outage, and the client has usually given up long before the
        # retries finish anyway.
        retry=Retry(NoBackoff(), 0),
        health_check_interval=30,
    )
    params.update(overrides)
    return redis.Redis(**params)


def summary() -> str:
    """One line for the startup log. Never includes the password."""
    return (
        f"role={ROLE} redis={REDIS_HOST}:{REDIS_PORT}/{REDIS_DB} "
        f"queue={QUEUE_KEY} workers={WORKER_COUNT}"
    )
