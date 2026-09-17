"""Redis priority queue with leased fetch.

Popping a job does not delete it. It moves it, atomically, into a processing
set with a lease deadline. The worker acknowledges the job once its outcome is
recorded, and renews the lease while the job runs. If the worker dies holding
the job, renewals stop, the lease expires, and the reaper puts the job back on
the queue with its original priority.

Why: the original pop was ZPOPMAX, which removed the job from Redis before the
worker ran it. Game day 1 killed a worker pod mid-job and lost four jobs - their
submitters had been told "accepted", the work never ran, and nothing alerted.
The heartbeat monitor that requeues stalled jobs lives inside the worker
process, so it died with the pod. A lease lives in Redis, so it does not.

Delivery is therefore AT-LEAST-ONCE, not exactly-once. A worker that finishes a
job just after its lease expired, or that crashes between finishing and
acknowledging, means the job runs twice. Tasks must be idempotent.

Keys, all derived from queue_key so tests and benchmarks stay isolated:

    <queue_key>                       sorted set  job json -> priority score
    <queue_key>:processing            sorted set  job id   -> lease deadline
    <queue_key>:processing:payload    hash        job id   -> job json
    <queue_key>:processing:score      hash        job id   -> priority score
"""

import json
import time
from datetime import datetime

import config
from job import Job

# ZPOPMAX and the lease are one atomic step. Without that, a crash between the
# two would lose the job exactly as before.
_POP = """
local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then return false end
local member, score = popped[1], popped[2]
local ok, decoded = pcall(cjson.decode, member)
if ok and type(decoded) == 'table' and decoded['id'] then
  local id = decoded['id']
  redis.call('ZADD', KEYS[2], ARGV[1], id)
  redis.call('HSET', KEYS[3], id, member)
  redis.call('HSET', KEYS[4], id, score)
end
return member
"""

# Acknowledge the previous job and lease the next one in a single round trip.
# Without this, at-least-once delivery doubled the Redis round trips per job
# (pop, then ack) and halved throughput on the benchmark, where the task itself
# costs nothing and round trips are the entire cost.
# Returns {held, member}: held is -1 when there was nothing to acknowledge,
# otherwise as for _ACK; member is '' when the queue was empty.
_ACK_AND_POP = """
local held = -1
if ARGV[2] ~= '' then
  held = redis.call('ZREM', KEYS[2], ARGV[2])
  redis.call('HDEL', KEYS[3], ARGV[2])
  redis.call('HDEL', KEYS[4], ARGV[2])
end
local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then return {held, ''} end
local member, score = popped[1], popped[2]
local ok, decoded = pcall(cjson.decode, member)
if ok and type(decoded) == 'table' and decoded['id'] then
  local id = decoded['id']
  redis.call('ZADD', KEYS[2], ARGV[1], id)
  redis.call('HSET', KEYS[3], id, member)
  redis.call('HSET', KEYS[4], id, score)
end
return {held, member}
"""

# Returns 1 if the caller still held the lease, 0 if it had already expired
# and been requeued - in which case the job is running a second time elsewhere.
_ACK = """
local held = redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return held
"""

# Only requeues if the lease is still held. Otherwise the reaper has already
# put the job back, and adding it again would create a duplicate in the queue.
_REQUEUE = """
local held = redis.call('ZREM', KEYS[1], ARGV[1])
if held == 0 then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[2])
return 1
"""

# XX: extend existing leases only. A lease the reaper has already taken is
# not resurrected.
_RENEW = """
local n = 0
for i = 2, #ARGV do
  n = n + redis.call('ZADD', KEYS[1], 'XX', 'CH', ARGV[1], ARGV[i])
end
return n
"""

# Atomic per call, so two reapers can never requeue the same job: whichever
# runs second no longer finds it. The reaper does not need to be a singleton.
_REAP = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local requeued = 0
for _, id in ipairs(ids) do
  local member = redis.call('HGET', KEYS[2], id)
  local score = redis.call('HGET', KEYS[3], id)
  redis.call('ZREM', KEYS[1], id)
  redis.call('HDEL', KEYS[2], id)
  redis.call('HDEL', KEYS[3], id)
  if member then
    redis.call('ZADD', KEYS[4], score or '0', member)
    requeued = requeued + 1
  end
end
return requeued
"""


class RedisPriorityQueue:
    def __init__(self, host=None, port=None, queue_key=None, client=None,
                 lease_seconds=None):
        # Defaults come from the environment, so the same code runs against
        # localhost on a laptop and against a Redis Service in the cluster.
        # Explicit arguments still win.
        if client:
            self.client = client
        else:
            overrides = {}
            if host is not None:
                overrides["host"] = host
            if port is not None:
                overrides["port"] = port
            self.client = config.redis_client(**overrides)

        self.queue_key = queue_key if queue_key is not None else config.QUEUE_KEY
        self.lease_seconds = (
            config.LEASE_SECONDS if lease_seconds is None else lease_seconds
        )

        self.processing_key = f"{self.queue_key}:processing"
        self.payload_key = f"{self.processing_key}:payload"
        self.score_key = f"{self.processing_key}:score"
        self._lease_keys = [self.processing_key, self.payload_key, self.score_key]

        # register_script does no network I/O; redis-py sends EVALSHA and
        # falls back to EVAL if the server has not seen the script yet.
        self._pop = self.client.register_script(_POP)
        self._ack_and_pop = self.client.register_script(_ACK_AND_POP)
        self._ack = self.client.register_script(_ACK)
        self._requeue = self.client.register_script(_REQUEUE)
        self._renew = self.client.register_script(_RENEW)
        self._reap = self.client.register_script(_REAP)

    @staticmethod
    def score(job: Job) -> float:
        # Priority first; within a priority, earlier submissions sort higher.
        timestamp = datetime.fromisoformat(job.submit_time).timestamp()
        return job.priority - (timestamp / 1e12)

    def push(self, job: Job):
        self.client.zadd(self.queue_key, {json.dumps(job.to_dict()): self.score(job)})

    def pop(self) -> Job | None:
        """Take the highest-priority job and lease it to the caller."""
        member = self._pop(
            keys=[self.queue_key, *self._lease_keys],
            args=[time.time() + self.lease_seconds],
        )
        return self._parse(member)

    def take(self, ack: Job | None = None) -> tuple[bool | None, Job | None]:
        """Acknowledge `ack`, if given, and lease the next job - one round trip.

        Returns (acked, job). acked is None when nothing was acknowledged,
        otherwise as ack() would have returned.
        """
        held, member = self._ack_and_pop(
            keys=[self.queue_key, *self._lease_keys],
            args=[time.time() + self.lease_seconds, ack.id if ack else ""],
        )
        acked = None if int(held) < 0 else bool(int(held))
        return acked, self._parse(member or None)

    @staticmethod
    def _parse(member) -> Job | None:
        if not member:
            return None
        try:
            return Job.from_dict(json.loads(member))
        except (ValueError, KeyError, TypeError) as exc:
            # An entry that cannot be parsed would crash every worker that
            # pops it. It has been removed from the queue; say so and move on.
            print(f"[queue] discarding unreadable entry: {exc}", flush=True)
            return None

    def ack(self, job: Job) -> bool:
        """Release the lease. False means it had already expired."""
        return bool(self._ack(keys=self._lease_keys, args=[job.id]))

    def requeue(self, job: Job) -> bool:
        """Put a leased job back on the queue, if the lease is still held."""
        return bool(self._requeue(
            keys=[*self._lease_keys, self.queue_key],
            args=[job.id, json.dumps(job.to_dict()), self.score(job)],
        ))

    def renew(self, job_ids, lease_seconds=None) -> int:
        """Extend live leases. Returns how many were extended."""
        ids = list(job_ids)
        if not ids:
            return 0
        deadline = time.time() + (
            self.lease_seconds if lease_seconds is None else lease_seconds
        )
        return int(self._renew(keys=[self.processing_key], args=[deadline, *ids]))

    def reap_expired(self, now=None, limit=100) -> int:
        """Requeue jobs whose lease has expired. Returns how many."""
        return int(self._reap(
            keys=[*self._lease_keys, self.queue_key],
            args=[time.time() if now is None else now, limit],
        ))

    def in_flight(self) -> int:
        return self.client.zcard(self.processing_key)

    def size(self) -> int:
        return self.client.zcard(self.queue_key)

    def clear(self):
        self.client.delete(self.queue_key, *self._lease_keys)
