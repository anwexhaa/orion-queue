import time
from datetime import datetime, timedelta

import pytest

import config
from job import Job, JobStatus, JobPriority
from priority_queue import RedisPriorityQueue
from delayed_queue import DelayedQueue
from dead_letter_queue import DeadLetterQueue
from job_store import JobStore
from retry_manager import RetryManager
from worker_pool import HEARTBEAT_TIMEOUT, WorkerPool

r = config.redis_client()

def make_queue():
    q = RedisPriorityQueue(client=r, queue_key="test_queue")
    r.delete("test_queue")
    return q

def test_priority_ordering():
    q = make_queue()
    low = Job(task_name="t", payload={}, priority=JobPriority.LOW.value)
    high = Job(task_name="t", payload={}, priority=JobPriority.HIGH.value)
    normal = Job(task_name="t", payload={}, priority=JobPriority.NORMAL.value)

    q.push(low)
    q.push(high)
    q.push(normal)

    assert q.pop().priority == JobPriority.HIGH.value
    assert q.pop().priority == JobPriority.NORMAL.value
    assert q.pop().priority == JobPriority.LOW.value

def test_retry_then_dead():
    dlq = DeadLetterQueue(client=r, dlq_key="test_dlq")
    r.delete("test_dlq")
    dq = DelayedQueue(client=r, delayed_key="test_delayed")
    r.delete("test_delayed")
    rm = RetryManager(dq, dlq)

    j = Job(task_name="fail", payload={}, max_retries=3)
    rm.handle_failure(j, "err")
    rm.handle_failure(j, "err")
    rm.handle_failure(j, "err")

    assert j.status == JobStatus.DEAD
    assert dlq.size() == 1

def test_delayed_queue_requeues():
    q = make_queue()
    dq = DelayedQueue(client=r, delayed_key="test_delayed2")
    r.delete("test_delayed2")

    j = Job(task_name="t", payload={})
    # set next_retry_time to the past so it's immediately due
    j.next_retry_time = "2000-01-01T00:00:00"
    dq.add(j)

    moved = dq.poll(q)
    assert moved == 1
    assert q.size() == 1

def test_dlq_stores_dead_jobs():
    dlq = DeadLetterQueue(client=r, dlq_key="test_dlq2")
    r.delete("test_dlq2")

    j = Job(task_name="dead", payload={"x": 1}, status=JobStatus.DEAD)
    dlq.add(j)

    all_jobs = dlq.get_all()
    assert len(all_jobs) == 1
    assert all_jobs[0]["task_name"] == "dead"
    assert "dead_at" in all_jobs[0]

class RecordingQueue:
    """Stands in for the Redis queue so the pool test never races a live worker.

    A replacement worker starts a real thread the moment it is created, and it
    would pop the very job the test is asserting on.
    """

    def __init__(self):
        self.pushed = []

    def push(self, job):
        self.pushed.append(job)

    def pop(self):
        return None

    def size(self):
        return len(self.pushed)


def test_stalled_worker_is_replaced_and_its_job_requeued():
    q = RecordingQueue()
    pool = WorkerPool(n_workers=0, queue=q, retry_manager=None)

    worker = pool._new_worker()
    pool.workers = [worker]

    stuck = Job(task_name="t", payload={})
    worker.current_job = stuck
    worker.last_heartbeat = datetime.now() - (HEARTBEAT_TIMEOUT + timedelta(seconds=1))

    assert pool.check_once() == 1
    assert pool.workers[0] is not worker, "stalled worker was not replaced"
    assert len(q.pushed) == 1, "in-flight job was not requeued"
    assert q.pushed[0].status is JobStatus.PENDING


def test_requeued_job_survives_serialisation():
    """Regression: the monitor used to assign the string "pending" instead of
    JobStatus.PENDING, so to_dict() raised AttributeError on status.value and
    took the heartbeat thread down with it."""
    q = RecordingQueue()
    pool = WorkerPool(n_workers=0, queue=q, retry_manager=None)

    worker = pool._new_worker()
    pool.workers = [worker]
    worker.current_job = Job(task_name="t", payload={})
    worker.last_heartbeat = datetime.now() - (HEARTBEAT_TIMEOUT + timedelta(seconds=1))

    pool.check_once()
    assert q.pushed[0].to_dict()["status"] == "pending"


def test_healthy_worker_is_left_alone():
    q = RecordingQueue()
    pool = WorkerPool(n_workers=0, queue=q, retry_manager=None)

    worker = pool._new_worker()
    worker.last_heartbeat = datetime.now()
    pool.workers = [worker]

    assert pool.check_once() == 0
    assert pool.workers[0] is worker
    assert q.pushed == []


def test_job_store_records_terminal_status():
    """Regression: the worker mutated job.status in memory only, so
    /status/{id} reported pending for every job forever."""
    store = JobStore(r, ttl_seconds=60)
    job = Job(task_name="t", payload={})

    store.save(job)
    assert store.get(job.id)["status"] == "pending"

    job.status = JobStatus.DONE
    store.save(job)
    assert store.get(job.id)["status"] == "done"

    ttl = r.ttl(store.key(job.id))
    assert 0 < ttl <= 60, f"job record should expire, got ttl={ttl}"

    r.delete(store.key(job.id))


def test_job_store_returns_none_for_unknown_job():
    assert JobStore(r).get("does-not-exist") is None
