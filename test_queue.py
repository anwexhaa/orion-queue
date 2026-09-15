import pytest
import config
import time
from job import Job, JobStatus, JobPriority
from priority_queue import RedisPriorityQueue
from delayed_queue import DelayedQueue
from dead_letter_queue import DeadLetterQueue
from retry_manager import RetryManager

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