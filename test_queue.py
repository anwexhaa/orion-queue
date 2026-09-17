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

def make_queue(lease_seconds=30):
    q = RedisPriorityQueue(client=r, queue_key="test_queue", lease_seconds=lease_seconds)
    q.clear()  # the queue and every lease key derived from it
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

    def requeue(self, job):
        # The pool requeues a stalled worker's job through its lease; the
        # stub records it the same way as a push.
        self.pushed.append(job)
        return True

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


def _series_value(text, name, **labels):
    """Value of one exposed series, or None if the series is absent."""
    want = ",".join(f'{k}="{v}"' for k, v in labels.items())
    for line in text.splitlines():
        if line.startswith(name + "{") and all(f'{k}="{v}"' in line for k, v in labels.items()):
            return float(line.rsplit(" ", 1)[1])
    return None


def test_http_error_series_exists_before_any_error():
    """Regression, from game day 2.

    Four real 500s during a Redis outage and the availability SLI recorded
    none: the status="500" series did not exist until the first error, so
    Prometheus first saw it already at 4 and increase() read 4 - 4 = 0.

    The series must be exposed at zero before the first error ever happens.
    """
    import api  # noqa: F401 - importing initialises the series
    import metrics

    body, _ = metrics.render()
    text = body.decode()

    value = _series_value(
        text, "orion_http_requests_total",
        method="POST", route="/submit", status="500",
    )
    assert value is not None, "500 series must exist before the first 500"
    assert value == 0.0


def test_job_outcome_series_exist_for_registered_tasks():
    import metrics
    from task_registry import TaskRegistry

    TaskRegistry.register("series_probe")(lambda payload: None)
    metrics.init_job_series(["series_probe"])

    text = metrics.render()[0].decode()
    assert _series_value(
        text, "orion_jobs_processed_total", task_name="series_probe", status="dead"
    ) == 0.0
    assert _series_value(
        text, "orion_job_retries_total", task_name="series_probe"
    ) == 0.0



# --- Leased fetch -------------------------------------------------------------
#
# Game day 1 killed a worker pod mid-job and lost four jobs: ZPOPMAX had removed
# them from Redis, and the monitor that requeues stalled work died with the pod.
# These tests pin down the lease that replaced it.


def _leased_ids(q):
    return set(r.zrange(q.processing_key, 0, -1))


def test_pop_leases_the_job_instead_of_deleting_it():
    q = make_queue(lease_seconds=30)
    job = Job(task_name="t", payload={})
    q.push(job)

    before = time.time()
    popped = q.pop()

    assert popped.id == job.id
    assert q.size() == 0
    assert _leased_ids(q) == {job.id}
    deadline = r.zscore(q.processing_key, job.id)
    assert before + 29 <= deadline <= time.time() + 31


def test_ack_releases_the_lease():
    q = make_queue()
    q.push(Job(task_name="t", payload={}))
    job = q.pop()

    assert q.ack(job) is True
    assert q.in_flight() == 0
    assert r.hlen(q.payload_key) == 0
    assert r.hlen(q.score_key) == 0


def test_job_held_by_a_dead_worker_is_recovered():
    """The game day 1 failure, reproduced and fixed.

    A worker pops a job and dies without acknowledging it. Nothing renews the
    lease, so once it expires the reaper must put the job back.
    """
    q = make_queue(lease_seconds=30)
    job = Job(task_name="t", payload={"n": 1})
    q.push(job)

    taken = q.pop()          # the worker takes the job...
    assert q.size() == 0     # ...and then is killed. No ack, no renewal.

    # Not yet expired: nothing to recover.
    assert q.reap_expired() == 0
    assert q.size() == 0

    # Past the lease deadline, the job comes back.
    assert q.reap_expired(now=time.time() + 31) == 1
    assert q.size() == 1
    assert q.in_flight() == 0

    again = q.pop()
    assert again.id == taken.id
    assert again.payload == {"n": 1}


def test_recovered_job_keeps_its_priority():
    q = make_queue(lease_seconds=30)
    high = Job(task_name="t", payload={}, priority=JobPriority.HIGH.value)
    q.push(high)
    lost = q.pop()                        # a worker takes the HIGH job and dies

    q.push(Job(task_name="t", payload={}, priority=JobPriority.LOW.value))
    q.reap_expired(now=time.time() + 31)  # HIGH comes back

    assert q.pop().id == lost.id, "a recovered job must not lose its place"


def test_renew_extends_only_leases_that_still_exist():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    job = q.pop()
    first = r.zscore(q.processing_key, job.id)

    assert q.renew([job.id], lease_seconds=300) == 1
    assert r.zscore(q.processing_key, job.id) > first + 200

    # Once the reaper has taken it back, renewal must not resurrect the lease,
    # or the job would be both queued and leased.
    q.reap_expired(now=time.time() + 1000)
    assert q.renew([job.id]) == 0
    assert q.in_flight() == 0


def test_late_ack_reports_that_the_lease_was_lost():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    job = q.pop()
    q.reap_expired(now=time.time() + 31)

    # The original worker finishes after its job was handed back. The job will
    # run twice; ack must say so rather than pretend it held the lease.
    assert q.ack(job) is False
    assert q.size() == 1


def test_requeue_after_reap_does_not_duplicate_the_job():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    job = q.pop()
    q.reap_expired(now=time.time() + 31)   # already back on the queue

    job.status = JobStatus.PENDING
    assert q.requeue(job) is False
    assert q.size() == 1, "requeueing an already-reaped job must not add a copy"


def test_requeue_while_leased_moves_the_job_back():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    job = q.pop()
    job.status = JobStatus.PENDING

    assert q.requeue(job) is True
    assert q.size() == 1
    assert q.in_flight() == 0


def test_reaper_drains_a_large_backlog_in_one_pass():
    """A whole node lost at once leaves many expired leases."""
    from lease_reaper import LeaseReaper

    q = make_queue(lease_seconds=1)
    for i in range(250):
        q.push(Job(task_name="t", payload={"i": i}))
    for _ in range(250):
        q.pop()
    time.sleep(1.2)

    assert LeaseReaper(q, batch=100).reap_once() == 250
    assert q.size() == 250
    assert q.in_flight() == 0


# --- The worker's side of the lease --------------------------------------------


def _wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


def test_worker_acknowledges_a_finished_job():
    from task_registry import TaskRegistry

    done = []
    TaskRegistry.register("lease_ok")(lambda payload: done.append(1))

    q = make_queue()
    dq = DelayedQueue(client=r, delayed_key="test_lease_delayed")
    dlq = DeadLetterQueue(client=r, dlq_key="test_lease_dlq")
    pool = WorkerPool(n_workers=1, queue=q, retry_manager=RetryManager(dq, dlq))
    pool.start()
    try:
        q.push(Job(task_name="lease_ok", payload={}))
        assert _wait_for(lambda: done and q.in_flight() == 0)
    finally:
        pool.scale_down(1)


def test_worker_keeps_the_lease_when_a_failure_cannot_be_recorded():
    """If recording the outcome fails, giving the lease back would lose the job.

    Holding it instead means the reaper retries the job later.
    """
    from task_registry import TaskRegistry

    def boom(payload):
        raise RuntimeError("task failed")

    TaskRegistry.register("lease_boom")(boom)

    class BrokenRetryManager:
        def handle_failure(self, job, error):
            raise ConnectionError("redis unavailable")

    q = make_queue()
    pool = WorkerPool(n_workers=1, queue=q, retry_manager=BrokenRetryManager())
    pool.start()
    try:
        job = Job(task_name="lease_boom", payload={})
        q.push(job)
        assert _wait_for(lambda: q.size() == 0)
        time.sleep(0.3)
        assert _leased_ids(q) == {job.id}, "the lease must survive an unrecorded failure"
    finally:
        pool.scale_down(1)


def test_worker_survives_the_queue_raising():
    """A Redis outage used to raise out of pop() and kill the worker thread."""
    import worker as worker_module

    class FlakyQueue:
        def __init__(self):
            self.calls = 0

        def pop(self):
            self.calls += 1
            if self.calls <= 3:
                raise ConnectionError("redis unavailable")
            return None

    original = worker_module.ERROR_BACKOFF_SECONDS
    worker_module.ERROR_BACKOFF_SECONDS = 0.01
    try:
        flaky = FlakyQueue()
        w = worker_module.Worker("w", flaky, retry_manager=None)
        w.start()
        assert _wait_for(lambda: flaky.calls > 3)
        assert w._thread.is_alive()
        w.stop()
    finally:
        worker_module.ERROR_BACKOFF_SECONDS = original


def test_pool_renews_the_leases_of_running_jobs():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    job = q.pop()
    first = r.zscore(q.processing_key, job.id)

    pool = WorkerPool(n_workers=0, queue=q, retry_manager=None)
    holder = pool._new_worker()          # never started
    holder.current_job = job
    pool.workers = [holder]

    time.sleep(0.05)
    assert pool.renew_leases() == 1
    assert r.zscore(q.processing_key, job.id) > first



# --- One round trip per job ----------------------------------------------------
#
# Leasing alone doubled the Redis round trips per job - pop, then ack - and
# halved benchmark throughput. take() acknowledges the previous job and leases
# the next in one call.


def test_take_acknowledges_and_leases_in_one_call():
    q = make_queue()
    q.push(Job(task_name="t", payload={"n": 1}))
    q.push(Job(task_name="t", payload={"n": 2}))

    acked, first = q.take()
    assert acked is None                  # nothing to acknowledge yet
    assert q.in_flight() == 1

    acked, second = q.take(ack=first)
    assert acked is True
    assert second.id != first.id
    assert _leased_ids(q) == {second.id}  # first released, second leased

    acked, nothing = q.take(ack=second)
    assert acked is True
    assert nothing is None
    assert q.in_flight() == 0


def test_take_reports_a_lease_that_was_already_lost():
    q = make_queue(lease_seconds=30)
    q.push(Job(task_name="t", payload={}))
    _, job = q.take()
    q.reap_expired(now=time.time() + 31)

    acked, again = q.take(ack=job)
    assert acked is False
    assert again.id == job.id   # the requeued copy, which will now run twice


def test_stopping_mid_job_still_sends_the_last_acknowledgement():
    """The ack for the final job rides on a take() that never happens.

    If a worker is stopped while a job runs, the loop exits straight after the
    job. Without the flush on exit the lease would stay held, expire, and the
    finished job would be run again by another worker.
    """
    from task_registry import TaskRegistry
    from worker import Worker

    TaskRegistry.register("lease_slow")(lambda payload: time.sleep(0.5))

    q = make_queue()
    q.push(Job(task_name="lease_slow", payload={}))
    w = Worker("w", q, retry_manager=None)
    w.start()
    try:
        assert _wait_for(lambda: q.in_flight() == 1)   # the job is running
        w.stop()                                       # stop mid-job
        w.join(3)
        assert not w.is_alive()
        assert q.in_flight() == 0, "the last acknowledgement was stranded"
        assert q.size() == 0
    finally:
        w.stop()
