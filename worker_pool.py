import threading
import time
import uuid
from datetime import datetime, timedelta

import config
import metrics
from job import JobStatus
from worker import Worker

HEARTBEAT_TIMEOUT = timedelta(seconds=10)


class WorkerPool:
    """A fixed-size pool of worker threads with two layers of recovery.

    Within the process, a heartbeat monitor replaces a wedged thread within
    about ten seconds and requeues the job it was holding.

    Across processes, every job a worker holds is leased in Redis, and this
    pool renews those leases while the jobs run. If the whole process dies -
    pod killed, node lost - the renewals stop with it, the leases expire, and
    the reaper in the scheduler requeues the work. Game day 1 found the first
    layer alone lost four jobs when a pod was killed; the second layer is what
    closes that gap.
    """

    def __init__(self, n_workers: int, queue, retry_manager, job_store=None):
        self.queue = queue
        self.retry_manager = retry_manager
        self.job_store = job_store
        self.workers: list[Worker] = []
        self.n_workers = n_workers
        # Renew well inside the lease, so one missed renewal - a slow Redis
        # round trip, a GC pause - does not hand a running job to a second
        # worker.
        lease = getattr(queue, "lease_seconds", config.LEASE_SECONDS)
        self.renew_interval = max(1.0, lease / 3)

    def start(self):
        for _ in range(self.n_workers):
            self._spawn_worker()
        self._start_monitor()

    def _new_worker(self) -> Worker:
        return Worker(
            str(uuid.uuid4()),
            self.queue,
            self.retry_manager,
            job_store=self.job_store,
        )

    def _spawn_worker(self):
        w = self._new_worker()
        w.start()
        self.workers.append(w)

    def _start_monitor(self):
        t = threading.Thread(target=self._monitor, daemon=True, name="heartbeat")
        t.start()

    def check_once(self, now: datetime | None = None) -> int:
        """One pass of the heartbeat monitor. Returns how many workers were replaced.

        Split out from the monitor loop so the self-healing path can be tested
        directly, rather than only by waiting on a background thread.
        """
        now = now or datetime.now()
        replaced = 0

        for i, worker in enumerate(self.workers):
            if now - worker.last_heartbeat <= HEARTBEAT_TIMEOUT:
                continue

            stalled_job = worker.current_job
            if stalled_job:
                # JobStatus.PENDING, not the string "pending": Job.to_dict()
                # reads status.value, and a raw string raises inside this
                # thread, taking self-healing down with it.
                stalled_job.status = JobStatus.PENDING
                if self.job_store:
                    self.job_store.save(stalled_job)
                # requeue, not push. The job is leased; pushing a copy would
                # leave the lease live, and when it expired the reaper would
                # requeue it a second time. requeue releases the lease and
                # re-adds the job in one step, and does nothing if the reaper
                # already got there first.
                self.queue.requeue(stalled_job)

            worker.stop()
            replacement = self._new_worker()
            replacement.start()
            self.workers[i] = replacement
            replaced += 1

        if replaced:
            metrics.workers_replaced_total.inc(replaced)
        metrics.workers_alive.set(len(self.workers))
        return replaced

    def renew_leases(self) -> int:
        """Extend the lease on every job a worker in this pool is running."""
        renew = getattr(self.queue, "renew", None)
        if renew is None:
            return 0
        ids = []
        for worker in self.workers:
            job = worker.current_job  # read once; the worker may clear it
            if job is not None:
                ids.append(job.id)
        return renew(ids) if ids else 0

    def _monitor(self):
        last_renew = 0.0
        while True:
            try:
                self.check_once()
            except Exception as exc:  # noqa: BLE001 - deliberately broad
                # This thread is the self-healing mechanism. If it dies, the
                # pool stops replacing stalled workers and nothing says so.
                print(f"[pool] heartbeat check failed: {exc}", flush=True)

            if time.monotonic() - last_renew >= self.renew_interval:
                try:
                    self.renew_leases()
                except Exception as exc:  # noqa: BLE001
                    # A missed renewal is survivable - there are two more
                    # before the lease runs out.
                    print(f"[pool] lease renewal failed: {exc}", flush=True)
                last_renew = time.monotonic()

            time.sleep(1)

    def scale_up(self, n: int):
        for _ in range(n):
            self._spawn_worker()
        metrics.workers_alive.set(len(self.workers))

    def scale_down(self, n: int):
        for _ in range(min(n, len(self.workers))):
            w = self.workers.pop()
            w.stop()
        metrics.workers_alive.set(len(self.workers))
