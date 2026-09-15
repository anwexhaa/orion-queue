import threading
import time
import uuid
from datetime import datetime, timedelta

import metrics
from job import JobStatus
from worker import Worker

HEARTBEAT_TIMEOUT = timedelta(seconds=10)


class WorkerPool:
    def __init__(self, n_workers: int, queue, retry_manager, job_store=None):
        self.queue = queue
        self.retry_manager = retry_manager
        self.job_store = job_store
        self.workers: list[Worker] = []
        self.n_workers = n_workers

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
                # JobStatus.PENDING, not the string "pending". Job.to_dict()
                # reads status.value, so a raw string raises AttributeError
                # here — inside the monitor thread, where it would take the
                # whole self-healing mechanism down silently.
                stalled_job.status = JobStatus.PENDING
                if self.job_store:
                    self.job_store.save(stalled_job)
                self.queue.push(stalled_job)

            worker.stop()
            replacement = self._new_worker()
            replacement.start()
            self.workers[i] = replacement
            replaced += 1

        if replaced:
            metrics.workers_replaced_total.inc(replaced)
        metrics.workers_alive.set(len(self.workers))
        return replaced

    def _monitor(self):
        while True:
            try:
                self.check_once()
            except Exception as exc:  # noqa: BLE001 - deliberately broad
                # This thread is the self-healing mechanism. If it dies, the
                # pool stops replacing stalled workers and nothing says so.
                print(f"[pool] heartbeat check failed: {exc}", flush=True)
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
