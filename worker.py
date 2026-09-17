import threading
import time
from datetime import datetime

import metrics
from job import Job, JobStatus
from task_registry import TaskRegistry

# How long to wait before polling again after the queue itself errored - a
# Redis outage, say. Short enough to recover quickly, long enough not to spin.
ERROR_BACKOFF_SECONDS = 1.0


class Worker:
    def __init__(self, worker_id: str, queue, retry_manager, job_store=None):
        self.id = worker_id
        self.queue = queue
        self.retry_manager = retry_manager
        # Optional so the existing benchmark and pool check, which never
        # needed a store, keep working unchanged.
        self.job_store = job_store
        self.last_heartbeat = datetime.now()
        self.current_job: Job | None = None
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._running = False

    def join(self, timeout=None):
        self._thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _persist(self, job: Job):
        """Write the job's current state so /status/{id} reflects reality."""
        if self.job_store:
            self.job_store.save(job)

    @staticmethod
    def _count_ack(acked):
        if acked is False:
            # The lease had already expired and the job was requeued, so
            # another worker will run it too. At-least-once delivery working as
            # designed - but worth counting, because a rising rate means
            # LEASE_SECONDS is too short for the real tasks.
            metrics.lease_ack_late_total.inc()

    def _take(self, ack: Job | None):
        """Acknowledge `ack` and fetch the next job, in one round trip if possible."""
        take = getattr(self.queue, "take", None)
        if take is not None:
            return take(ack)
        # Queues without take() - test stubs, mostly - get the two calls.
        acked = None
        if ack is not None and hasattr(self.queue, "ack"):
            acked = self.queue.ack(ack)
        return acked, self.queue.pop()

    def _release(self, job: Job):
        """Acknowledge a job on its own, when there is no next pop to carry it.

        Never raises. If Redis is unreachable the lease simply expires and the
        reaper requeues the job, so the worst case is a duplicate execution,
        not a lost job.
        """
        ack = getattr(self.queue, "ack", None)
        if ack is None:
            return
        try:
            self._count_ack(ack(job))
        except Exception as exc:  # noqa: BLE001 - deliberately broad
            print(f"[worker {self.id[:8]}] could not ack {job.id}: {exc}", flush=True)

    def _record_dispatch_latency(self, job: Job):
        """Time from submission to this worker picking the job up.

        Only recorded on the first attempt. A retried job keeps its original
        submit_time, so measuring retries here would fold the deliberate
        backoff delay into the latency SLI and make the service look slow
        precisely when it is behaving correctly.
        """
        if job.attempt != 0:
            return
        try:
            submitted = datetime.fromisoformat(job.submit_time)
        except (ValueError, TypeError):
            return
        metrics.dispatch_latency.observe(
            max(0.0, (datetime.now() - submitted).total_seconds())
        )

    def _run(self):
        # The job whose outcome is recorded but whose lease has not been given
        # back yet. Its acknowledgement rides on the next take(), so each job
        # costs one Redis round trip instead of two. A one-item list, so _loop
        # can update it and this frame still sees it on the way out.
        pending: list[Job | None] = [None]
        try:
            self._loop(pending)
        finally:
            # Stopping - a SIGTERM drain, a scale-down - must not strand the
            # last acknowledgement. Left unsent, the lease would expire and the
            # reaper would run a finished job a second time.
            if pending[0] is not None:
                self._release(pending[0])

    def _loop(self, pending):
        while self._running:
            try:
                acked, job = self._take(pending[0])
            except Exception as exc:  # noqa: BLE001 - deliberately broad
                # Previously uncaught: a Redis outage raised out of pop() and
                # killed the thread, the heartbeat monitor replaced it, and
                # the replacement died the same way. A thread looping on a
                # dependency error is alive, not wedged, so it keeps its
                # heartbeat and waits.
                print(f"[worker {self.id[:8]}] pop failed: {exc}", flush=True)
                # The pending acknowledgement is kept and retried next time.
                self.last_heartbeat = datetime.now()
                time.sleep(ERROR_BACKOFF_SECONDS)
                continue

            if pending[0] is not None:
                self._count_ack(acked)
                pending[0] = None

            if not job:
                self.last_heartbeat = datetime.now()
                time.sleep(0.1)
                continue

            self.current_job = job
            job.status = JobStatus.RUNNING
            self._persist(job)
            self._record_dispatch_latency(job)

            # Only give the lease back once the outcome is durable. If
            # recording a failure itself fails, keeping the lease means the
            # reaper retries the job later instead of it disappearing.
            outcome_recorded = False
            started = time.perf_counter()
            try:
                task_fn = TaskRegistry.get(job.task_name)
                task_fn(job.payload)
                job.status = JobStatus.DONE
                outcome_recorded = True
                self._persist(job)
                metrics.jobs_processed.labels(
                    task_name=job.task_name, status=JobStatus.DONE.value
                ).inc()
            except Exception as e:
                print(f"[worker {self.id[:8]}] job {job.task_name} failed: {e}")
                try:
                    # handle_failure decides between another retry and the
                    # dead letter queue, and records that decision on the job.
                    self.retry_manager.handle_failure(job, str(e))
                    outcome_recorded = True
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[worker {self.id[:8]}] could not record failure of "
                        f"{job.id}, leaving its lease to expire: {exc}",
                        flush=True,
                    )
                self._persist(job)
                if outcome_recorded:
                    if job.status == JobStatus.DEAD:
                        metrics.jobs_processed.labels(
                            task_name=job.task_name, status=JobStatus.DEAD.value
                        ).inc()
                    else:
                        metrics.job_retries.labels(task_name=job.task_name).inc()
            finally:
                if outcome_recorded:
                    pending[0] = job
                metrics.job_duration.labels(task_name=job.task_name).observe(
                    time.perf_counter() - started
                )
                self.last_heartbeat = datetime.now()
                self.current_job = None
