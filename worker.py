import threading
import time
from datetime import datetime

import metrics
from job import Job, JobStatus
from task_registry import TaskRegistry


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

    def _persist(self, job: Job):
        """Write the job's current state so /status/{id} reflects reality."""
        if self.job_store:
            self.job_store.save(job)

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
        while self._running:
            job = self.queue.pop()
            if not job:
                self.last_heartbeat = datetime.now()
                time.sleep(0.1)
                continue

            self.current_job = job
            job.status = JobStatus.RUNNING
            self._persist(job)
            self._record_dispatch_latency(job)

            started = time.perf_counter()
            try:
                task_fn = TaskRegistry.get(job.task_name)
                task_fn(job.payload)
                job.status = JobStatus.DONE
                self._persist(job)
                metrics.jobs_processed.labels(
                    task_name=job.task_name, status=JobStatus.DONE.value
                ).inc()
            except Exception as e:
                print(f"[worker {self.id[:8]}] job {job.task_name} failed: {e}")
                self.retry_manager.handle_failure(job, str(e))
                self._persist(job)
                # handle_failure decides between another retry and the dead
                # letter queue, and records that decision on the job.
                if job.status == JobStatus.DEAD:
                    metrics.jobs_processed.labels(
                        task_name=job.task_name, status=JobStatus.DEAD.value
                    ).inc()
                else:
                    metrics.job_retries.labels(task_name=job.task_name).inc()
            finally:
                metrics.job_duration.labels(task_name=job.task_name).observe(
                    time.perf_counter() - started
                )
                self.last_heartbeat = datetime.now()
                self.current_job = None
