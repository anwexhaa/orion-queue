import threading
import time
from datetime import datetime
from job import Job, JobStatus
from task_registry import TaskRegistry

class Worker:
    def __init__(self, worker_id: str, queue, retry_manager):
        self.id = worker_id
        self.queue = queue
        self.retry_manager = retry_manager
        self.last_heartbeat = datetime.now()
        self.current_job: Job | None = None
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._running = False

    def _run(self):
        while self._running:
            job = self.queue.pop()
            if not job:
                self.last_heartbeat = datetime.now()
                time.sleep(0.1)
                continue

            self.current_job = job
            job.status = JobStatus.RUNNING

            try:
                task_fn = TaskRegistry.get(job.task_name)
                task_fn(job.payload)
                job.status = JobStatus.DONE
            except Exception as e:
                print(f"[worker {self.id[:8]}] job {job.task_name} failed: {e}")
                self.retry_manager.handle_failure(job, str(e))
            finally:
                self.last_heartbeat = datetime.now()
                self.current_job = None