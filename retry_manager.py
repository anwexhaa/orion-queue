import random
from datetime import datetime, timedelta
from job import Job, JobStatus

BASE_DELAY = 0.5

class RetryManager:
    def __init__(self, delayed_queue, dlq):
        self.delayed_queue = delayed_queue
        self.dlq = dlq

    def handle_failure(self, job: Job, error: str):
        job.attempt += 1
        job.error = error

        if job.attempt >= job.max_retries:
            job.status = JobStatus.DEAD
            self.dlq.add(job)
            return

        # exponential backoff with jitter
        delay = BASE_DELAY * (2 ** job.attempt) + random.uniform(0, 0.5)
        job.next_retry_time = (
            datetime.now() + timedelta(seconds=delay)
        ).isoformat()
        job.status = JobStatus.PENDING
        self.delayed_queue.add(job)