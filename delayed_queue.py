import redis
import json
import time
import threading
from job import Job, JobStatus
from datetime import datetime

class DelayedQueue:
    def __init__(self, client: redis.Redis, delayed_key="delayed_queue"):
        self.client = client
        self.delayed_key = delayed_key

    def add(self, job: Job):
        score = datetime.fromisoformat(job.next_retry_time).timestamp()
        self.client.zadd(self.delayed_key, {json.dumps(job.to_dict()): score})

    def poll(self, main_queue) -> int:
        now = datetime.now().timestamp()
        due_jobs = self.client.zrangebyscore(self.delayed_key, "-inf", now)
        if not due_jobs:
            return 0
        for job_data in due_jobs:
            job = Job.from_dict(json.loads(job_data))
            job.status = JobStatus.PENDING
            main_queue.push(job)
            self.client.zrem(self.delayed_key, job_data)
        return len(due_jobs)

class DelayedQueueScheduler:
    def __init__(self, delayed_queue: DelayedQueue, main_queue, poll_interval=0.5):
        self.delayed_queue = delayed_queue
        self.main_queue = main_queue
        self.poll_interval = poll_interval
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        while True:
            self.delayed_queue.poll(self.main_queue)
            time.sleep(self.poll_interval)