import redis
import json
from job import Job
from datetime import datetime

class RedisPriorityQueue:
    def __init__(self, host="localhost", port=6379, queue_key="task_queue", client=None):
        self.client = client if client else redis.Redis(host=host, port=port, decode_responses=True)
        self.queue_key = queue_key

    def push(self, job: Job):
        timestamp = datetime.fromisoformat(job.submit_time).timestamp()
        score = job.priority - (timestamp / 1e12)
        self.client.zadd(self.queue_key, {json.dumps(job.to_dict()): score})

    def pop(self) -> Job | None:
        result = self.client.zpopmax(self.queue_key, count=1)
        if not result:
            return None
        job_data, _ = result[0]
        return Job.from_dict(json.loads(job_data))

    def size(self) -> int:
        return self.client.zcard(self.queue_key)