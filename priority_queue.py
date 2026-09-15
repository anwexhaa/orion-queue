import json
from datetime import datetime

import config
from job import Job

class RedisPriorityQueue:
    def __init__(self, host=None, port=None, queue_key=None, client=None):
        # Defaults now come from the environment rather than being baked in,
        # so the same code runs against localhost on a laptop and against a
        # Redis Service in the cluster. Passing host/port/queue_key
        # explicitly still works and still wins.
        if client:
            self.client = client
        else:
            overrides = {}
            if host is not None:
                overrides["host"] = host
            if port is not None:
                overrides["port"] = port
            self.client = config.redis_client(**overrides)

        self.queue_key = queue_key if queue_key is not None else config.QUEUE_KEY

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