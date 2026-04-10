import redis
import json
from job import Job
from datetime import datetime

class DeadLetterQueue:
    def __init__(self, client: redis.Redis, dlq_key="dead_letter_queue"):
        self.client = client
        self.dlq_key = dlq_key

    def add(self, job: Job):
        record = job.to_dict()
        record["dead_at"] = datetime.now().isoformat()
        self.client.lpush(self.dlq_key, json.dumps(record))

    def get_all(self) -> list:
        records = self.client.lrange(self.dlq_key, 0, -1)
        return [json.loads(r) for r in records]

    def size(self) -> int:
        return self.client.llen(self.dlq_key)