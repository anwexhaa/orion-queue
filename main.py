import redis
import uvicorn
import threading
from priority_queue import RedisPriorityQueue
from delayed_queue import DelayedQueue, DelayedQueueScheduler
from dead_letter_queue import DeadLetterQueue
from retry_manager import RetryManager
from worker_pool import WorkerPool
import tasks  # registers all tasks on import

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = RedisPriorityQueue(client=r)
dlq = DeadLetterQueue(client=r)
dq = DelayedQueue(client=r)
rm = RetryManager(dq, dlq)

scheduler = DelayedQueueScheduler(dq, queue)
scheduler.start()

pool = WorkerPool(n_workers=4, queue=queue, retry_manager=rm)
pool.start()

print("Workers started, scheduler running")

uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)