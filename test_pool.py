import redis, time
from priority_queue import RedisPriorityQueue
from delayed_queue import DelayedQueue
from dead_letter_queue import DeadLetterQueue
from retry_manager import RetryManager
from task_registry import TaskRegistry
from worker_pool import WorkerPool
from job import Job

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
q = RedisPriorityQueue()
dlq = DeadLetterQueue(r)
dq = DelayedQueue(r)
rm = RetryManager(dq, dlq)

@TaskRegistry.register('multiply')
def multiply(payload):
    a, b = payload['a'], payload['b']
    print(f'{a} x {b} = {a * b}')

pool = WorkerPool(n_workers=3, queue=q, retry_manager=rm)
pool.start()

for i in range(6):
    q.push(Job(task_name='multiply', payload={'a': i, 'b': i+1}))

time.sleep(1)
print('all done, workers alive:', len(pool.workers))
