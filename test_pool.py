"""Manual smoke check for the worker pool.

Not a pytest test — there are no assertions, it needs a live Redis, and it
sleeps. Everything is behind a main guard so that collecting this file does
not start a worker pool as a side effect:

    python test_pool.py
"""

import time

import config
from dead_letter_queue import DeadLetterQueue
from delayed_queue import DelayedQueue
from job import Job
from priority_queue import RedisPriorityQueue
from retry_manager import RetryManager
from task_registry import TaskRegistry
from worker_pool import WorkerPool


@TaskRegistry.register('multiply')
def multiply(payload):
    a, b = payload['a'], payload['b']
    print(f'{a} x {b} = {a * b}')


def main():
    r = config.redis_client()
    # An explicit key, so running this never touches the real queue.
    q = RedisPriorityQueue(client=r, queue_key='pool_check_queue')
    r.delete('pool_check_queue')

    dlq = DeadLetterQueue(r, dlq_key='pool_check_dlq')
    dq = DelayedQueue(r, delayed_key='pool_check_delayed')
    rm = RetryManager(dq, dlq)

    pool = WorkerPool(n_workers=3, queue=q, retry_manager=rm)
    pool.start()

    for i in range(6):
        q.push(Job(task_name='multiply', payload={'a': i, 'b': i + 1}))

    time.sleep(1)
    print('all done, workers alive:', len(pool.workers))


if __name__ == '__main__':
    main()
