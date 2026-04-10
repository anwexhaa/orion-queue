import uuid
import time
import threading
from datetime import datetime, timedelta
from worker import Worker

HEARTBEAT_TIMEOUT = timedelta(seconds=10)

class WorkerPool:
    def __init__(self, n_workers: int, queue, retry_manager):
        self.queue = queue
        self.retry_manager = retry_manager
        self.workers: list[Worker] = []
        self.n_workers = n_workers

    def start(self):
        for _ in range(self.n_workers):
            self._spawn_worker()
        self._start_monitor()

    def _spawn_worker(self):
        w = Worker(str(uuid.uuid4()), self.queue, self.retry_manager)
        w.start()
        self.workers.append(w)

    def _start_monitor(self):
        t = threading.Thread(target=self._monitor, daemon=True)
        t.start()

    def _monitor(self):
        while True:
            now = datetime.now()
            for i, worker in enumerate(self.workers):
                if now - worker.last_heartbeat > HEARTBEAT_TIMEOUT:
                    if worker.current_job:
                        worker.current_job.status = "pending"
                        self.queue.push(worker.current_job)
                    worker.stop()
                    new_worker = Worker(
                        str(uuid.uuid4()), self.queue, self.retry_manager
                    )
                    new_worker.start()
                    self.workers[i] = new_worker
            time.sleep(1)

    def scale_up(self, n: int):
        for _ in range(n):
            self._spawn_worker()

    def scale_down(self, n: int):
        for _ in range(min(n, len(self.workers))):
            w = self.workers.pop()
            w.stop()