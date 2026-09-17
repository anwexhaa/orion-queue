"""Requeue jobs whose worker stopped renewing its lease.

A lease expires when the worker holding it has stopped renewing it — the
process was killed, the pod was evicted, the node went away. Every expiry is
therefore evidence that a worker died holding work, which is exactly the event
game day 1 found went entirely unreported. It is counted, and it alerts.

Safe to run in more than one process: requeueing happens inside one Redis
script, so a job can only be taken back once. It runs in the scheduler because
that process already exists and is otherwise idle, not because it must be
a singleton.
"""

import threading
import time

import config
import metrics


class LeaseReaper:
    def __init__(self, queue, interval=None, batch=100):
        self.queue = queue
        self.interval = config.REAP_INTERVAL if interval is None else interval
        self.batch = batch
        self._thread = threading.Thread(target=self._run, daemon=True, name="reaper")

    def reap_once(self) -> int:
        total = 0
        # Drain in batches so a large backlog of expired leases - say, after
        # a whole node was lost - is recovered in one pass, without holding
        # Redis in a single long script.
        while True:
            n = self.queue.reap_expired(limit=self.batch)
            total += n
            if n < self.batch:
                break
        if total:
            metrics.leases_expired_total.inc(total)
            print(f"[reaper] requeued {total} job(s) from expired leases", flush=True)
        return total

    def start(self):
        self._thread.start()

    def _run(self):
        while True:
            try:
                self.reap_once()
                metrics.jobs_in_flight.set(self.queue.in_flight())
            except Exception as exc:  # noqa: BLE001 - deliberately broad
                # This loop is how crashed work comes back. If it dies, lost
                # jobs stay lost, so a failure costs one cycle and no more.
                print(f"[reaper] cycle failed: {exc}", flush=True)
            time.sleep(self.interval)
