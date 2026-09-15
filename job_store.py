"""Persistence for job records.

`GET /status/{job_id}` reads whatever is under `job:{id}` in Redis. Previously
only the API ever wrote that key, at submission time, and the worker mutated
`job.status` purely in memory — so every job reported `pending` forever, including
ones that had finished successfully hours earlier.

Records carry a TTL. A queue that runs indefinitely would otherwise accumulate one
permanent Redis key per job ever submitted, which is a slow memory leak with no
upper bound.
"""

import json

import config
from job import Job


class JobStore:
    def __init__(self, client, ttl_seconds: int | None = None):
        self.client = client
        self.ttl = config.JOB_TTL_SECONDS if ttl_seconds is None else ttl_seconds

    def key(self, job_id: str) -> str:
        return f"job:{job_id}"

    def save(self, job: Job) -> None:
        """Write the current state of a job.

        Never raises: a worker must not fail a job it executed successfully
        just because the status write did not land. The job result is the
        product; this record is a convenience on top of it.
        """
        try:
            self.client.set(
                self.key(job.id), json.dumps(job.to_dict()), ex=self.ttl
            )
        except Exception as exc:  # noqa: BLE001 - deliberately broad
            print(f"[job_store] could not persist {job.id}: {exc}", flush=True)

    def get(self, job_id: str) -> dict | None:
        data = self.client.get(self.key(job_id))
        return json.loads(data) if data else None
