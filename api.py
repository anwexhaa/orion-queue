from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
from job import Job, JobStatus, JobPriority
from priority_queue import RedisPriorityQueue
from dead_letter_queue import DeadLetterQueue

app = FastAPI()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
queue = RedisPriorityQueue(client=r)
dlq = DeadLetterQueue(client=r)

class SubmitRequest(BaseModel):
    task_name: str
    payload: dict
    priority: int = JobPriority.NORMAL.value
    max_retries: int = 3

@app.post("/submit")
def submit_job(req: SubmitRequest):
    job = Job(
        task_name=req.task_name,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries
    )
    # write status key BEFORE pushing to queue to avoid race condition
    r.set(f"job:{job.id}", json.dumps(job.to_dict()))
    queue.push(job)
    return {"job_id": job.id, "status": job.status.value}

@app.get("/status/{job_id}")
def get_status(job_id: str):
    data = r.get(f"job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    return json.loads(data)

@app.get("/metrics")
def get_metrics():
    return {
        "queue_size": queue.size(),
        "dead_jobs": dlq.size()
    }