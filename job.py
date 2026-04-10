from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime
import uuid

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"

class JobPriority(Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20

@dataclass
class Job:
    task_name: str
    payload: dict
    priority: int = JobPriority.NORMAL.value
    max_retries: int = 3
    attempt: int = 0
    status: JobStatus = JobStatus.PENDING
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    submit_time: str = field(default_factory=lambda: datetime.now().isoformat())
    next_retry_time: str = field(default_factory=lambda: datetime.now().isoformat())
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "task_name": self.task_name,
            "payload": self.payload,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "attempt": self.attempt,
            "status": self.status.value,
            "submit_time": self.submit_time,
            "next_retry_time": self.next_retry_time,
            "error": self.error
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        data["status"] = JobStatus(data["status"])
        return cls(**data)