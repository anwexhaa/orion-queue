from task_registry import TaskRegistry
import time

@TaskRegistry.register("greet")
def greet(payload: dict):
    name = payload.get("name", "stranger")
    print(f"[greet] Hello, {name}!")

@TaskRegistry.register("add")
def add(payload: dict):
    result = payload["a"] + payload["b"]
    print(f"[add] {payload['a']} + {payload['b']} = {result}")

@TaskRegistry.register("slow_task")
def slow_task(payload: dict):
    duration = payload.get("duration", 2)
    print(f"[slow_task] starting, will take {duration}s")
    time.sleep(duration)
    print(f"[slow_task] done")

@TaskRegistry.register("failing_task")
def failing_task(payload: dict):
    raise ValueError("this task always fails — for testing retry logic")