from typing import Callable, Dict

class TaskRegistry:
    _registry: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(func: Callable):
            cls._registry[name] = func
            return func
        return decorator

    @classmethod
    def get(cls, name: str) -> Callable:
        if name not in cls._registry:
            raise ValueError(f"Task '{name}' not registered")
        return cls._registry[name]