"""API entrypoint.

Serves HTTP only. It does not run workers or the scheduler — those are
separate deployments, so that the API can be scaled for request volume and
the workers for queue backlog, independently.

For a single local process that runs all three, use `python main.py`.
"""

import uvicorn

import config

if __name__ == "__main__":
    print(f"[api] starting {config.summary()}", flush=True)
    uvicorn.run(
        "api:app",
        host=config.HTTP_HOST,
        port=config.HTTP_PORT,
        reload=False,
        access_log=False,  # probe traffic would otherwise dominate the logs
    )
