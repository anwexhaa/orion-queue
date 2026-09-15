# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage — resolve dependencies into a prefix we can copy wholesale.
# Keeping pip and its build machinery out of the final image removes a large
# amount of surface area that never gets used at runtime.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Unbuffered stdout matters more than it looks: without it, Python holds log
# lines in a buffer and `kubectl logs` shows nothing during exactly the
# incident you are trying to diagnose.

# Run as a non-root user. A container running as root is one escape away from
# a root process on the node, and AKS pod security standards flag it.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin orion

COPY --from=builder /install /usr/local

WORKDIR /app
COPY --chown=orion:orion . .

USER orion

EXPOSE 8000

# For `docker compose`. Kubernetes uses its own probes and ignores this.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status == 200 else 1)"

# Default role is the API. The worker and scheduler deployments override this
# with their own command; all three share this one image.
CMD ["python", "api_main.py"]
