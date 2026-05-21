from __future__ import annotations

import os


EXECUTOR_DATABASE_URL = os.getenv(
    "EXECUTOR_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
EVENT_STREAM_BLOCK_MS = int(os.getenv("EVENT_STREAM_BLOCK_MS", "1000"))
EVENT_STREAM_MAXLEN = int(os.getenv("EVENT_STREAM_MAXLEN", "1000"))
EXECUTOR_STEP_DELAY_SECONDS = float(os.getenv("EXECUTOR_STEP_DELAY_SECONDS", "1.0"))
EXECUTOR_QUEUE_POLL_SECONDS = float(os.getenv("EXECUTOR_QUEUE_POLL_SECONDS", "0.5"))
EXECUTOR_QUEUE_LEASE_SECONDS = float(os.getenv("EXECUTOR_QUEUE_LEASE_SECONDS", "30.0"))
EXECUTOR_QUEUE_RETRY_DELAY_SECONDS = float(os.getenv("EXECUTOR_QUEUE_RETRY_DELAY_SECONDS", "2.0"))
EXECUTOR_WORKER_CONCURRENCY = int(os.getenv("EXECUTOR_WORKER_CONCURRENCY", "2"))
EXECUTOR_QUEUE_MAX_ATTEMPTS = int(os.getenv("EXECUTOR_QUEUE_MAX_ATTEMPTS", "3"))
AUTH_ISSUER = os.getenv("AUTH_ISSUER", "a2a-local-auth")
AUTH_JWKS_URL = os.getenv("AUTH_JWKS_URL", "http://127.0.0.1:8000/.well-known/jwks.json")
EXECUTOR_API_AUDIENCE = os.getenv("EXECUTOR_API_AUDIENCE", "code-executor-service")
