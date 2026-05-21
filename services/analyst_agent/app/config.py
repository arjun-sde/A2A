from __future__ import annotations

import os


ANALYST_DATABASE_URL = os.getenv(
    "ANALYST_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
EVENT_STREAM_BLOCK_MS = int(os.getenv("EVENT_STREAM_BLOCK_MS", "1000"))
EVENT_STREAM_MAXLEN = int(os.getenv("EVENT_STREAM_MAXLEN", "1000"))
AUTH_ISSUER = os.getenv("AUTH_ISSUER", "a2a-local-auth")
AUTH_JWKS_URL = os.getenv("AUTH_JWKS_URL", "http://127.0.0.1:8000/.well-known/jwks.json")
ANALYST_API_AUDIENCE = os.getenv("ANALYST_API_AUDIENCE", "analyst-agent-service")
