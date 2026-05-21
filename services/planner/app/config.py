from __future__ import annotations

import os


EXECUTOR_AGENT_CARD_URL = os.getenv(
    "EXECUTOR_AGENT_CARD_URL",
    "http://127.0.0.1:8001/agents/code-executor/.well-known/agent-card.json",
)
ANALYST_AGENT_CARD_URL = os.getenv(
    "ANALYST_AGENT_CARD_URL",
    "http://127.0.0.1:8002/agents/analyst/.well-known/agent-card.json",
)
PLANNER_DATABASE_URL = os.getenv(
    "PLANNER_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
EVENT_STREAM_BLOCK_MS = int(os.getenv("EVENT_STREAM_BLOCK_MS", "1000"))
EVENT_STREAM_MAXLEN = int(os.getenv("EVENT_STREAM_MAXLEN", "1000"))
PLANNER_RECONCILE_INTERVAL_SECONDS = float(os.getenv("PLANNER_RECONCILE_INTERVAL_SECONDS", "5.0"))
AUTH_ISSUER = os.getenv("AUTH_ISSUER", "a2a-local-auth")
AUTH_JWKS_URL = os.getenv("AUTH_JWKS_URL", "http://127.0.0.1:8000/.well-known/jwks.json")
PLANNER_API_AUDIENCE = os.getenv("PLANNER_API_AUDIENCE", "planner-service")
EXECUTOR_API_AUDIENCE = os.getenv("EXECUTOR_API_AUDIENCE", "code-executor-service")
ANALYST_API_AUDIENCE = os.getenv("ANALYST_API_AUDIENCE", "analyst-agent-service")
SERVICE_PRIVATE_KEY_PATH = os.getenv("SERVICE_PRIVATE_KEY_PATH", "dev_auth/private_key.pem")
SERVICE_TOKEN_KID = os.getenv("SERVICE_TOKEN_KID", "planner-local-dev-key")
SERVICE_TOKEN_SUBJECT = os.getenv("SERVICE_TOKEN_SUBJECT", "planner-service")
SERVICE_TOKEN_TTL_SECONDS = int(os.getenv("SERVICE_TOKEN_TTL_SECONDS", "300"))
