# A2A LangGraph Multi-Agent Demo

This repository demonstrates two A2A communication patterns end to end:

- `planner-service`: supervisor/orchestrator agent
- `code-executor-service`: remote agent exposed like a specialized tool
- `analyst-agent-service`: conversational subordinate agent coordinated by the planner

The planner does not import downstream agent logic. It discovers downstream agents through Agent Cards, invokes them through A2A over HTTP, and consumes live task status over SSE backed by Redis Streams.

The runtime is intentionally runnable:

- FastAPI apps served by `uvicorn`
- Python `3.13`
- Postgres-backed service state
- LangGraph Postgres checkpointer for planner workflow persistence
- bearer-token auth validated against a local JWKS URL
- Postgres-backed durable executor queue for backpressure
- Redis Streams for live run and task event delivery
- repo-level agent context docs in `AGENTS.md` and `ARCHITECTURE.md`

The current planner runtime is intentionally pragmatic:

- planner API and workflow runner share one process/pod for now
- planner should stay single-replica until distributed run claiming is added
- a lightweight in-process reconciler periodically re-schedules non-terminal persisted runs

## What Changed

Compared with the earlier skeleton:

- planner to executor status tracking moved from `tasks/get` polling to SSE
- planner run state moved from SQLite to Postgres
- executor task state moved from SQLite to Postgres
- planner LangGraph workflow now compiles with `AsyncPostgresSaver`
- planner and downstream agent SSE endpoints are now backed by Redis Streams instead of DB polling
- Docker images now use Python `3.13`
- planner hosts a local JWKS endpoint and mints executor service tokens
- executor delivery moved to a bounded Postgres durable queue with leases and retries
- analyst sub-agent service added for supervisor-orchestrated delegated conversations
- health endpoints added for every service

## Layout

```text
shared/
  a2a/
    models.py
services/
  planner/
    app/
      a2a_client.py
      config.py
      main.py
      models.py
      prompts.py
      store.py
      workflow.py
    Dockerfile
    requirements.txt
  code_executor/
    app/
      config.py
      executor_logic.py
      main.py
      store.py
    Dockerfile
    requirements.txt
  analyst_agent/
    app/
      config.py
      main.py
      store.py
    Dockerfile
    requirements.txt
deploy/
  k8s/
    analyst-agent.yaml
    planner.yaml
    code-executor.yaml
docker-compose.yml
```

## End-to-End Flow

1. A client calls `POST /runs` on the planner service.
2. The planner stores the run in Postgres.
3. The planner LangGraph workflow starts with `thread_id = run_id`.
4. LangGraph checkpoints each workflow step in Postgres through `AsyncPostgresSaver`.
5. The planner chooses a delegation mode:
   - `tool`
   - `sub_agent`
6. The planner discovers the downstream agent through its Agent Card.
7. The planner sends `message/send` to the downstream A2A endpoint.
8. Tool mode:
   - code executor stores the task in Postgres and enqueues it in a durable Postgres queue
   - bounded executor workers claim queued tasks with leases and execute them
   - executor publishes task events to a Redis Stream keyed by `context_id`
9. Sub-agent mode:
   - analyst agent may first return `input_required`
   - planner sends a follow-up on the same `task_id` and `context_id`
   - analyst publishes task events to a Redis Stream keyed by `context_id`
10. The planner opens the downstream task SSE stream.
11. The downstream agent replays and tails the Redis-backed task stream.
12. The planner updates its own run state, publishes run events to a Redis Stream keyed by `run_id`, and finalizes the response.
13. Clients can watch planner-side progress through `GET /runs/{run_id}/events`.

## Communication Model

There are two distinct HTTP patterns in this repo:

1. A2A invocation between services
   - Agent Card discovery over `GET`
   - JSON-RPC invocation over `POST`
   - SSE status stream over `GET`, backed by Redis Streams
2. Client-facing run status stream
   - Browser or CLI connects to planner SSE endpoint backed by the planner run stream

### Inter-agent API calls

Planner to downstream agents:

- `GET /agents/code-executor/.well-known/agent-card.json`
- `GET /agents/analyst/.well-known/agent-card.json`
- `POST /agents/code-executor` with JSON-RPC `message/send`
- `POST /agents/analyst` with JSON-RPC `message/send`
- `GET /agents/code-executor/tasks/{task_id}/events` for SSE updates
- `GET /agents/analyst/tasks/{task_id}/events` for SSE updates

The older `tasks/get` JSON-RPC endpoint still exists for direct inspection and fallback, but the planner path no longer relies on polling.

## Repo Context Docs

The repo now keeps top-level coding-agent context in:

- `AGENTS.md`
- `ARCHITECTURE.md`

This mirrors the style used in the reference Harness-style repo you pointed to: keep repo-wide rules lean, keep architecture and ownership explicit, and avoid leaving important integration assumptions only in chat history.

## Backpressure

The executor now uses a durable Postgres queue internally:

- `message/send` persists the task and enqueues a queue row
- a fixed number of executor workers claim jobs with leases
- retries are bounded by `EXECUTOR_QUEUE_MAX_ATTEMPTS`
- failed workers release or requeue work instead of dropping it
- backlog stays in Postgres rather than exploding executor fanout

This gives the repo queue semantics similar to a managed service like GCP Pub/Sub, while staying runnable locally with only Postgres. If you later want Pub/Sub specifically, the executor queue layer is the seam to replace.

## Auth Model

This repo now uses bearer-token auth with JWKS validation:

- planner client endpoints require a bearer token with audience `planner-service`
- planner publishes `GET /.well-known/jwks.json`
- planner validates inbound tokens against `AUTH_JWKS_URL`
- planner mints its own short-lived service token for downstream agent calls
- executor and analyst agent validate those service tokens against the planner JWKS URL

This is a local-demo trust model, not a production identity system. In production you would typically replace this with workload identity plus an external issuer.

## Current Runtime Shape

- planner API and planner workflow runner currently live in the same process/pod
- accepted runs are persisted first and then scheduled locally
- on startup, the planner re-schedules any non-terminal persisted runs it finds
- the planner also periodically reconciles non-terminal runs in the same process
- executor and analyst task submission are atomically idempotent when the same delegated request id is retried
- executor queue leases are renewed while workers are still processing tasks
- live task and run updates flow through Redis Streams keyed by correlation id

## Persistence

Planner-owned state in Postgres:

- `run_id`
- `user_request`
- `planner_prompt`
- `execution_plan`
- `executor_prompt`
- `status`
- `executor_task_id`
- `executor_task_status`
- `final_response`

Executor-owned state in Postgres:

- `task_id`
- `task.status`
- serialized task payload
- durable queue rows for claiming, leasing, retries, and backlog

LangGraph checkpoint state in Postgres:

- workflow checkpoints
- thread history keyed by `thread_id`
- pending writes needed for durable execution

For the planner, `run_id` is used as the LangGraph `thread_id`.

## Event Transport

For this demo, the durable/live split is:

- Postgres for durable state, checkpoints, retries, and final artifacts
- Redis Streams for live run and task events keyed by `run_id` or `context_id`
- SSE as the edge transport exposed to planners and clients

That keeps the external HTTP model simple while avoiding per-stream database polling for live updates.

## Why Postgres Queue Instead Of Pub/Sub Here

GCP Pub/Sub is a reasonable production option for backpressure and decoupling, but it is not the best first implementation for this repo because:

- this repo is intended to run locally with minimal dependencies
- service state already lives in Postgres
- the queue semantics needed for the demo are simple: durable enqueue, bounded workers, lease/retry

So the repo now uses a Postgres durable queue as the local runnable baseline.

## Local Run

### Option 1: Docker Compose

Run the full stack:

```bash
docker compose up --build
```

Create a run:

```bash
TOKEN=$(python3.13 scripts/mint_dev_token.py --aud planner-service)
curl -X POST http://127.0.0.1:8000/runs \
  -H "authorization: Bearer $TOKEN" \
  -H "content-type: application/json" \
  -d '{"user_request":"Create a FastAPI health endpoint and explain the starter code.","delegation_mode":"tool"}'
```

Create a supervisor/sub-agent run:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "authorization: Bearer $TOKEN" \
  -H "content-type: application/json" \
  -d '{"user_request":"Analyze the implementation approach and tell me what the supervisor should do next.","delegation_mode":"sub_agent"}'
```

Watch planner status via SSE:

```bash
curl -N \
  -H "authorization: Bearer $TOKEN" \
  http://127.0.0.1:8000/runs/<run_id>/events
```

Inspect the final run record:

```bash
curl \
  -H "authorization: Bearer $TOKEN" \
  http://127.0.0.1:8000/runs/<run_id>
```

### Option 2: Run with local Uvicorn processes

Create a Python `3.13` environment and install dependencies:

```bash
python3.13 -m venv env
source env/bin/activate
pip install -r ./services/planner/requirements.txt
pip install -r ./services/code_executor/requirements.txt
pip install -r ./services/analyst_agent/requirements.txt
```

Start Postgres separately, then export:

```bash
export PLANNER_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable'
export EXECUTOR_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable'
export ANALYST_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/a2a?sslmode=disable'
export REDIS_URL='redis://127.0.0.1:6379/0'
export EXECUTOR_AGENT_CARD_URL='http://127.0.0.1:8001/agents/code-executor/.well-known/agent-card.json'
export ANALYST_AGENT_CARD_URL='http://127.0.0.1:8002/agents/analyst/.well-known/agent-card.json'
export PLANNER_RECONCILE_INTERVAL_SECONDS='5.0'
```

Start the executor:

```bash
PYTHONPATH=. uvicorn services.code_executor.app.main:app --reload --port 8001
```

Start the planner:

```bash
PYTHONPATH=. uvicorn services.planner.app.main:app --reload --port 8000
```

Start the analyst sub-agent:

```bash
PYTHONPATH=. uvicorn services.analyst_agent.app.main:app --reload --port 8002
```

Mint a local planner token:

```bash
python3.13 scripts/mint_dev_token.py --aud planner-service
```

## Kubernetes

Minimal manifests are included in `deploy/k8s/`:

- `planner.yaml`
- `code-executor.yaml`
- `analyst-agent.yaml`

These manifests assume an external Postgres-compatible database such as GCP Cloud SQL, an external Redis instance for streams, keep downstream Agent Card URLs internal to the cluster, and include probe-ready `/health` endpoints.

For now, keep `planner-service` at a single replica. The executor and analyst services can be scaled independently, but planner-side distributed run claiming is intentionally deferred.

Health endpoints:

- planner: `GET /health`
- code executor: `GET /health`
- analyst agent: `GET /health`

## Notes

- This repo now demonstrates both:
  - agent exposed as a tool-like remote worker
  - supervisor orchestrates a conversational sub-agent
- Executor backpressure now uses a durable Postgres queue with bounded workers.
