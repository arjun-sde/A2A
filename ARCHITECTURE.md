# Architecture

This repository is a small multi-service A2A system designed to demonstrate two different agent-to-agent patterns while staying runnable locally.

## Interaction Patterns

### 1. Agent Exposed As A Tool
- `planner-service` acts as the supervisor.
- `code-executor-service` is exposed through A2A as a remote specialized capability.
- The planner submits a delegated task and waits for terminal completion.
- Internally, the executor uses a durable Postgres queue plus bounded workers for backpressure.

### 2. Supervisor Orchestrates A Sub-Agent
- `planner-service` still acts as the supervisor.
- `analyst-agent-service` behaves like a conversational subordinate agent.
- The planner starts a delegated task.
- The analyst can return `input_required`.
- The planner sends a follow-up on the same task/context.
- The analyst returns a completed delegated result.

## Runtime Flow

### Planner
- Accepts `POST /runs`.
- Persists run state in Postgres.
- Compiles a LangGraph workflow with Postgres checkpointing.
- Chooses delegation mode:
  - `tool`
  - `sub_agent`
- Discovers the downstream agent by Agent Card.
- Sends the delegated task over JSON-RPC.
- Tracks task completion through SSE backed by Redis Streams.
- Publishes planner run events to a Redis Stream keyed by `run_id`.
- Marks the planner run complete once the delegated task reaches a terminal state.

### Code Executor
- Accepts delegated A2A tasks over JSON-RPC.
- Persists task state in Postgres.
- Enqueues work into a durable Postgres queue table.
- Bounded workers lease and process queue entries.
- Publishes task events to a Redis Stream keyed by `context_id`.
- SSE streams the current task state back to the supervisor from that stream.

### Analyst Sub-Agent
- Accepts delegated A2A tasks over JSON-RPC.
- Persists task state in Postgres.
- First response can be `input_required`.
- Supervisor sends a follow-up using the same `task_id` and `context_id`.
- Publishes task events to a Redis Stream keyed by `context_id`.
- SSE streams the updated task state back to the supervisor from that stream.

## State Ownership

### Planner-Owned State
- `run_id`
- `user_request`
- `delegation_mode`
- `planner_prompt`
- `execution_plan`
- delegated prompt
- delegated task id and status
- final response

### Code Executor-Owned State
- executor `task_id`
- executor task payload
- queue state
- lease/retry metadata

### Analyst-Owned State
- analyst `task_id`
- analyst task payload
- supervisor follow-up state

State is not shared implicitly across services. The planner passes only the scoped context it chooses to send in the A2A message and metadata.

## Context Contract

The current A2A context handoff is explicit and narrow:
- message body contains the role-specific delegated prompt
- metadata includes:
  - `source_agent`
  - `original_user_request`
  - `execution_plan`
  - `delegation_mode`

This keeps planner-only orchestration state private unless intentionally handed downstream.

## Scaling Shape

### Planner
- Today the planner process both accepts requests and runs the workflow.
- Near term, the planner API and workflow runner stay in the same pod/container.
- Near term, planner should stay single-replica because distributed run claiming is not implemented yet.
- Future production scaling should split this into:
  - planner API deployment
  - planner worker deployment
- Planner API should then scale with HPA on CPU/memory.
- Planner worker should scale based on durable backlog or reconciler pressure.

### Code Executor
- The code executor is already shaped for queue-depth-driven scaling.
- No KEDA objects are included in this repo, but the service is designed so KEDA can scale executor replicas using queue backlog semantics.
- Worker concurrency is controlled per pod with env vars.

### Analyst Sub-Agent
- The analyst service is lightweight and stateful only through Postgres.
- It can scale horizontally once task continuation/idempotency rules are hardened further.

## Operational Surfaces

Each service exposes:
- Agent Card endpoint
- A2A RPC endpoint
- SSE task event endpoint
- `GET /health`

Live event transport uses:
- Redis Streams for internal run/task event delivery
- `run_id` as the planner run stream correlation key
- `context_id` as the delegated task stream correlation key

Local auth uses:
- planner-hosted JWKS
- bearer tokens for client -> planner
- planner-issued bearer tokens for planner -> downstream agent calls

Deployment assumptions:
- Kubernetes manifests assume an external Postgres-compatible database such as GCP Cloud SQL.
- Workload identity is a planned follow-up and is not yet implemented in this repo.

## Current Known Limits
- Planner execution still runs inside API pods rather than a separate worker deployment.
- Planner recovery is process-local: a periodic reconciler re-schedules non-terminal runs, but there is still no distributed ownership across planner replicas.
- SSE stays as the HTTP transport, but it now replays and tails Redis streams rather than polling Postgres.
- Downstream auth audience selection is still based on URL heuristics rather than explicit agent metadata.
- The repo demonstrates the two A2A patterns, but the sub-agent loop is still intentionally minimal.
