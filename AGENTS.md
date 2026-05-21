# A2A Repo Rules

## Canonical Docs
- Architecture and runtime shape: `ARCHITECTURE.md`
- Hands-on usage and local runbook: `README.md`
- Shared env contract: `.env.example`
- Service-specific env contracts:
  - `services/planner/.env.example`
  - `services/code_executor/.env.example`
  - `services/analyst_agent/.env.example`

## Keep This File Lean
- Keep only repo-wide rules and invariants here.
- Put detailed setup, behavior, and integration notes in `README.md` or `ARCHITECTURE.md`.
- When service boundaries, context contracts, or A2A behavior change, update the canonical docs in the same change.

## Non-Negotiable Invariants
- This repo demonstrates two A2A interaction patterns:
  - remote agent exposed like a tool
  - supervisor orchestrates a sub-agent
- `planner-service` is the only supervisor/orchestrator in the current repo.
- Downstream agents must not assume access to planner-only state unless the planner sends it explicitly.
- Agent-to-agent communication stays over HTTP A2A surfaces:
  - Agent Card discovery
  - JSON-RPC task submission
  - SSE task status streaming
- Planner workflow state is checkpointed in Postgres through LangGraph.
- Executor work delivery is durable and queue-backed in Postgres with bounded worker concurrency.
- Auth uses bearer tokens validated against JWKS; local dev uses the planner-hosted JWKS flow.
- Health endpoints must remain available on each service for probes and operational checks.

## Service Ownership
- `services/planner/`
  Owns request intake, orchestration, LangGraph checkpointing, and delegated-run lifecycle.
- `services/code_executor/`
  Owns tool-style delegated execution and durable queue-backed worker processing.
- `services/analyst_agent/`
  Owns conversational sub-agent behavior where the supervisor can resume the same delegated task.
- `shared/`
  Owns A2A models and shared auth helpers.

## Documentation Discipline
- Keep `ARCHITECTURE.md` focused on runtime boundaries, state ownership, and scaling shape.
- Keep `README.md` focused on usage, examples, and run instructions.
- Do not leave important agent-context assumptions only in chat history.
