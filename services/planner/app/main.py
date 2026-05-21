from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from services.planner.app.a2a_client import A2AClient
from services.planner.app.config import ANALYST_API_AUDIENCE, AUTH_ISSUER, AUTH_JWKS_URL, EVENT_STREAM_BLOCK_MS, EVENT_STREAM_MAXLEN, EXECUTOR_API_AUDIENCE, PLANNER_API_AUDIENCE, PLANNER_DATABASE_URL, PLANNER_RECONCILE_INTERVAL_SECONDS, REDIS_URL, SERVICE_PRIVATE_KEY_PATH, SERVICE_TOKEN_KID, SERVICE_TOKEN_SUBJECT, SERVICE_TOKEN_TTL_SECONDS
from services.planner.app.models import CreateRunRequest, PlannerRun
from services.planner.app.prompts import build_planner_prompt
from services.planner.app.store import PlannerStateStore
from services.planner.app.workflow import build_workflow
from shared.auth import BearerTokenValidationError, JWKSBearerValidator, ServiceTokenIssuer, build_local_jwks, extract_bearer_token
from shared.event_bus import RedisEventBus


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    state_store = PlannerStateStore(PLANNER_DATABASE_URL)
    await state_store.open()
    await state_store.initialize()
    event_bus = RedisEventBus(REDIS_URL, block_ms=EVENT_STREAM_BLOCK_MS, maxlen=EVENT_STREAM_MAXLEN)
    await event_bus.open()

    checkpointer_context = AsyncPostgresSaver.from_conn_string(PLANNER_DATABASE_URL)
    checkpointer = await checkpointer_context.__aenter__()
    await checkpointer.setup()

    token_issuer = ServiceTokenIssuer(
        SERVICE_PRIVATE_KEY_PATH,
        AUTH_ISSUER,
        SERVICE_TOKEN_KID,
        ttl_seconds=SERVICE_TOKEN_TTL_SECONDS,
    )
    planner_validator = JWKSBearerValidator(AUTH_JWKS_URL, AUTH_ISSUER, PLANNER_API_AUDIENCE)

    app.state.state_store = state_store
    app.state.token_issuer = token_issuer
    app.state.planner_validator = planner_validator
    app.state.local_jwks = build_local_jwks(SERVICE_PRIVATE_KEY_PATH, SERVICE_TOKEN_KID)
    def issue_downstream_token(url: str) -> str:
        audience = ANALYST_API_AUDIENCE if "/agents/analyst" in url or ":8002" in url else EXECUTOR_API_AUDIENCE
        return token_issuer.issue_token(audience, SERVICE_TOKEN_SUBJECT)
    app.state.a2a_client = A2AClient(auth_token_factory=issue_downstream_token)
    app.state.event_bus = event_bus
    app.state.workflow = build_workflow(checkpointer, state_store, app.state.a2a_client, event_bus)
    app.state.checkpointer_context = checkpointer_context
    app.state.run_tasks = {}
    app.state.reconciler_task = asyncio.create_task(reconcile_runs_loop(app))

    for run_id in await state_store.list_incomplete_run_ids():
        schedule_run(app, run_id)

    try:
        yield
    finally:
        app.state.reconciler_task.cancel()
        await asyncio.gather(app.state.reconciler_task, return_exceptions=True)
        run_tasks = list(app.state.run_tasks.values())
        for task in run_tasks:
            task.cancel()
        await asyncio.gather(*run_tasks, return_exceptions=True)
        await checkpointer_context.__aexit__(None, None, None)
        await event_bus.close()
        await state_store.close()


app = FastAPI(title="Planner Service", lifespan=lifespan)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail, headers={"WWW-Authenticate": "Bearer"})


async def require_planner_auth(request: Request) -> dict:
    validator: JWKSBearerValidator = request.app.state.planner_validator
    try:
        token = extract_bearer_token(request.headers.get("authorization"))
        return await validator.validate(token)
    except BearerTokenValidationError as exc:
        raise _unauthorized(str(exc)) from exc


async def process_run(app: FastAPI, run_id: str) -> None:
    state_store: PlannerStateStore = app.state.state_store
    event_bus: RedisEventBus = app.state.event_bus
    workflow = app.state.workflow

    try:
        run = await state_store.get_run(run_id)
        if run.status in {"completed", "failed"}:
            return
        await workflow.ainvoke(
            {
                "run_id": run.run_id,
                "user_request": run.user_request,
                "delegation_mode": run.delegation_mode,
            },
            {"configurable": {"thread_id": run.run_id}},
        )
    except Exception as exc:
        failed_run = await state_store.update_run(run_id, status="failed", error=str(exc))
        await publish_run_event(event_bus, failed_run)
    finally:
        app.state.run_tasks.pop(run_id, None)


def schedule_run(app: FastAPI, run_id: str) -> None:
    run_tasks: dict[str, asyncio.Task[None]] = app.state.run_tasks
    current = run_tasks.get(run_id)
    if current is not None and not current.done():
        return
    run_tasks[run_id] = asyncio.create_task(process_run(app, run_id))


async def reconcile_runs_loop(app: FastAPI) -> None:
    state_store: PlannerStateStore = app.state.state_store
    while True:
        await asyncio.sleep(PLANNER_RECONCILE_INTERVAL_SECONDS)
        for run_id in await state_store.list_incomplete_run_ids():
            schedule_run(app, run_id)


async def run_event_stream(request: Request, run_id: str) -> AsyncIterator[ServerSentEvent]:
    state_store: PlannerStateStore = request.app.state.state_store
    event_bus: RedisEventBus = request.app.state.event_bus

    try:
        await state_store.get_run(run_id)
    except KeyError:
        yield ServerSentEvent(event="run_error", data={"detail": f"Unknown run id: {run_id}"}, id="0")
        return

    try:
        async for event in event_bus.stream_events(event_bus.run_stream_key(run_id)):
            if await request.is_disconnected():
                return
            yield ServerSentEvent(event=event.event, data=event.payload, id=event.id)
            if event.event == "run_complete":
                return
    except Exception as exc:
        logger.exception("Planner run stream failed for run %s", run_id)
        yield ServerSentEvent(event="run_error", data={"detail": str(exc)}, id="error")


async def publish_run_event(event_bus: RedisEventBus, run: PlannerRun) -> None:
    try:
        await event_bus.publish_run_event(run.model_dump(mode="json"), source_agent="planner-service")
    except Exception:
        logger.exception("Failed to publish planner run event for run %s", run.run_id)


@app.post("/runs", response_model=PlannerRun, status_code=202)
async def create_run(
    request: CreateRunRequest,
    raw_request: Request,
    _: dict = Depends(require_planner_auth),
) -> PlannerRun:
    state_store: PlannerStateStore = raw_request.app.state.state_store
    event_bus: RedisEventBus = raw_request.app.state.event_bus

    run = PlannerRun(
        run_id=str(uuid4()),
        user_request=request.user_request,
        delegation_mode=request.delegation_mode,
        planner_prompt=build_planner_prompt(request.user_request),
        status="queued",
    )
    await state_store.create_run(run)
    await publish_run_event(event_bus, run)
    schedule_run(raw_request.app, run.run_id)
    return await state_store.get_run(run.run_id)


@app.get("/runs/{run_id}", response_model=PlannerRun)
async def get_run(run_id: str, request: Request, _: dict = Depends(require_planner_auth)) -> PlannerRun:
    state_store: PlannerStateStore = request.app.state.state_store
    try:
        return await state_store.get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run id: {run_id}") from exc


@app.get("/.well-known/jwks.json")
async def jwks(request: Request) -> dict:
    return dict(request.app.state.local_jwks)


@app.get("/health")
async def health(request: Request) -> dict:
    state_store: PlannerStateStore = request.app.state.state_store
    event_bus: RedisEventBus = request.app.state.event_bus
    await state_store.ping()
    await event_bus.ping()
    return {
        "status": "ok",
        "service": "planner-service",
        "delegation_modes": ["tool", "sub_agent"],
        "event_transport": "redis-streams",
        "db_pool_open": not state_store.pool.closed,
        "active_run_tasks": len(request.app.state.run_tasks),
        "reconciler_running": not request.app.state.reconciler_task.done(),
    }


@app.get("/runs/{run_id}/events", response_class=EventSourceResponse)
async def stream_run_events(
    run_id: str,
    request: Request,
    _: dict = Depends(require_planner_auth),
) -> EventSourceResponse:
    return EventSourceResponse(run_event_stream(request, run_id))
