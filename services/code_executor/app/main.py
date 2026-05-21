from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent

from services.code_executor.app.config import AUTH_ISSUER, AUTH_JWKS_URL, EVENT_STREAM_BLOCK_MS, EVENT_STREAM_MAXLEN, EXECUTOR_API_AUDIENCE, EXECUTOR_DATABASE_URL, EXECUTOR_QUEUE_LEASE_SECONDS, EXECUTOR_QUEUE_MAX_ATTEMPTS, EXECUTOR_QUEUE_POLL_SECONDS, EXECUTOR_QUEUE_RETRY_DELAY_SECONDS, EXECUTOR_WORKER_CONCURRENCY, REDIS_URL
from services.code_executor.app.executor_logic import TERMINAL_TASK_STATES, run_task
from services.code_executor.app.store import ExecutorTaskStore
from shared.auth import BearerTokenValidationError, JWKSBearerValidator, extract_bearer_token
from shared.a2a.models import AgentCapabilities, AgentCard, AgentSkill, JSONRPCResponse, MessageSendParams, Task, TaskCancelParams, TaskQueryParams, TaskStatus, task_to_result
from shared.event_bus import RedisEventBus


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task_store = ExecutorTaskStore(EXECUTOR_DATABASE_URL)
    await task_store.open()
    await task_store.initialize()
    event_bus = RedisEventBus(REDIS_URL, block_ms=EVENT_STREAM_BLOCK_MS, maxlen=EVENT_STREAM_MAXLEN)
    await event_bus.open()
    app.state.task_store = task_store
    app.state.event_bus = event_bus
    app.state.executor_validator = JWKSBearerValidator(AUTH_JWKS_URL, AUTH_ISSUER, EXECUTOR_API_AUDIENCE)
    app.state.queue_workers = [
        asyncio.create_task(queue_worker_loop(app, f"executor-worker-{index + 1}"))
        for index in range(EXECUTOR_WORKER_CONCURRENCY)
    ]

    try:
        yield
    finally:
        workers = list(app.state.queue_workers)
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await event_bus.close()
        await task_store.close()


app = FastAPI(title="Code Executor Service", lifespan=lifespan)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail, headers={"WWW-Authenticate": "Bearer"})


async def require_executor_auth(request: Request) -> dict:
    validator: JWKSBearerValidator = request.app.state.executor_validator
    try:
        token = extract_bearer_token(request.headers.get("authorization"))
        return await validator.validate(token)
    except BearerTokenValidationError as exc:
        raise _unauthorized(str(exc)) from exc


@app.get("/health")
async def health(request: Request) -> dict:
    task_store: ExecutorTaskStore = request.app.state.task_store
    event_bus: RedisEventBus = request.app.state.event_bus
    await task_store.ping()
    await event_bus.ping()
    return {
        "status": "ok",
        "service": "code-executor-service",
        "delivery_mode": "postgres-durable-queue",
        "event_transport": "redis-streams",
        "worker_concurrency": EXECUTOR_WORKER_CONCURRENCY,
        "queue_depth": await task_store.get_queue_depth(),
        "db_pool_open": not task_store.pool.closed,
    }


def build_agent_card(request: Request) -> AgentCard:
    base_url = str(request.base_url).rstrip("/")
    return AgentCard(
        name="code-executor-agent",
        description="Isolated code executor service reachable through A2A.",
        url=str(request.url_for("a2a_rpc")),
        capabilities=AgentCapabilities(streaming=True, pushNotifications=False),
        skills=[
            AgentSkill(
                id="code.execute",
                name="Code Execution",
                description="Executes scoped coding tasks delegated by the planner service.",
                examples=[
                    "Create a health endpoint",
                    "Draft a helper function",
                    "Prepare starter code for a feature",
                ],
            )
        ],
        metadata={
            "task_stream_url_template": f"{base_url}/agents/code-executor/tasks/{{task_id}}/events",
            "delivery_mode": "postgres-durable-queue",
            "event_transport": "redis-streams",
            "stream_correlation_field": "context_id",
            "worker_concurrency": EXECUTOR_WORKER_CONCURRENCY,
        },
    )


def extract_text(params: MessageSendParams) -> str:
    return "\n".join(part.text for part in params.message.parts if part.type == "text").strip()


async def queue_worker_loop(app: FastAPI, worker_id: str) -> None:
    task_store: ExecutorTaskStore = app.state.task_store
    event_bus: RedisEventBus = app.state.event_bus

    while True:
        lease = await task_store.claim_next_task(worker_id, EXECUTOR_QUEUE_LEASE_SECONDS)
        if lease is None:
            await asyncio.sleep(EXECUTOR_QUEUE_POLL_SECONDS)
            continue

        try:
            task = await task_store.get_task(lease.task_id)
            task.metadata["queue_attempt"] = lease.attempts
            task.metadata["queue_delivery"] = "postgres-durable-queue"
            await task_store.save_task(task)
            renew_task = asyncio.create_task(lease_renewer(task_store, lease.task_id, worker_id))
            try:
                await run_task(
                    task_store,
                    lease.task_id,
                    lambda current_task: publish_task_event(event_bus, current_task),
                )
            finally:
                renew_task.cancel()
                await asyncio.gather(renew_task, return_exceptions=True)
            final_task = await task_store.get_task(lease.task_id)
            if final_task.status.state == "canceled":
                await task_store.cancel_queue_task(lease.task_id)
            elif final_task.status.state == "completed":
                await task_store.complete_queue_task(lease.task_id)
            elif final_task.status.state == "failed":
                await task_store.fail_queue_task(lease.task_id, "Task marked failed during execution.")
            else:
                await task_store.retry_queue_task(
                    lease.task_id,
                    EXECUTOR_QUEUE_RETRY_DELAY_SECONDS,
                    "Task did not reach a terminal state before the worker released it.",
                )
        except asyncio.CancelledError:
            await task_store.release_lease(lease.task_id)
            raise
        except Exception as exc:
            error_text = str(exc)
            try:
                task = await task_store.get_task(lease.task_id)
                task.metadata["last_error"] = error_text
                if lease.attempts >= EXECUTOR_QUEUE_MAX_ATTEMPTS:
                    task.status = TaskStatus(state="failed", message="Executor exhausted the maximum queue retry attempts.")
                    await task_store.save_task(task)
                    await publish_task_event(event_bus, task)
                    await task_store.fail_queue_task(lease.task_id, error_text)
                else:
                    task.status = TaskStatus(state="submitted", message="Executor requeued the task after a worker failure.")
                    await task_store.save_task(task)
                    await publish_task_event(event_bus, task)
                    await task_store.retry_queue_task(
                        lease.task_id,
                        EXECUTOR_QUEUE_RETRY_DELAY_SECONDS,
                        error_text,
                    )
            except KeyError:
                await task_store.fail_queue_task(lease.task_id, error_text)


async def lease_renewer(task_store: ExecutorTaskStore, task_id: str, worker_id: str) -> None:
    renew_interval = max(1.0, EXECUTOR_QUEUE_LEASE_SECONDS / 3)
    while True:
        await asyncio.sleep(renew_interval)
        renewed = await task_store.renew_lease(task_id, worker_id, EXECUTOR_QUEUE_LEASE_SECONDS)
        if not renewed:
            return


async def task_event_stream(request: Request, task_id: str) -> AsyncIterator[ServerSentEvent]:
    task_store: ExecutorTaskStore = request.app.state.task_store
    event_bus: RedisEventBus = request.app.state.event_bus

    try:
        task = await task_store.get_task(task_id)
    except KeyError:
        yield ServerSentEvent(event="task_error", data={"detail": f"Unknown task id: {task_id}"}, id="0")
        return

    stream_key = event_bus.task_stream_key(task.context_id)

    try:
        async for event in event_bus.stream_events(stream_key):
            if await request.is_disconnected():
                return
            if event.fields.get("task_id") != task_id:
                continue
            yield ServerSentEvent(event=event.event, data=event.payload, id=event.id)
            if event.event == "task_complete":
                return
    except Exception as exc:
        logger.exception("Executor task stream failed for task %s", task_id)
        yield ServerSentEvent(event="task_error", data={"detail": str(exc)}, id="error")


async def publish_task_event(event_bus: RedisEventBus, task: Task) -> None:
    try:
        await event_bus.publish_task_event(task.model_dump(mode="json"), source_agent="code-executor-service")
    except Exception:
        logger.exception("Failed to publish executor task event for task %s", task.id)


@app.get("/agents/code-executor/.well-known/agent-card.json", response_model=AgentCard)
async def agent_card(request: Request) -> AgentCard:
    return build_agent_card(request)


@app.get("/agents/code-executor/tasks/{task_id}/events", name="task_events", response_class=EventSourceResponse)
async def stream_task_events(
    task_id: str,
    request: Request,
    _: dict = Depends(require_executor_auth),
) -> EventSourceResponse:
    return EventSourceResponse(task_event_stream(request, task_id))


@app.post("/agents/code-executor", name="a2a_rpc")
async def a2a_rpc(
    payload: dict,
    request: Request,
    _: dict = Depends(require_executor_auth),
) -> JSONRPCResponse:
    task_store: ExecutorTaskStore = request.app.state.task_store
    event_bus: RedisEventBus = request.app.state.event_bus
    method = payload.get("method")
    request_id = payload.get("id")
    params = payload.get("params", {})

    if method == "message/send":
        send_params = MessageSendParams.model_validate(params)
        client_request_id = str(send_params.metadata.get("request_id", "")).strip() or None
        task = Task(
            id=str(uuid4()),
            context_id=str(uuid4()),
            status=TaskStatus(state="submitted", message="Executor received the task and queued it for isolated processing."),
            metadata={
                "client_request_id": client_request_id,
                "user_request": send_params.metadata.get("original_user_request", extract_text(send_params)),
                "source_agent": send_params.metadata.get("source_agent", "unknown"),
                "executor_prompt": extract_text(send_params),
                "execution_plan": send_params.metadata.get("execution_plan", ""),
                "queue_delivery": "postgres-durable-queue",
            },
        )
        task, created = await task_store.create_task_if_absent(task)
        if created:
            await task_store.enqueue_task(task.id)
        await publish_task_event(event_bus, task)
        return JSONRPCResponse(id=request_id, result=task_to_result(task))

    if method == "tasks/get":
        query = TaskQueryParams.model_validate(params)
        try:
            task = await task_store.get_task(query.id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown task id: {query.id}") from exc
        return JSONRPCResponse(id=request_id, result=task_to_result(task))

    if method == "tasks/cancel":
        cancel = TaskCancelParams.model_validate(params)
        try:
            task = await task_store.get_task(cancel.id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown task id: {cancel.id}") from exc
        task.status = TaskStatus(state="canceled", message="Executor task canceled by planner.")
        await task_store.save_task(task)
        await publish_task_event(event_bus, task)
        await task_store.cancel_queue_task(task.id)
        return JSONRPCResponse(id=request_id, result=task_to_result(task))

    raise HTTPException(status_code=400, detail=f"Unsupported method: {method}")
