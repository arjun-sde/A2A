from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent

from services.analyst_agent.app.config import ANALYST_API_AUDIENCE, ANALYST_DATABASE_URL, AUTH_ISSUER, AUTH_JWKS_URL, EVENT_STREAM_BLOCK_MS, EVENT_STREAM_MAXLEN, REDIS_URL
from services.analyst_agent.app.store import AnalystTaskStore
from shared.a2a.models import AgentCapabilities, AgentCard, AgentSkill, Artifact, JSONRPCResponse, MessageSendParams, Task, TaskCancelParams, TaskQueryParams, TaskStatus, TextPart, task_to_result
from shared.auth import BearerTokenValidationError, JWKSBearerValidator, extract_bearer_token
from shared.event_bus import RedisEventBus

TERMINAL_TASK_STATES = {"completed", "failed", "canceled"}
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task_store = AnalystTaskStore(ANALYST_DATABASE_URL)
    await task_store.open()
    await task_store.initialize()
    event_bus = RedisEventBus(REDIS_URL, block_ms=EVENT_STREAM_BLOCK_MS, maxlen=EVENT_STREAM_MAXLEN)
    await event_bus.open()
    app.state.task_store = task_store
    app.state.event_bus = event_bus
    app.state.analyst_validator = JWKSBearerValidator(AUTH_JWKS_URL, AUTH_ISSUER, ANALYST_API_AUDIENCE)
    try:
        yield
    finally:
        await event_bus.close()
        await task_store.close()


app = FastAPI(title="Analyst Agent Service", lifespan=lifespan)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail, headers={"WWW-Authenticate": "Bearer"})


async def require_analyst_auth(request: Request) -> dict:
    validator: JWKSBearerValidator = request.app.state.analyst_validator
    try:
        token = extract_bearer_token(request.headers.get("authorization"))
        return await validator.validate(token)
    except BearerTokenValidationError as exc:
        raise _unauthorized(str(exc)) from exc


def build_agent_card(request: Request) -> AgentCard:
    base_url = str(request.base_url).rstrip("/")
    return AgentCard(
        name="analyst-sub-agent",
        description="Conversational analyst sub-agent coordinated by a supervisor.",
        url=str(request.url_for("a2a_rpc")),
        capabilities=AgentCapabilities(streaming=True, pushNotifications=False),
        skills=[
            AgentSkill(
                id="analysis.delegate",
                name="Delegated Analysis",
                description="Requests missing supervisor guidance and then returns a scoped analysis result.",
                examples=[
                    "Analyze a feature request and ask for missing constraints",
                    "Return a concise delegated recommendation",
                ],
            )
        ],
        metadata={
            "task_stream_url_template": f"{base_url}/agents/analyst/tasks/{{task_id}}/events",
            "interaction_mode": "supervisor-sub-agent",
            "event_transport": "redis-streams",
            "stream_correlation_field": "context_id",
        },
    )


def extract_text(params: MessageSendParams) -> str:
    return "\n".join(part.text for part in params.message.parts if part.type == "text").strip()


def build_completion_artifact(task: Task, supervisor_followup: str) -> Artifact:
    user_request = str(task.metadata.get("user_request", ""))
    execution_plan = str(task.metadata.get("execution_plan", ""))
    return Artifact(
        name="analyst-result",
        parts=[
            TextPart(
                text=(
                    "Analyst sub-agent completed the delegated analysis.\n\n"
                    f"Original request:\n{user_request}\n\n"
                    f"Supervisor follow-up:\n{supervisor_followup}\n\n"
                    f"Execution plan:\n{execution_plan}\n\n"
                    "Recommended next step:\n"
                    "Proceed with the scoped implementation or planning action using the clarified constraints."
                )
            )
        ],
        metadata={"kind": "delegated-analysis"},
    )


async def task_event_stream(request: Request, task_id: str) -> AsyncIterator[ServerSentEvent]:
    task_store: AnalystTaskStore = request.app.state.task_store
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
        logger.exception("Analyst task stream failed for task %s", task_id)
        yield ServerSentEvent(event="task_error", data={"detail": str(exc)}, id="error")


async def publish_task_event(event_bus: RedisEventBus, task: Task) -> None:
    try:
        await event_bus.publish_task_event(task.model_dump(mode="json"), source_agent="analyst-agent-service")
    except Exception:
        logger.exception("Failed to publish analyst task event for task %s", task.id)


@app.get("/health")
async def health(request: Request) -> dict:
    task_store: AnalystTaskStore = request.app.state.task_store
    event_bus: RedisEventBus = request.app.state.event_bus
    await task_store.ping()
    await event_bus.ping()
    return {
        "status": "ok",
        "service": "analyst-agent",
        "interaction_mode": "supervisor-sub-agent",
        "event_transport": "redis-streams",
        "db_pool_open": not task_store.pool.closed,
    }


@app.get("/agents/analyst/.well-known/agent-card.json", response_model=AgentCard)
async def agent_card(request: Request) -> AgentCard:
    return build_agent_card(request)


@app.get("/agents/analyst/tasks/{task_id}/events", response_class=EventSourceResponse)
async def stream_task_events(
    task_id: str,
    request: Request,
    _: dict = Depends(require_analyst_auth),
) -> EventSourceResponse:
    return EventSourceResponse(task_event_stream(request, task_id))


@app.post("/agents/analyst", name="a2a_rpc")
async def a2a_rpc(payload: dict, request: Request, _: dict = Depends(require_analyst_auth)) -> JSONRPCResponse:
    task_store: AnalystTaskStore = request.app.state.task_store
    event_bus: RedisEventBus = request.app.state.event_bus
    method = payload.get("method")
    request_id = payload.get("id")
    params = payload.get("params", {})

    if method == "message/send":
        send_params = MessageSendParams.model_validate(params)
        text = extract_text(send_params)
        message_task_id = send_params.message.task_id
        message_context_id = send_params.message.context_id
        client_request_id = str(send_params.metadata.get("request_id", "")).strip() or None

        if message_task_id:
            try:
                task = await task_store.get_task(message_task_id)
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=f"Unknown task id: {message_task_id}") from exc
            if message_context_id and task.context_id != message_context_id:
                raise HTTPException(status_code=400, detail="Task context mismatch.")
            task.metadata["supervisor_followup"] = text
            task.status = TaskStatus(state="completed", message="Analyst completed the delegated sub-agent task.")
            task.artifacts = [build_completion_artifact(task, text)]
            await task_store.save_task(task)
            await publish_task_event(event_bus, task)
            return JSONRPCResponse(id=request_id, result=task_to_result(task))

        task = Task(
            id=str(uuid4()),
            context_id=str(uuid4()),
            status=TaskStatus(
                state="input_required",
                message="Analyst needs a short supervisor follow-up before continuing.",
            ),
            artifacts=[
                Artifact(
                    name="analyst-question",
                    parts=[
                        TextPart(
                            text=(
                                "Please confirm the expected output format and any critical success criteria "
                                "for this delegated analysis."
                            )
                        )
                    ],
                    metadata={"kind": "supervisor-question"},
                )
            ],
            metadata={
                "client_request_id": client_request_id,
                "user_request": send_params.metadata.get("original_user_request", text),
                "source_agent": send_params.metadata.get("source_agent", "unknown"),
                "analyst_prompt": text,
                "execution_plan": send_params.metadata.get("execution_plan", ""),
            },
        )
        task, _ = await task_store.create_task_if_absent(task)
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
        task.status = TaskStatus(state="canceled", message="Analyst task canceled by supervisor.")
        await task_store.save_task(task)
        await publish_task_event(event_bus, task)
        return JSONRPCResponse(id=request_id, result=task_to_result(task))

    raise HTTPException(status_code=400, detail=f"Unsupported method: {method}")
