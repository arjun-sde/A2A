from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from services.code_executor.app.config import EXECUTOR_STEP_DELAY_SECONDS
from services.code_executor.app.store import ExecutorTaskStore
from shared.a2a.models import Artifact, Task, TaskStatus, TextPart

TERMINAL_TASK_STATES = {"completed", "failed", "canceled"}


def build_execution_artifact(user_request: str) -> Artifact:
    return Artifact(
        name="code-execution-result",
        parts=[
            TextPart(
                text=(
                    "Executor completed the isolated coding task.\n\n"
                    f"Scoped request:\n{user_request}\n\n"
                    "Starter code:\n"
                    f"{select_code_snippet(user_request)}"
                )
            )
        ],
        metadata={"kind": "execution-summary"},
    )


def select_code_snippet(user_request: str) -> str:
    normalized = user_request.lower()
    if "fastapi" in normalized or "endpoint" in normalized:
        return (
            "```python\n"
            "from fastapi import APIRouter\n\n"
            "router = APIRouter()\n\n"
            "@router.get('/health')\n"
            "def health() -> dict[str, str]:\n"
            "    return {'status': 'ok'}\n"
            "```"
        )
    if "function" in normalized or "helper" in normalized:
        return (
            "```python\n"
            "def helper(value: str) -> str:\n"
            "    return value.strip().lower()\n"
            "```"
        )
    return (
        "```python\n"
        "def implement_feature() -> None:\n"
        "    print('Apply the requested code change here.')\n"
        "```"
    )


async def run_task(
    task_store: ExecutorTaskStore,
    task_id: str,
    publish_task_event: Callable[[Task], Awaitable[None]],
) -> None:
    task = await task_store.get_task(task_id)
    if task.status.state in TERMINAL_TASK_STATES:
        return

    await asyncio.sleep(EXECUTOR_STEP_DELAY_SECONDS)
    task = await task_store.get_task(task_id)
    if task.status.state in TERMINAL_TASK_STATES:
        return

    task.status = TaskStatus(state="working", message="Executor is preparing the isolated coding environment.")
    await task_store.save_task(task)
    await publish_task_event(task)

    await asyncio.sleep(EXECUTOR_STEP_DELAY_SECONDS)
    task = await task_store.get_task(task_id)
    if task.status.state in TERMINAL_TASK_STATES:
        return

    task.status = TaskStatus(state="completed", message="Executor finished the isolated coding task.")
    task.artifacts = [build_execution_artifact(str(task.metadata.get("user_request", "")))]
    await task_store.save_task(task)
    await publish_task_event(task)
