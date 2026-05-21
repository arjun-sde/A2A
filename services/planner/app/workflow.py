from __future__ import annotations

import logging

from langgraph.graph import END, START, StateGraph

from services.planner.app.a2a_client import A2AClient
from services.planner.app.config import ANALYST_AGENT_CARD_URL, EXECUTOR_AGENT_CARD_URL
from services.planner.app.prompts import build_analyst_prompt, build_executor_prompt, build_sub_agent_followup_prompt
from services.planner.app.store import PlannerStateStore
from shared.a2a.models import Task
from shared.event_bus import RedisEventBus


logger = logging.getLogger(__name__)


def _artifact_text(task: Task) -> str:
    parts: list[str] = []
    for artifact in task.artifacts:
        for part in artifact.parts:
            if part.type == "text":
                parts.append(part.text)
    return "\n\n".join(parts).strip()


def build_workflow(checkpointer, state_store: PlannerStateStore, a2a_client: A2AClient, event_bus: RedisEventBus):
    async def update_run_and_publish(run_id: str, **fields: str | None) -> None:
        run = await state_store.update_run(run_id, **fields)
        try:
            await event_bus.publish_run_event(run.model_dump(mode="json"), source_agent="planner-service")
        except Exception:
            logger.exception("Failed to publish planner run event for run %s", run_id)

    async def plan_request(state: dict) -> dict:
        run_id = state["run_id"]
        user_request = state["user_request"]
        delegation_mode = state["delegation_mode"]

        await update_run_and_publish(run_id, status="planning")
        plan_lines = [
            "1. Understand the user request.",
            "2. Delegate through A2A using the configured delegation mode.",
            "3. Consume delegated task status over SSE until it reaches a terminal state.",
            "4. Merge the delegated artifact into the final response.",
        ]
        execution_plan = "\n".join(plan_lines)
        executor_prompt = (
            build_executor_prompt(execution_plan, user_request)
            if delegation_mode == "tool"
            else build_analyst_prompt(execution_plan, user_request)
        )
        await update_run_and_publish(run_id, execution_plan=execution_plan, executor_prompt=executor_prompt)
        return {
            "delegation_mode": delegation_mode,
            "execution_plan": execution_plan,
            "executor_prompt": executor_prompt,
        }

    async def delegate_task(state: dict) -> dict:
        run_id = state["run_id"]
        delegation_mode = state["delegation_mode"]
        target_card_url = EXECUTOR_AGENT_CARD_URL if delegation_mode == "tool" else ANALYST_AGENT_CARD_URL

        agent_card, task = await a2a_client.send_message(
            target_card_url,
            state["executor_prompt"],
            metadata={
                "request_id": f"{run_id}:delegate:initial",
                "source_agent": "planner-service",
                "original_user_request": state["user_request"],
                "execution_plan": state["execution_plan"],
                "delegation_mode": delegation_mode,
            },
        )
        if delegation_mode == "sub_agent" and task.status.state == "input_required":
            task = await a2a_client.send_message_to_agent_url(
                agent_card.url,
                build_sub_agent_followup_prompt(state["execution_plan"], state["user_request"]),
                metadata={
                    "request_id": f"{run_id}:delegate:followup",
                    "source_agent": "planner-service",
                    "original_user_request": state["user_request"],
                    "execution_plan": state["execution_plan"],
                    "delegation_mode": delegation_mode,
                },
                task_id=task.id,
                context_id=task.context_id,
            )
        await update_run_and_publish(
            run_id,
            status="delegated",
            executor_task_id=task.id,
            executor_task_status=task.status.state,
        )
        return {
            "delegation_mode": delegation_mode,
            "executor_agent_url": agent_card.url,
            "executor_task_id": task.id,
            "executor_task_stream_url": a2a_client.build_task_stream_url(agent_card, task.id),
        }

    async def stream_executor(state: dict) -> dict:
        run_id = state["run_id"]
        task_id = state["executor_task_id"]

        await update_run_and_publish(run_id, status="streaming_executor")

        async for task in a2a_client.stream_task_updates_from_url(state["executor_task_stream_url"]):
            await update_run_and_publish(run_id, executor_task_status=task.status.state)
            if task.status.state in {"completed", "failed", "canceled"}:
                return {
                    "executor_result": _artifact_text(task),
                    "executor_task_status": task.status.state,
                }

        raise RuntimeError(f"Executor stream closed before task {task_id} reached a terminal state.")

    async def finalize_response(state: dict) -> dict:
        run_id = state["run_id"]

        final_response = (
            "Planner completed the run.\n\n"
            f"Delegation mode: {state['delegation_mode']}\n\n"
            f"Plan:\n{state['execution_plan']}\n\n"
            f"Delegated task status: {state['executor_task_status']}\n\n"
            f"Delegated result:\n{state.get('executor_result', 'No delegated artifact returned.')}"
        )
        await update_run_and_publish(run_id, status="completed", final_response=final_response)
        return {"final_response": final_response}

    graph = StateGraph(dict)
    graph.add_node("plan", plan_request)
    graph.add_node("delegate", delegate_task)
    graph.add_node("stream", stream_executor)
    graph.add_node("finish", finalize_response)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "delegate")
    graph.add_edge("delegate", "stream")
    graph.add_edge("stream", "finish")
    graph.add_edge("finish", END)
    return graph.compile(checkpointer=checkpointer)
