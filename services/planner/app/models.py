from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


RunStatus = Literal["queued", "planning", "delegated", "streaming_executor", "completed", "failed"]
DelegationMode = Literal["tool", "sub_agent"]


class CreateRunRequest(BaseModel):
    user_request: str
    delegation_mode: DelegationMode = "tool"


class PlannerRun(BaseModel):
    run_id: str
    user_request: str
    delegation_mode: DelegationMode = "tool"
    planner_prompt: str
    execution_plan: str = ""
    executor_prompt: str = ""
    status: RunStatus = "queued"
    executor_task_id: str | None = None
    executor_task_status: str | None = None
    final_response: str | None = None
    error: str | None = None
