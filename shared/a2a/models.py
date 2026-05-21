from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TaskState = Literal["submitted", "working", "input_required", "completed", "failed", "canceled"]


class TextPart(BaseModel):
    type: Literal["text"] = "text"
    text: str


class Message(BaseModel):
    role: Literal["user", "agent"]
    parts: list[TextPart]
    task_id: str | None = None
    context_id: str | None = None


class Artifact(BaseModel):
    name: str
    parts: list[TextPart]
    metadata: dict[str, Any] = Field(default_factory=dict)


class TaskStatus(BaseModel):
    state: TaskState
    message: str


class Task(BaseModel):
    id: str
    context_id: str
    status: TaskStatus
    artifacts: list[Artifact] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentCapabilities(BaseModel):
    streaming: bool = False
    pushNotifications: bool = False


class AgentSkill(BaseModel):
    id: str
    name: str
    description: str
    examples: list[str] = Field(default_factory=list)


class AgentCard(BaseModel):
    name: str
    description: str
    url: str
    version: str = "0.1.0"
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    skills: list[AgentSkill] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MessageSendConfiguration(BaseModel):
    acceptedOutputModes: list[str] = Field(default_factory=lambda: ["text/plain"])
    historyLength: int = 0


class MessageSendParams(BaseModel):
    message: Message
    configuration: MessageSendConfiguration | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TaskQueryParams(BaseModel):
    id: str


class TaskCancelParams(BaseModel):
    id: str


class JSONRPCRequest(BaseModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class JSONRPCResponse(BaseModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None
    result: Any | None = None
    error: dict[str, Any] | None = None


def task_to_result(task: Task) -> dict[str, Any]:
    return task.model_dump(mode="json")
