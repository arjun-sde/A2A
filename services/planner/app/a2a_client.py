from __future__ import annotations

import json
from collections.abc import AsyncIterator
from inspect import isawaitable
from typing import Any, Awaitable, Callable
from uuid import uuid4

import httpx

from shared.a2a.models import AgentCard, JSONRPCRequest, Message, MessageSendConfiguration, MessageSendParams, Task, TextPart


class A2AClient:
    def __init__(
        self,
        timeout: float = 15.0,
        auth_token_factory: Callable[[str], str | Awaitable[str]] | None = None,
    ) -> None:
        self.timeout = timeout
        self.auth_token_factory = auth_token_factory

    async def get_agent_card(self, card_url: str) -> AgentCard:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(card_url)
            response.raise_for_status()
        return AgentCard.model_validate(response.json())

    async def send_message(self, card_url: str, prompt: str, metadata: dict | None = None) -> tuple[AgentCard, Task]:
        agent_card = await self.get_agent_card(card_url)
        task = await self.send_message_to_agent_url(agent_card.url, prompt, metadata=metadata)
        return agent_card, task

    async def send_message_to_agent_url(
        self,
        agent_url: str,
        prompt: str,
        *,
        metadata: dict | None = None,
        task_id: str | None = None,
        context_id: str | None = None,
    ) -> Task:
        params = MessageSendParams(
            message=Message(
                role="user",
                parts=[TextPart(text=prompt)],
                task_id=task_id,
                context_id=context_id,
            ),
            configuration=MessageSendConfiguration(),
            metadata=metadata or {},
        )
        request = JSONRPCRequest(
            id=str(uuid4()),
            method="message/send",
            params=params.model_dump(mode="json", exclude_none=True),
        )
        payload = await self._post_jsonrpc(agent_url, request)
        return Task.model_validate(payload["result"])

    async def get_task(self, agent_url: str, task_id: str) -> Task:
        request = JSONRPCRequest(
            id=str(uuid4()),
            method="tasks/get",
            params={"id": task_id},
        )
        payload = await self._post_jsonrpc(agent_url, request)
        return Task.model_validate(payload["result"])

    def build_task_stream_url(self, agent_card: AgentCard, task_id: str) -> str:
        template = str(agent_card.metadata.get("task_stream_url_template", "")).strip()
        if not template:
            raise RuntimeError("Agent card does not advertise a task stream URL template.")
        return template.format(task_id=task_id)

    async def stream_task_updates(self, agent_card: AgentCard, task_id: str) -> AsyncIterator[Task]:
        stream_url = self.build_task_stream_url(agent_card, task_id)
        async for task in self.stream_task_updates_from_url(stream_url):
            yield task

    async def stream_task_updates_from_url(self, stream_url: str) -> AsyncIterator[Task]:
        headers = await self._auth_headers(stream_url)
        headers["accept"] = "text/event-stream"
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "GET",
                stream_url,
                headers=headers,
            ) as response:
                response.raise_for_status()
                event_name = "message"
                data_lines: list[str] = []

                async for line in response.aiter_lines():
                    if not line:
                        if data_lines and event_name in {"task_status", "task_complete"}:
                            yield Task.model_validate(json.loads("\n".join(data_lines)))
                            if event_name == "task_complete":
                                return
                        event_name = "message"
                        data_lines = []
                        continue

                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event_name = line[len("event:") :].strip()
                        continue
                    if line.startswith("data:"):
                        data_lines.append(line[len("data:") :].lstrip())

    async def _post_jsonrpc(self, url: str, request: JSONRPCRequest) -> dict:
        headers = await self._auth_headers(url)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=request.model_dump(mode="json"), headers=headers)
            response.raise_for_status()
            payload = response.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        return payload

    async def _auth_headers(self, url: str) -> dict[str, str]:
        if self.auth_token_factory is None:
            return {}
        token: Any = self.auth_token_factory(url)
        if isawaitable(token):
            token = await token
        return {"authorization": f"Bearer {token}"}
