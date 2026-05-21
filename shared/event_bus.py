from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis


@dataclass(slots=True)
class StreamEvent:
    id: str
    event: str
    payload: dict[str, Any]
    correlation_id: str
    fields: dict[str, str]


class RedisEventBus:
    def __init__(
        self,
        redis_url: str,
        *,
        namespace: str = "a2a",
        maxlen: int = 1000,
        block_ms: int = 1000,
    ) -> None:
        self.redis_url = redis_url
        self.namespace = namespace
        self.maxlen = maxlen
        self.block_ms = block_ms
        self._redis: Redis | None = None

    async def open(self) -> None:
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        await self.ping()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def ping(self) -> None:
        redis = self._require_redis()
        await redis.ping()

    def task_stream_key(self, context_id: str) -> str:
        return f"{self.namespace}:task:{context_id}"

    def run_stream_key(self, run_id: str) -> str:
        return f"{self.namespace}:run:{run_id}"

    async def publish_task_event(
        self,
        task_payload: dict[str, Any],
        *,
        source_agent: str,
    ) -> str:
        status = str(task_payload.get("status", {}).get("state", "submitted"))
        context_id = str(task_payload["context_id"])
        event_type = "task_complete" if status in {"completed", "failed", "canceled"} else "task_status"
        return await self._publish(
            self.task_stream_key(context_id),
            event_type=event_type,
            correlation_id=context_id,
            source_agent=source_agent,
            payload=task_payload,
            extra_fields={
                "task_id": str(task_payload["id"]),
                "task_state": status,
            },
        )

    async def publish_run_event(
        self,
        run_payload: dict[str, Any],
        *,
        source_agent: str,
    ) -> str:
        status = str(run_payload.get("status", "queued"))
        run_id = str(run_payload["run_id"])
        event_type = "run_complete" if status in {"completed", "failed"} else "run_status"
        return await self._publish(
            self.run_stream_key(run_id),
            event_type=event_type,
            correlation_id=run_id,
            source_agent=source_agent,
            payload=run_payload,
            extra_fields={"run_id": run_id, "run_status": status},
        )

    async def stream_events(
        self,
        stream_key: str,
        *,
        start_id: str = "0-0",
    ) -> AsyncIterator[StreamEvent]:
        redis = self._require_redis()
        last_id = start_id

        while True:
            results = await redis.xread({stream_key: last_id}, block=self.block_ms)
            if not results:
                continue

            for _, entries in results:
                for entry_id, fields in entries:
                    last_id = entry_id
                    payload = json.loads(fields["payload"])
                    yield StreamEvent(
                        id=entry_id,
                        event=fields["event"],
                        payload=payload,
                        correlation_id=fields["correlation_id"],
                        fields=fields,
                    )

    async def _publish(
        self,
        stream_key: str,
        *,
        event_type: str,
        correlation_id: str,
        source_agent: str,
        payload: dict[str, Any],
        extra_fields: dict[str, str] | None = None,
    ) -> str:
        redis = self._require_redis()
        fields = {
            "event": event_type,
            "correlation_id": correlation_id,
            "source_agent": source_agent,
            "payload": json.dumps(payload, sort_keys=True),
        }
        if extra_fields:
            fields.update(extra_fields)
        return await redis.xadd(stream_key, fields, maxlen=self.maxlen, approximate=True)

    def _require_redis(self) -> Redis:
        if self._redis is None:
            raise RuntimeError("Redis event bus is not open.")
        return self._redis
