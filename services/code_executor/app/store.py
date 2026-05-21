from __future__ import annotations

import json
from dataclasses import dataclass

from psycopg_pool import AsyncConnectionPool

from shared.a2a.models import Task


@dataclass(slots=True)
class QueueLease:
    task_id: str
    attempts: int


class ExecutorTaskStore:
    def __init__(self, database_url: str) -> None:
        self.pool = AsyncConnectionPool(conninfo=database_url, open=False)

    async def open(self) -> None:
        await self.pool.open()

    async def close(self) -> None:
        await self.pool.close()

    async def initialize(self) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS executor_tasks (
                    task_id TEXT PRIMARY KEY,
                    client_request_id TEXT,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            await conn.execute("ALTER TABLE executor_tasks ADD COLUMN IF NOT EXISTS client_request_id TEXT")
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS executor_tasks_client_request_id_idx
                ON executor_tasks (client_request_id)
                WHERE client_request_id IS NOT NULL
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS executor_task_queue (
                    task_id TEXT PRIMARY KEY REFERENCES executor_tasks(task_id) ON DELETE CASCADE,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    leased_at TIMESTAMPTZ,
                    lease_expires_at TIMESTAMPTZ,
                    worker_id TEXT,
                    last_error TEXT
                )
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS executor_task_queue_claim_idx
                ON executor_task_queue (status, available_at, lease_expires_at, enqueued_at)
                """
            )

    async def save_task(self, task: Task) -> None:
        payload = json.dumps(task.model_dump(mode="json"))
        client_request_id = task.metadata.get("client_request_id")
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO executor_tasks (task_id, client_request_id, status, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(task_id) DO UPDATE
                SET client_request_id = excluded.client_request_id,
                    status = excluded.status,
                    payload = excluded.payload
                """,
                (task.id, client_request_id, task.status.state, payload),
            )

    async def create_task_if_absent(self, task: Task) -> tuple[Task, bool]:
        client_request_id = task.metadata.get("client_request_id")
        if not client_request_id:
            await self.save_task(task)
            return task, True

        payload = json.dumps(task.model_dump(mode="json"))
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO executor_tasks (task_id, client_request_id, status, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (client_request_id) WHERE client_request_id IS NOT NULL
                DO UPDATE SET client_request_id = executor_tasks.client_request_id
                RETURNING payload, (xmax = 0) AS inserted
                """,
                (task.id, client_request_id, task.status.state, payload),
            )
            row = await cursor.fetchone()

        persisted_task = Task.model_validate_json(row[0])
        created = bool(row[1])
        return persisted_task, created

    async def get_task(self, task_id: str) -> Task:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT payload FROM executor_tasks WHERE task_id = %s",
                (task_id,),
            )
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(task_id)
        return Task.model_validate_json(row[0])

    async def get_task_by_client_request_id(self, client_request_id: str) -> Task | None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT payload FROM executor_tasks WHERE client_request_id = %s",
                (client_request_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return Task.model_validate_json(row[0])

    async def enqueue_task(self, task_id: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO executor_task_queue (task_id, status)
                VALUES (%s, 'queued')
                ON CONFLICT(task_id) DO UPDATE
                SET status = 'queued',
                    available_at = NOW(),
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    worker_id = NULL,
                    last_error = NULL
                WHERE executor_task_queue.status NOT IN ('done', 'canceled')
                """,
                (task_id,),
            )

    async def claim_next_task(self, worker_id: str, lease_seconds: float) -> QueueLease | None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                WITH next_job AS (
                    SELECT task_id
                    FROM executor_task_queue
                    WHERE (status = 'queued' AND available_at <= NOW())
                       OR (status = 'leased' AND lease_expires_at <= NOW())
                    ORDER BY available_at, enqueued_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE executor_task_queue AS queue
                SET status = 'leased',
                    attempts = queue.attempts + 1,
                    leased_at = NOW(),
                    lease_expires_at = NOW() + (%s * interval '1 second'),
                    worker_id = %s,
                    last_error = NULL
                FROM next_job
                WHERE queue.task_id = next_job.task_id
                RETURNING queue.task_id, queue.attempts
                """,
                (lease_seconds, worker_id),
            )
            row = await cursor.fetchone()

        if row is None:
            return None
        return QueueLease(task_id=row[0], attempts=row[1])

    async def complete_queue_task(self, task_id: str) -> None:
        await self._update_queue_status(task_id, "done")

    async def cancel_queue_task(self, task_id: str) -> None:
        await self._update_queue_status(task_id, "canceled")

    async def fail_queue_task(self, task_id: str, error: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                UPDATE executor_task_queue
                SET status = 'failed',
                    last_error = %s,
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    worker_id = NULL
                WHERE task_id = %s
                """,
                (error, task_id),
            )

    async def retry_queue_task(self, task_id: str, retry_delay_seconds: float, error: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                UPDATE executor_task_queue
                SET status = 'queued',
                    available_at = NOW() + (%s * interval '1 second'),
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    worker_id = NULL,
                    last_error = %s
                WHERE task_id = %s
                """,
                (retry_delay_seconds, error, task_id),
            )

    async def release_lease(self, task_id: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                UPDATE executor_task_queue
                SET status = 'queued',
                    available_at = NOW(),
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    worker_id = NULL
                WHERE task_id = %s AND status = 'leased'
                """,
                (task_id,),
            )

    async def renew_lease(self, task_id: str, worker_id: str, lease_seconds: float) -> bool:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE executor_task_queue
                SET lease_expires_at = NOW() + (%s * interval '1 second')
                WHERE task_id = %s AND status = 'leased' AND worker_id = %s
                RETURNING task_id
                """,
                (lease_seconds, task_id, worker_id),
            )
            row = await cursor.fetchone()
        return row is not None

    async def ping(self) -> None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute("SELECT 1")
            await cursor.fetchone()

    async def get_queue_depth(self) -> int:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT COUNT(*)
                FROM executor_task_queue
                WHERE status IN ('queued', 'leased')
                """
            )
            row = await cursor.fetchone()
        return int(row[0]) if row is not None else 0

    async def _update_queue_status(self, task_id: str, status: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                UPDATE executor_task_queue
                SET status = %s,
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    worker_id = NULL
                WHERE task_id = %s
                """,
                (status, task_id),
            )
