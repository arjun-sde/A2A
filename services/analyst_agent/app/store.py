from __future__ import annotations

import json

from psycopg_pool import AsyncConnectionPool

from shared.a2a.models import Task


class AnalystTaskStore:
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
                CREATE TABLE IF NOT EXISTS analyst_tasks (
                    task_id TEXT PRIMARY KEY,
                    client_request_id TEXT,
                    payload TEXT NOT NULL
                )
                """
            )
            await conn.execute("ALTER TABLE analyst_tasks ADD COLUMN IF NOT EXISTS client_request_id TEXT")
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS analyst_tasks_client_request_id_idx
                ON analyst_tasks (client_request_id)
                WHERE client_request_id IS NOT NULL
                """
            )

    async def save_task(self, task: Task) -> None:
        payload = json.dumps(task.model_dump(mode="json"))
        client_request_id = task.metadata.get("client_request_id")
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO analyst_tasks (task_id, client_request_id, payload)
                VALUES (%s, %s, %s)
                ON CONFLICT(task_id) DO UPDATE
                SET client_request_id = excluded.client_request_id, payload = excluded.payload
                """,
                (task.id, client_request_id, payload),
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
                INSERT INTO analyst_tasks (task_id, client_request_id, payload)
                VALUES (%s, %s, %s)
                ON CONFLICT (client_request_id) WHERE client_request_id IS NOT NULL
                DO UPDATE SET client_request_id = analyst_tasks.client_request_id
                RETURNING payload, (xmax = 0) AS inserted
                """,
                (task.id, client_request_id, payload),
            )
            row = await cursor.fetchone()

        persisted_task = Task.model_validate_json(row[0])
        created = bool(row[1])
        return persisted_task, created

    async def get_task(self, task_id: str) -> Task:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT payload FROM analyst_tasks WHERE task_id = %s",
                (task_id,),
            )
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(task_id)
        return Task.model_validate_json(row[0])

    async def get_task_by_client_request_id(self, client_request_id: str) -> Task | None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT payload FROM analyst_tasks WHERE client_request_id = %s",
                (client_request_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return Task.model_validate_json(row[0])

    async def ping(self) -> None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute("SELECT 1")
            await cursor.fetchone()
