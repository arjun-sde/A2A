from __future__ import annotations

from psycopg_pool import AsyncConnectionPool

from services.planner.app.models import PlannerRun


class PlannerStateStore:
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
                CREATE TABLE IF NOT EXISTS planner_runs (
                    run_id TEXT PRIMARY KEY,
                    user_request TEXT NOT NULL,
                    delegation_mode TEXT NOT NULL,
                    planner_prompt TEXT NOT NULL,
                    execution_plan TEXT NOT NULL,
                    executor_prompt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    executor_task_id TEXT,
                    executor_task_status TEXT,
                    final_response TEXT,
                    error TEXT
                )
                """
            )
            await conn.execute("ALTER TABLE planner_runs ADD COLUMN IF NOT EXISTS delegation_mode TEXT NOT NULL DEFAULT 'tool'")

    async def ping(self) -> None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute("SELECT 1")
            await cursor.fetchone()

    async def create_run(self, run: PlannerRun) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO planner_runs (
                    run_id, user_request, delegation_mode, planner_prompt, execution_plan, executor_prompt,
                    status, executor_task_id, executor_task_status, final_response, error
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run.run_id,
                    run.user_request,
                    run.delegation_mode,
                    run.planner_prompt,
                    run.execution_plan,
                    run.executor_prompt,
                    run.status,
                    run.executor_task_id,
                    run.executor_task_status,
                    run.final_response,
                    run.error,
                ),
            )

    async def update_run(self, run_id: str, **fields: str | None) -> PlannerRun:
        if not fields:
            return await self.get_run(run_id)

        allowed = {
            "user_request",
            "delegation_mode",
            "planner_prompt",
            "execution_plan",
            "executor_prompt",
            "status",
            "executor_task_id",
            "executor_task_status",
            "final_response",
            "error",
        }
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"Unsupported planner state fields: {sorted(invalid)}")

        assignments = ", ".join(f"{key} = %s" for key in fields)
        values = list(fields.values()) + [run_id]
        async with self.pool.connection() as conn:
            await conn.execute(f"UPDATE planner_runs SET {assignments} WHERE run_id = %s", values)
        return await self.get_run(run_id)

    async def get_run(self, run_id: str) -> PlannerRun:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT run_id, user_request, delegation_mode, planner_prompt, execution_plan, executor_prompt,
                       status, executor_task_id, executor_task_status, final_response, error
                FROM planner_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(run_id)

        return PlannerRun(
            run_id=row[0],
            user_request=row[1],
            delegation_mode=row[2],
            planner_prompt=row[3],
            execution_plan=row[4],
            executor_prompt=row[5],
            status=row[6],
            executor_task_id=row[7],
            executor_task_status=row[8],
            final_response=row[9],
            error=row[10],
        )

    async def list_incomplete_run_ids(self) -> list[str]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT run_id
                FROM planner_runs
                WHERE status NOT IN ('completed', 'failed')
                ORDER BY run_id
                """
            )
            rows = await cursor.fetchall()
        return [row[0] for row in rows]
