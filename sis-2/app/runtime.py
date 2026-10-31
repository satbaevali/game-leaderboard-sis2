"""Application runtime wiring: Postgres pool (13 conns), Redis, gRPC server, and background jobs."""

import asyncio
from typing import List, Optional

from redis.asyncio import Redis

from app import background_jobs, postgres
from app.config import Settings, load_settings
from app.grpc_server import start_grpc_server
from app.metrics import Metrics
from app.service import LeaderboardService


class Runtime:
    """Holds all long-lived resources started once per process."""

    def __init__(self):
        self.settings: Settings = load_settings()
        self.metrics = Metrics()
        self.tasks: List[asyncio.Task] = []
        self.pg_pool = None
        self.admin_pool = None
        self.redis = None
        self.grpc_server = None
        self.service: Optional[LeaderboardService] = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        settings = self.settings
        self.admin_pool = await postgres.open_admin_pool(settings)
        await postgres.create_schema(self.admin_pool)
        await background_jobs.maintain_partitions_once(self.admin_pool)
        self.pg_pool = await postgres.open_pool(settings)
        self.redis = Redis.from_url(settings.redis_url, socket_timeout=0.2, socket_connect_timeout=0.2)
        await background_jobs.rebuild_empty_seasons(self.admin_pool, self.redis)
        self.service = LeaderboardService(settings, self.pg_pool, self.redis, self.metrics)
        self.grpc_server = await start_grpc_server(
            self.service, settings.grpc_port, settings.default_submit_deadline_s
        )
        self._start_background_tasks()
        self._started = True

    def _start_background_tasks(self) -> None:
        settings = self.settings
        reconciler = background_jobs.reconcile_forever(self.admin_pool, self.redis, settings)
        maintenance = background_jobs.maintain_partitions_forever(self.admin_pool, settings)
        self.tasks = [asyncio.create_task(reconciler), asyncio.create_task(maintenance)]

    async def stop(self) -> None:
        if not self._started:
            return
        for task in self.tasks:
            task.cancel()
        await self.grpc_server.stop(grace=2)
        await self.redis.aclose()
        await self.pg_pool.close()
        await self.admin_pool.close()
        self._started = False


_GLOBAL_RUNTIME: Optional[Runtime] = None
_RUNTIME_LOCK = asyncio.Lock()


async def get_runtime() -> Runtime:
    global _GLOBAL_RUNTIME
    async with _RUNTIME_LOCK:
        if _GLOBAL_RUNTIME is None:
            _GLOBAL_RUNTIME = Runtime()
            await _GLOBAL_RUNTIME.start()
        return _GLOBAL_RUNTIME
