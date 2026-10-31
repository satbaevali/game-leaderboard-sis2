"""Tests against real Postgres + Redis (skipped when they are not running).
They use their own season id, so they do not touch the seeded season 1.
"""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from redis.asyncio import Redis

from app import background_jobs, postgres, redis_index
from app.config import load_settings
from app.deadline import Deadline
from app.errors import InvalidArgument, PlayerNotFound
from app.metrics import Metrics
from app.service import LeaderboardService, SubmitCommand

TEST_SEASON = 9001
FIRST_PLAYER = 9_000_001
PLAYER_COUNT = 3


def infra_is_up() -> bool:
    async def probe():
        settings = load_settings()
        redis = Redis.from_url(settings.redis_url, socket_connect_timeout=0.3)
        try:
            await redis.ping()
            pool = await postgres.open_admin_pool(settings)
            await pool.close()
            return True
        except Exception:
            return False
        finally:
            await redis.aclose()

    return asyncio.run(probe())


pytestmark = pytest.mark.skipif(not infra_is_up(), reason="Postgres/Redis not running")


class Environment:
    def __init__(self):
        self.settings = load_settings()

    async def open(self):
        self.admin_pool = await postgres.open_admin_pool(self.settings)
        await postgres.create_schema(self.admin_pool)
        await postgres.ensure_partitions(self.admin_pool, datetime.now(timezone.utc).date())
        self.pool = await postgres.open_pool(self.settings)
        self.redis = Redis.from_url(self.settings.redis_url)
        self.service = LeaderboardService(self.settings, self.pool, self.redis, Metrics())
        await self._reset_data()

    async def _reset_data(self):
        async with self.admin_pool.connection() as conn:
            await conn.execute("DELETE FROM player_scores WHERE season_id = %s", (TEST_SEASON,))
            await conn.execute("DELETE FROM score_events WHERE season_id = %s", (TEST_SEASON,))
            for offset in range(PLAYER_COUNT):
                await conn.execute(
                    "INSERT INTO players (player_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (FIRST_PLAYER + offset, f"test_{offset}"),
                )
        await self.redis.delete(redis_index.season_key(TEST_SEASON))

    async def close(self):
        await self.pool.close()
        await self.admin_pool.close()
        await self.redis.aclose()

    def command(self, player_offset: int, score: int, match_id: str = None) -> SubmitCommand:
        return SubmitCommand(
            match_id=match_id or str(uuid.uuid4()),
            player_id=FIRST_PLAYER + player_offset,
            season_id=TEST_SEASON,
            score=score,
            match_started_at=datetime.now(timezone.utc),
        )

    async def submit(self, command: SubmitCommand):
        return await self.service.submit_score(command, Deadline.after_seconds(2))


def run_scenario(scenario):
    async def wrapper():
        env = Environment()
        await env.open()
        try:
            return await scenario(env)
        finally:
            await env.close()

    return asyncio.run(wrapper())


def test_duplicate_match_is_applied_once():
    async def scenario(env):
        command = env.command(0, 500)
        first = await env.submit(command)
        second = await env.submit(command)
        return first, second

    first, second = run_scenario(scenario)
    assert first.improved and not first.duplicate
    assert second.duplicate and not second.improved
    assert second.best_score == 500


def test_lower_score_does_not_reduce_best():
    async def scenario(env):
        await env.submit(env.command(0, 900))
        return await env.submit(env.command(0, 100))

    result = run_scenario(scenario)
    assert result.best_score == 900 and not result.improved


def test_top_rank_and_around_are_ordered():
    async def scenario(env):
        await env.submit(env.command(0, 100))
        await env.submit(env.command(1, 300))
        await env.submit(env.command(2, 200))
        top = await env.service.get_top(TEST_SEASON, 3)
        rank = await env.service.get_rank(TEST_SEASON, FIRST_PLAYER + 2)
        around = await env.service.get_around(TEST_SEASON, FIRST_PLAYER + 2, 1)
        return top, rank, around

    top, rank, around = run_scenario(scenario)
    assert [entry.score for entry in top] == [300, 200, 100]
    assert rank.rank == 2
    assert [entry.rank for entry in around] == [1, 2, 3]


def test_equal_scores_earlier_submit_ranks_higher():
    async def scenario(env):
        await env.submit(env.command(0, 400))
        await asyncio.sleep(1.1)  # timestamps have 1-second resolution
        await env.submit(env.command(1, 400))
        return await env.service.get_top(TEST_SEASON, 2)

    top = run_scenario(scenario)
    assert [entry.player_id for entry in top] == [FIRST_PLAYER, FIRST_PLAYER + 1]


def test_unknown_player_is_not_found():
    async def scenario(env):
        command = env.command(0, 10)
        unknown = SubmitCommand(command.match_id, 777_777_777, TEST_SEASON, 10, command.match_started_at)
        await env.submit(unknown)

    with pytest.raises(PlayerNotFound):
        run_scenario(scenario)


def test_invalid_score_is_rejected():
    async def scenario(env):
        await env.submit(env.command(0, -5))

    with pytest.raises(InvalidArgument):
        run_scenario(scenario)


def test_redis_index_is_rebuilt_from_postgres():
    async def scenario(env):
        await env.submit(env.command(0, 111))
        await env.submit(env.command(1, 222))
        await env.redis.delete(redis_index.season_key(TEST_SEASON))
        rebuilt = await redis_index.rebuild_season(env.redis, env.admin_pool, TEST_SEASON)
        return rebuilt, await redis_index.get_top(env.redis, TEST_SEASON, 2)

    rebuilt, top = run_scenario(scenario)
    assert rebuilt == 2
    assert [player.score for player in top] == [222, 111]


def test_reconciler_heals_missing_redis_entry():
    async def scenario(env):
        since = datetime.now(timezone.utc)
        await env.submit(env.command(0, 333))
        await env.redis.delete(redis_index.season_key(TEST_SEASON))  # simulate a lost Redis write
        await background_jobs.reconcile_once(env.admin_pool, env.redis, since)
        return await redis_index.get_rank(env.redis, TEST_SEASON, FIRST_PLAYER)

    player = run_scenario(scenario)
    assert player is not None and player.score == 333
