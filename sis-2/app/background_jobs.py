"""Background jobs: (1) repair Redis from Postgres, (2) manage daily partitions."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from redis.exceptions import RedisError

from app import postgres, redis_index, repository
from app.config import Settings

log = logging.getLogger("leaderboard.jobs")


async def reconcile_once(pg_pool, redis, since: datetime) -> int:
    """Re-publish every player that got a new event since `since`. ZADD GT makes this safe to repeat."""
    async with pg_pool.connection() as conn:
        changed = await repository.players_changed_since(conn, since)
        for season_id, player_id in changed:
            best = await repository.get_best(conn, season_id, player_id)
            if best is not None:
                await redis_index.publish_best(redis, season_id, player_id, best)
    return len(changed)


async def reconcile_forever(pg_pool, redis, settings: Settings) -> None:
    overlap = timedelta(seconds=settings.reconcile_overlap_s)
    watermark = datetime.now(timezone.utc) - overlap
    while True:
        round_started = datetime.now(timezone.utc)
        try:
            await reconcile_once(pg_pool, redis, watermark)
            watermark = round_started - overlap
        except (RedisError, OSError) as error:
            log.warning("reconcile failed, will retry: %s", error)
        await asyncio.sleep(settings.reconcile_interval_s)


async def maintain_partitions_once(pg_pool) -> None:
    today = datetime.now(timezone.utc).date()
    await postgres.ensure_partitions(pg_pool, today)
    dropped = await postgres.drop_old_partitions(pg_pool, today)
    if dropped:
        log.info("dropped old partitions: %s", dropped)


async def maintain_partitions_forever(pg_pool, settings: Settings) -> None:
    while True:
        try:
            await maintain_partitions_once(pg_pool)
        except OSError as error:
            log.warning("partition maintenance failed: %s", error)
        await asyncio.sleep(settings.maintenance_interval_s)


async def rebuild_empty_seasons(pg_pool, redis) -> None:
    """On startup: a season that exists in Postgres but is empty in Redis (Redis was wiped) is rebuilt."""
    async with pg_pool.connection() as conn:
        season_ids = await repository.list_season_ids(conn)
    for season_id in season_ids:
        if await redis_index.count_players(redis, season_id) == 0:
            rebuilt = await redis_index.rebuild_season(redis, pg_pool, season_id)
            log.info("rebuilt season %s: %s players", season_id, rebuilt)
