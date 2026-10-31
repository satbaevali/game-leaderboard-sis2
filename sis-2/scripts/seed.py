"""Seeds players and best scores into Postgres, then rebuilds the Redis index.

Usage: .venv/bin/python -m scripts.seed --players 100000 --season 1
"""

import argparse
import asyncio
import random
from datetime import datetime, timezone

from redis.asyncio import Redis

from app import postgres, redis_index
from app.config import load_settings

BATCH_SIZE = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=100_000)
    parser.add_argument("--season", type=int, default=1)
    return parser.parse_args()


def make_player_rows(first_id: int, last_id: int) -> list:
    return [(player_id, f"player_{player_id}") for player_id in range(first_id, last_id + 1)]


def make_score_rows(season: int, first_id: int, last_id: int, now: datetime) -> list:
    return [(season, pid, random.randint(0, 1_000_000), now) for pid in range(first_id, last_id + 1)]


async def insert_batch(conn, season: int, first_id: int, last_id: int, now: datetime) -> None:
    async with conn.cursor() as cursor:
        await cursor.executemany(
            "INSERT INTO players (player_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            make_player_rows(first_id, last_id),
        )
        await cursor.executemany(
            "INSERT INTO player_scores (season_id, player_id, best_score, achieved_at) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            make_score_rows(season, first_id, last_id, now),
        )


async def seed_postgres(pool, season: int, total: int) -> None:
    now = datetime.now(timezone.utc)
    for first_id in range(1, total + 1, BATCH_SIZE):
        last_id = min(first_id + BATCH_SIZE - 1, total)
        async with pool.connection() as conn:
            await insert_batch(conn, season, first_id, last_id, now)


async def main() -> None:
    args = parse_args()
    settings = load_settings()
    pool = await postgres.open_admin_pool(settings)
    await postgres.create_schema(pool)
    await seed_postgres(pool, args.season, args.players)
    redis = Redis.from_url(settings.redis_url)
    rebuilt = await redis_index.rebuild_season(redis, pool, args.season)
    print(f"seeded {args.players} players, indexed {rebuilt} in Redis")
    await redis.aclose()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
