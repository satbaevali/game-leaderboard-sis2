"""Redis sorted set (ZSET) that answers top / rank / around in O(log N).

Key: lb:season:{season_id}   member: player_id   score: composite (see scoring.py)
Redis is only an INDEX. If it is lost, rebuild_season() recreates it from Postgres.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from redis.asyncio import Redis

from app import repository
from app.scoring import decode_score, encode_score

REBUILD_BATCH_SIZE = 1000


@dataclass(frozen=True)
class RankedPlayer:
    rank: int  # 1 = best
    player_id: int
    score: int


def season_key(season_id: int) -> str:
    return f"lb:season:{season_id}"


def to_ranked_players(pairs: List[Tuple[str, float]], first_rank: int) -> List[RankedPlayer]:
    ranked = []
    for offset, (member, composite) in enumerate(pairs):
        ranked.append(RankedPlayer(first_rank + offset, int(member), decode_score(composite)))
    return ranked


def to_unix_seconds(moment: datetime) -> int:
    return int(moment.timestamp())


async def publish_best(redis: Redis, season_id: int, player_id: int, best: repository.BestScore) -> int:
    """ZADD GT only ever raises a player's value, so old/duplicate/reordered writes are harmless.
    Returns the 1-based rank right after the write."""
    composite = encode_score(best.best_score, to_unix_seconds(best.achieved_at))
    key = season_key(season_id)
    pipe = redis.pipeline(transaction=False)
    pipe.zadd(key, {str(player_id): composite}, gt=True)
    pipe.zrevrank(key, str(player_id))
    _, zero_based_rank = await pipe.execute()
    return zero_based_rank + 1


async def get_top(redis: Redis, season_id: int, limit: int) -> List[RankedPlayer]:
    pairs = await redis.zrevrange(season_key(season_id), 0, limit - 1, withscores=True)
    return to_ranked_players(pairs, first_rank=1)


async def get_rank(redis: Redis, season_id: int, player_id: int) -> Optional[RankedPlayer]:
    key = season_key(season_id)
    member = str(player_id)
    pipe = redis.pipeline(transaction=False)
    pipe.zrevrank(key, member)
    pipe.zscore(key, member)
    zero_based_rank, composite = await pipe.execute()
    if zero_based_rank is None:
        return None
    return RankedPlayer(zero_based_rank + 1, player_id, decode_score(composite))


def around_window(rank: int, radius: int) -> Tuple[int, int]:
    """0-based inclusive index range around a 1-based rank, never below 0."""
    start = max(0, rank - 1 - radius)
    end = rank - 1 + radius
    return start, end


async def get_around(redis: Redis, season_id: int, player_id: int, radius: int) -> List[RankedPlayer]:
    me = await get_rank(redis, season_id, player_id)
    if me is None:
        return []
    start, end = around_window(me.rank, radius)
    pairs = await redis.zrevrange(season_key(season_id), start, end, withscores=True)
    return to_ranked_players(pairs, first_rank=start + 1)


async def count_players(redis: Redis, season_id: int) -> int:
    return await redis.zcard(season_key(season_id))


async def add_batch(redis: Redis, season_id: int, rows: List[tuple]) -> None:
    mapping = {}
    for player_id, best_score, achieved_at in rows:
        mapping[str(player_id)] = encode_score(best_score, to_unix_seconds(achieved_at))
    if mapping:
        await redis.zadd(season_key(season_id), mapping, gt=True)


async def rebuild_season(redis: Redis, pg_pool, season_id: int) -> int:
    """Re-read all best scores from Postgres page by page (keyset paging) and push them into Redis."""
    total = 0
    last_player_id = 0
    while True:
        async with pg_pool.connection() as conn:
            rows = await repository.get_scores_page(conn, season_id, last_player_id, REBUILD_BATCH_SIZE)
        if not rows:
            return total
        await add_batch(redis, season_id, rows)
        total += len(rows)
        last_player_id = rows[-1][0]
