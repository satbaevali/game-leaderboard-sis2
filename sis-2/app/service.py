"""Business logic. Transport-independent: HTTP and gRPC both call this class.

Rules from the design:
  * Postgres commit FIRST, Redis AFTER. Postgres is the truth, Redis is an index.
  * No network call (Redis) while a DB transaction/connection is held.
  * If Redis fails after the commit, the reconciler heals it later; the user still gets success.
"""

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Dict, List, Optional

from psycopg_pool import PoolTimeout
from redis.exceptions import RedisError

from app import redis_index, repository
from app.cache import CoalescingTtlCache, UsernameCache
from app.config import Settings
from app.deadline import Deadline
from app.errors import InvalidArgument, Overloaded, PlayerNotFound
from app.metrics import Metrics
from app.scoring import MAX_SAFE_SCORE

MAX_CLOCK_SKEW = timedelta(days=1)
MAX_MATCH_ID_LENGTH = 64


@dataclass(frozen=True)
class SubmitCommand:
    match_id: str
    player_id: int
    season_id: int
    score: int
    match_started_at: datetime


@dataclass(frozen=True)
class SubmitResult:
    improved: bool
    best_score: int
    rank: Optional[int]  # None = Redis was not updated yet (reconciler will fix it)
    duplicate: bool


@dataclass(frozen=True)
class LeaderboardEntry:
    rank: int
    player_id: int
    username: str
    score: int


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LeaderboardService:
    def __init__(self, settings: Settings, pg_pool, redis, metrics: Metrics):
        self._settings = settings
        self._pg_pool = pg_pool
        self._redis = redis
        self._metrics = metrics
        self._top_cache = CoalescingTtlCache(settings.top_cache_ttl_s)
        self._usernames = UsernameCache(settings.names_cache_ttl_s)

    # ---------- validation ----------

    def _validate_submit(self, cmd: SubmitCommand) -> None:
        if not cmd.match_id or len(cmd.match_id) > MAX_MATCH_ID_LENGTH:
            raise InvalidArgument("match_id must be 1..64 chars")
        if cmd.player_id <= 0 or cmd.season_id <= 0:
            raise InvalidArgument("player_id and season_id must be positive")
        if not 0 <= cmd.score <= min(self._settings.max_score, MAX_SAFE_SCORE):
            raise InvalidArgument("score out of range")
        if abs(utc_now() - cmd.match_started_at) > MAX_CLOCK_SKEW:
            raise InvalidArgument("match_started_at must be within one day of now")

    def _validate_limit(self, limit: int) -> None:
        if not 1 <= limit <= self._settings.max_top_limit:
            raise InvalidArgument(f"limit must be 1..{self._settings.max_top_limit}")

    def _validate_radius(self, radius: int) -> None:
        if not 0 <= radius <= self._settings.max_around_radius:
            raise InvalidArgument(f"radius must be 0..{self._settings.max_around_radius}")

    # ---------- submit ----------

    async def _store_in_postgres(self, cmd: SubmitCommand) -> repository.StoredResult:
        event_date = cmd.match_started_at.astimezone(timezone.utc).date()
        try:
            async with self._pg_pool.connection() as conn:
                return await repository.store_score(
                    conn, event_date, cmd.match_id, cmd.player_id, cmd.season_id, cmd.score
                )
        except PoolTimeout:
            raise Overloaded("database is busy, retry later") from None
        except Exception as error:
            if "statement timeout" in str(error) or "lock timeout" in str(error):
                from app.errors import DeadlineExceeded

                raise DeadlineExceeded("database statement timeout") from None
            raise

    async def _publish_to_redis(self, cmd: SubmitCommand, best: repository.BestScore) -> Optional[int]:
        try:
            return await redis_index.publish_best(self._redis, cmd.season_id, cmd.player_id, best)
        except RedisError:
            self._metrics.count("redis_publish_failed_total")
            return None

    async def submit_score(self, cmd: SubmitCommand, deadline: Deadline) -> SubmitResult:
        started = time.perf_counter()
        self._validate_submit(cmd)
        deadline.check()
        stored = await self._store_in_postgres(cmd)
        rank = None
        if not deadline.is_expired():
            rank = await self._publish_to_redis(cmd, stored.best)
        self._metrics.count("submit_total")
        if stored.duplicate:
            self._metrics.count("submit_duplicate_total")
        self._metrics.observe_latency_ms("submit", (time.perf_counter() - started) * 1000)
        return SubmitResult(stored.improved, stored.best.best_score, rank, stored.duplicate)

    # ---------- reads ----------

    async def _load_usernames(self, player_ids: List[int]) -> Dict[int, str]:
        try:
            async with self._pg_pool.connection() as conn:
                return await repository.get_usernames(conn, player_ids)
        except PoolTimeout:
            raise Overloaded("database is busy, retry later") from None

    async def _attach_usernames(self, players: List[redis_index.RankedPlayer]) -> List[LeaderboardEntry]:
        ids = [p.player_id for p in players]
        names = await self._usernames.get_many(ids, self._load_usernames)
        return [LeaderboardEntry(p.rank, p.player_id, names[p.player_id], p.score) for p in players]

    async def _load_top(self, season_id: int, limit: int) -> List[LeaderboardEntry]:
        players = await redis_index.get_top(self._redis, season_id, limit)
        return await self._attach_usernames(players)

    async def get_top(self, season_id: int, limit: int) -> List[LeaderboardEntry]:
        self._validate_limit(limit)
        cache_key = f"top:{season_id}:{limit}"
        loader = partial(self._load_top, season_id, limit)
        return await self._top_cache.get(cache_key, loader)

    async def get_rank(self, season_id: int, player_id: int) -> LeaderboardEntry:
        me = await redis_index.get_rank(self._redis, season_id, player_id)
        if me is None:
            raise PlayerNotFound(f"player {player_id} has no score in season {season_id}")
        return (await self._attach_usernames([me]))[0]

    async def get_around(self, season_id: int, player_id: int, radius: int) -> List[LeaderboardEntry]:
        self._validate_radius(radius)
        players = await redis_index.get_around(self._redis, season_id, player_id, radius)
        if not players:
            raise PlayerNotFound(f"player {player_id} has no score in season {season_id}")
        return await self._attach_usernames(players)
