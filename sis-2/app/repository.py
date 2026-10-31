"""All SQL lives here. Every function takes a connection, so the caller owns the transaction."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional

import psycopg
from psycopg import AsyncConnection

from app.errors import PlayerNotFound

INSERT_EVENT_SQL = """
INSERT INTO score_events (event_date, match_id, player_id, season_id, score)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
RETURNING created_at
"""

# One atomic statement: no read-then-write race between concurrent submits.
UPSERT_BEST_SQL = """
INSERT INTO player_scores (season_id, player_id, best_score, achieved_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (season_id, player_id) DO UPDATE
   SET best_score = EXCLUDED.best_score, achieved_at = EXCLUDED.achieved_at
 WHERE player_scores.best_score < EXCLUDED.best_score
RETURNING best_score
"""

GET_BEST_SQL = """
SELECT best_score, achieved_at FROM player_scores
WHERE season_id = %s AND player_id = %s
"""

USERNAMES_SQL = "SELECT player_id, username FROM players WHERE player_id = ANY(%s)"

EVENTS_SINCE_SQL = """
SELECT DISTINCT season_id, player_id FROM score_events
WHERE created_at >= %s
"""

SCORES_PAGE_SQL = """
SELECT player_id, best_score, achieved_at FROM player_scores
WHERE season_id = %s AND player_id > %s
ORDER BY player_id
LIMIT %s
"""


@dataclass(frozen=True)
class BestScore:
    best_score: int
    achieved_at: datetime


@dataclass(frozen=True)
class StoredResult:
    duplicate: bool
    improved: bool
    best: BestScore


async def insert_event(
    conn: AsyncConnection, event_date: date, match_id: str, player_id: int, season_id: int, score: int
) -> Optional[datetime]:
    """Returns created_at, or None when this match was already stored (duplicate)."""
    params = (event_date, match_id, player_id, season_id, score)
    try:
        cursor = await conn.execute(INSERT_EVENT_SQL, params)
    except psycopg.errors.ForeignKeyViolation:
        raise PlayerNotFound(f"player {player_id} not found") from None
    row = await cursor.fetchone()
    return row[0] if row else None


async def upsert_best(
    conn: AsyncConnection, season_id: int, player_id: int, score: int, achieved_at: datetime
) -> bool:
    """True when the best score really changed."""
    params = (season_id, player_id, score, achieved_at)
    try:
        cursor = await conn.execute(UPSERT_BEST_SQL, params)
    except psycopg.errors.ForeignKeyViolation:
        raise PlayerNotFound(f"player {player_id} not found") from None
    return await cursor.fetchone() is not None


async def get_best(conn: AsyncConnection, season_id: int, player_id: int) -> Optional[BestScore]:
    cursor = await conn.execute(GET_BEST_SQL, (season_id, player_id))
    row = await cursor.fetchone()
    if row is None:
        return None
    return BestScore(best_score=row[0], achieved_at=row[1])


async def store_score(
    conn: AsyncConnection,
    event_date: date,
    match_id: str,
    player_id: int,
    season_id: int,
    score: int,
) -> StoredResult:
    """Event + best-score update in ONE transaction (the caller commits). Duplicate match -> no change."""
    created_at = await insert_event(conn, event_date, match_id, player_id, season_id, score)
    duplicate = created_at is None
    improved = False
    if not duplicate:
        improved = await upsert_best(conn, season_id, player_id, score, created_at)
    best = await get_best(conn, season_id, player_id)
    return StoredResult(duplicate=duplicate, improved=improved, best=best)


async def get_usernames(conn: AsyncConnection, player_ids: List[int]) -> Dict[int, str]:
    cursor = await conn.execute(USERNAMES_SQL, (player_ids,))
    rows = await cursor.fetchall()
    return {row[0]: row[1] for row in rows}


async def players_changed_since(conn: AsyncConnection, since: datetime) -> List[tuple]:
    cursor = await conn.execute(EVENTS_SINCE_SQL, (since,))
    return await cursor.fetchall()


async def get_scores_page(
    conn: AsyncConnection, season_id: int, after_player_id: int, page_size: int
) -> List[tuple]:
    cursor = await conn.execute(SCORES_PAGE_SQL, (season_id, after_player_id, page_size))
    return await cursor.fetchall()


async def list_season_ids(conn: AsyncConnection) -> List[int]:
    cursor = await conn.execute("SELECT DISTINCT season_id FROM player_scores")
    rows = await cursor.fetchall()
    return [row[0] for row in rows]
