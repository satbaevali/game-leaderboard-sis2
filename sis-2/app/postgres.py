"""Postgres connection pool, schema setup and daily partition maintenance."""

from datetime import date, timedelta
from pathlib import Path
from typing import List

from psycopg_pool import AsyncConnectionPool

from app.config import Settings

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
PARTITIONS_AHEAD_DAYS = 7
PARTITIONS_BEHIND_DAYS = 1
KEEP_PARTITION_DAYS = 7


def build_session_options(settings: Settings) -> str:
    """Server-side safety nets: a slow query or a forgotten transaction is killed by Postgres itself."""
    return (
        f"-c statement_timeout={settings.statement_timeout_ms} "
        f"-c lock_timeout={settings.lock_timeout_ms} "
        f"-c idle_in_transaction_session_timeout={settings.idle_in_tx_timeout_ms}"
    )


async def open_pool(settings: Settings) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        conninfo=settings.postgres_dsn,
        min_size=settings.db_pool_size,
        max_size=settings.db_pool_size,
        timeout=settings.db_acquire_timeout_s,
        kwargs={"options": build_session_options(settings), "autocommit": False},
        open=False,
    )
    await pool.open(wait=True, timeout=10)
    return pool


async def open_admin_pool(settings: Settings) -> AsyncConnectionPool:
    """Pool without strict timeouts, for schema work, seeding and background jobs."""
    pool = AsyncConnectionPool(
        conninfo=settings.postgres_dsn,
        min_size=1,
        max_size=3,
        timeout=10,
        open=False,
    )
    await pool.open(wait=True, timeout=10)
    return pool


async def create_schema(pool: AsyncConnectionPool) -> None:
    schema_sql = SCHEMA_FILE.read_text()
    async with pool.connection() as conn:
        await conn.execute(schema_sql)


def partition_name(day: date) -> str:
    return "score_events_" + day.strftime("%Y%m%d")


def days_to_keep(today: date) -> List[date]:
    first = today - timedelta(days=PARTITIONS_BEHIND_DAYS)
    last = today + timedelta(days=PARTITIONS_AHEAD_DAYS)
    count = (last - first).days + 1
    return [first + timedelta(days=offset) for offset in range(count)]


def create_partition_sql(day: date) -> str:
    next_day = day + timedelta(days=1)
    return (
        f"CREATE TABLE IF NOT EXISTS {partition_name(day)} "
        f"PARTITION OF score_events "
        f"FOR VALUES FROM ('{day.isoformat()}') TO ('{next_day.isoformat()}')"
    )


async def ensure_partitions(pool: AsyncConnectionPool, today: date) -> None:
    async with pool.connection() as conn:
        for day in days_to_keep(today):
            await conn.execute(create_partition_sql(day))


async def list_partition_names(pool: AsyncConnectionPool) -> List[str]:
    query = (
        "SELECT c.relname FROM pg_inherits i "
        "JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent "
        "WHERE p.relname = 'score_events'"
    )
    async with pool.connection() as conn:
        cursor = await conn.execute(query)
        rows = await cursor.fetchall()
    return [row[0] for row in rows]


def is_expired_partition(name: str, today: date) -> bool:
    day_text = name.removeprefix("score_events_")
    day = date(int(day_text[0:4]), int(day_text[4:6]), int(day_text[6:8]))
    return day < today - timedelta(days=KEEP_PARTITION_DAYS)


async def drop_old_partitions(pool: AsyncConnectionPool, today: date) -> List[str]:
    """DROP of a whole day is instant, unlike DELETE of millions of rows (no dead tuples, no vacuum)."""
    dropped = []
    for name in await list_partition_names(pool):
        if is_expired_partition(name, today):
            async with pool.connection() as conn:
                await conn.execute(f"DROP TABLE {name}")
            dropped.append(name)
    return dropped
