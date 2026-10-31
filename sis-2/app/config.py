"""Settings read from environment variables, with the numbers from the SIS #1 design as defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str
    redis_url: str
    grpc_port: int
    http_port: int

    # Design: 13 DB connections per pod, fail fast when none is free.
    db_pool_size: int
    db_acquire_timeout_s: float
    statement_timeout_ms: int
    lock_timeout_ms: int
    idle_in_tx_timeout_ms: int

    top_cache_ttl_s: float
    names_cache_ttl_s: float
    max_top_limit: int
    max_around_radius: int
    max_score: int

    reconcile_interval_s: float
    reconcile_overlap_s: int
    maintenance_interval_s: float
    default_submit_deadline_s: float


def load_settings() -> Settings:
    return Settings(
        postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://postgres@localhost:54329/leaderboard"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:63790/0"),
        grpc_port=int(os.getenv("GRPC_PORT", "50051")),
        http_port=int(os.getenv("HTTP_PORT", "8099")),
        db_pool_size=int(os.getenv("DB_POOL_SIZE", "13")),
        db_acquire_timeout_s=float(os.getenv("DB_ACQUIRE_TIMEOUT_S", "0.010")),
        statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS", "30")),
        lock_timeout_ms=int(os.getenv("LOCK_TIMEOUT_MS", "20")),
        idle_in_tx_timeout_ms=int(os.getenv("IDLE_IN_TX_TIMEOUT_MS", "30000")),
        top_cache_ttl_s=float(os.getenv("TOP_CACHE_TTL_S", "1.0")),
        names_cache_ttl_s=float(os.getenv("NAMES_CACHE_TTL_S", "60")),
        max_top_limit=100,
        max_around_radius=50,
        max_score=2_000_000,
        reconcile_interval_s=float(os.getenv("RECONCILE_INTERVAL_S", "10")),
        reconcile_overlap_s=30,
        maintenance_interval_s=float(os.getenv("MAINTENANCE_INTERVAL_S", "3600")),
        default_submit_deadline_s=float(os.getenv("DEFAULT_SUBMIT_DEADLINE_S", "0.05")),
    )
