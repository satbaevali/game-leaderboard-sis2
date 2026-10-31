#!/bin/bash
# Starts a local Postgres (port 54329) and Redis (port 63790) without Docker.
# Data lives in ./.data (git-ignored). Safe to run twice.
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="$PROJECT_DIR/.data"
PG_DIR="$DATA_DIR/pg"
REDIS_DIR="$DATA_DIR/redis"
PG_PORT=54329
REDIS_PORT=63790
BREW_BIN=/opt/homebrew/bin

# Postgres from brew keeps its server binaries in the keg, find them via pg_config.
PG_BIN="$("$BREW_BIN/pg_config" --bindir 2>/dev/null || echo "$BREW_BIN")"

start_postgres() {
  mkdir -p "$DATA_DIR"
  if [ ! -d "$PG_DIR" ]; then
    "$PG_BIN/initdb" -D "$PG_DIR" -U postgres --auth=trust >/dev/null
  fi
  if ! "$PG_BIN/pg_isready" -q -p "$PG_PORT"; then
    "$PG_BIN/pg_ctl" -D "$PG_DIR" -o "-p $PG_PORT -k /tmp" -l "$DATA_DIR/pg.log" -w start
  fi
  "$PG_BIN/createdb" -h localhost -p "$PG_PORT" -U postgres leaderboard 2>/dev/null || true
}

start_redis() {
  mkdir -p "$REDIS_DIR"
  if ! "$BREW_BIN/redis-cli" -p "$REDIS_PORT" ping >/dev/null 2>&1; then
    "$BREW_BIN/redis-server" --port "$REDIS_PORT" --dir "$REDIS_DIR" \
      --maxmemory-policy noeviction --appendonly yes \
      --daemonize yes --logfile "$DATA_DIR/redis.log"
  fi
}

start_postgres
start_redis
echo "postgres: postgresql://postgres@localhost:$PG_PORT/leaderboard"
echo "redis:    redis://localhost:$REDIS_PORT/0"
