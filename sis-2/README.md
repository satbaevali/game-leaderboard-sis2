# Game Leaderboard — SIS #2 Implementation of SIS #1 Design (Django ASGI + gRPC + PostgreSQL + Redis ZSET)

Python 3.14 + **Django (ASGI async views)** · **gRPC (`grpc.aio`)** · **PostgreSQL 16 (source of truth)** · **Redis ZSET (O(log N) ranking index)**

---

## 1. Quick Start (Run Everything Locally)

```bash
# 1. Create virtualenv and install dependencies
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 2. Start local PostgreSQL (:54329) and Redis (:63790, noeviction + AOF)
bash scripts/start_infra.sh

# 3. Generate gRPC code from proto/leaderboard.proto
bash scripts/generate_proto.sh

# 4. Seed 100,000 players and build the Redis ZSET index
.venv/bin/python -m scripts.seed --players 100000 --season 1

# 5. Start the Django ASGI server (REST on :8099 + gRPC on :50051)
.venv/bin/python -m uvicorn leaderboard_project.asgi:application --port 8099

# 6. Run the smoke demo (submit via gRPC client, read via Django REST)
.venv/bin/python -m scripts.demo

# 7. Run all 20 automated tests (unit + integration against real Postgres & Redis)
.venv/bin/pytest -q

# 8. Run the load test (50 concurrent gRPC writers + 20 concurrent REST readers)
.venv/bin/python -m scripts.loadtest --seconds 10 --writers 50 --readers 20
```

---

## 2. Technology Choices & Justification (gRPC vs REST/JSON, Django ASGI, Postgres + Redis)

### Why **gRPC (Protobuf over HTTP/2)** for internal `SubmitScore` instead of REST/JSON?
- **Caller:** Internal game servers reporting match results at **5,000 RPS** (service-to-service boundary).
- **Comparison with REST/JSON:**
  1. **Wire size & CPU serialization (~64 B vs ~180 B):** In Protobuf ([leaderboard.proto](proto/leaderboard.proto)), integers (`player_id`, `season_id`, `score`, `match_started_at`) are encoded as compact binary `varint` and field names are replaced with field tags (`1..5`). The entire request is ~64 bytes vs ~180 bytes of text JSON, saving CPU parsing overhead across 5,000 writes/sec.
  2. **HTTP/2 multiplexing on a single connection:** One long-lived `grpc.aio` channel multiplexes thousands of concurrent in-flight RPCs without application-level head-of-line blocking or repeated TCP + TLS handshakes.
  3. **Native Deadline Propagation:** The client sets `timeout=200ms` per attempt; gRPC automatically transmits the remaining budget via the `grpc-timeout` wire header, and the server reads `context.time_remaining()` in [grpc_server.py](app/grpc_server.py) to abort work if the caller's deadline has already expired. REST/JSON has no standard deadline header.
  4. **Strict schema evolution:** Field numbers and types are enforced by `protoc`.

### Why **REST / JSON (Django async views)** for public reads (`/top`, `/rank`, `/around`) instead of gRPC?
- **Caller:** External browsers, mobile clients, and CDN edge nodes (public boundary).
- **Comparison with gRPC:**
  1. **HTTP/CDN Caching (99.9% offload of `GET /top`):** `GET /api/v1/leaderboards/{season_id}/top?limit=100` sets `Cache-Control: public, max-age=1, s-maxage=2, stale-while-revalidate=5` and an `ETag` header (`304 Not Modified` on match) in [views.py](app/views.py). A CDN caches the top-100 response for 1–2 seconds, absorbing almost all 2,000 RPS of read traffic before it reaches the origin pods. gRPC uses `HTTP POST`, which CDNs and browsers do not cache by default.
  2. **Universal browser & mobile support:** Browsers natively speak `GET` + JSON without needing a `gRPC-Web` translation proxy.
  3. **Private caching headers for personalized endpoints:** `/rank` and `/around` return `Cache-Control: private, no-store` so a shared CDN never leaks one player's rank to another.

### Why **Django 5/6 ASGI (`async def` views)** + `asyncio`?
- Standard synchronous WSGI blocks an OS thread on every database or Redis call.
- Running Django under **ASGI (`leaderboard_project.asgi:application`)** with `async def` views in [views.py](app/views.py) lets a single Python process handle hundreds of concurrent requests in one event loop alongside `psycopg_pool.AsyncConnectionPool`, `redis.asyncio`, and `grpc.aio`.

### Why **PostgreSQL AND Redis ZSET** together?
- **PostgreSQL** is the durable **source of truth** (`players`, `player_scores`, partitioned `score_events`). Calculating a player's rank among 10,000,000 rows in SQL (`COUNT(*) WHERE best_score > X`) is $O(N)$ and too slow at 2,000 RPS.
- **Redis Sorted Set (`ZSET`)** is an in-memory **ranking index** (`lb:season:{id}`). `ZADD GT` and `ZREVRANK` run in $O(\log N)$ (~0.1 ms) via skip-list + hash table.
- Because Redis is only an index, we always **commit to PostgreSQL FIRST** and update Redis **AFTER** the DB transaction closes. If Redis is wiped or unreachable, `rebuild_season()` and the background reconciler in [background_jobs.py](app/background_jobs.py) reconstruct the index from PostgreSQL.

---

## 3. SIS #1 Design → Code Map

| SIS #1 Design Element | Implementation File |
|---|---|
| Django URL routing (`/top`, `/rank`, `/around`, `/health`, `/metrics`) | [app/urls.py](app/urls.py), [leaderboard_project/urls.py](leaderboard_project/urls.py) |
| Django async views + `Cache-Control` + `ETag` (`304 Not Modified`) | [app/views.py](app/views.py) |
| Django ASGI lifecycle (starts DB pools, Redis, gRPC server, reconciler) | [leaderboard_project/asgi.py](leaderboard_project/asgi.py), [app/runtime.py](app/runtime.py) |
| Internal gRPC `ScoreService.SubmitScore` + deadline propagation | [proto/leaderboard.proto](proto/leaderboard.proto), [app/grpc_server.py](app/grpc_server.py) |
| Redis ZSET `lb:season:{id}`, `ZADD GT`, `ZREVRANK`, `ZREVRANGE` | [app/redis_index.py](app/redis_index.py) |
| Composite score + earlier-timestamp tie-break in 53-bit float | [app/scoring.py](app/scoring.py) |
| Postgres schema (`fillfactor = 90`, daily range partitions, BRIN index, no index on `best_score`) | [sql/schema.sql](sql/schema.sql) |
| Atomic `ON CONFLICT DO UPDATE WHERE best_score < EXCLUDED.best_score` + idempotent `score_events` | [app/repository.py](app/repository.py) |
| Postgres commit FIRST, Redis AFTER (no Redis call inside DB transaction) | [app/service.py](app/service.py) (`submit_score`) |
| L1 in-memory cache (1s TTL for `/top`) with single-flight request coalescing + $\pm 10\%$ TTL jitter | [app/cache.py](app/cache.py) (`CoalescingTtlCache`) |
| Batched username lookup (`WHERE player_id = ANY(%s)`, no N+1 queries) + 60s L1 cache | [app/cache.py](app/cache.py) (`UsernameCache`), [app/repository.py](app/repository.py) |
| 13-connection DB pool (Bulkhead) + 10 ms fail-fast acquire timeout + `statement_timeout=30ms`, `lock_timeout=20ms` | [app/postgres.py](app/postgres.py), [app/config.py](app/config.py) |
| Background reconciler (every 10s with 30s overlap) + daily partition creation & 7-day `DROP TABLE` | [app/background_jobs.py](app/background_jobs.py), [app/postgres.py](app/postgres.py) |

---

## 4. SIS #2 Production Client Checklist (Lecture 3, Slide 34) → [client/submit_client.py](client/submit_client.py)

1. **Sized connection pool:** Reuses a single `grpc.aio` HTTP/2 channel across all calls; server DB pool is strictly capped at 13 connections with a 10 ms acquire timeout.
2. **Explicit connect + request timeouts:** Connect timeout = `2.0 s` (`wait_for(channel_ready())`); per-attempt RPC timeout = `200 ms`.
3. **Deadline propagation & budget check:** Total deadline = `1.0 s`; remaining budget is checked before every attempt and before sleeping; gRPC propagates the deadline to the server.
4. **Retries with full jitter, cap & retry budget:** Max 3 attempts, ONLY on retryable statuses (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`), never on `INVALID_ARGUMENT` or `NOT_FOUND`. Exponential backoff `uniform(0, min(2.0s, 0.05s * 2^attempt))` plus a token-bucket `RetryBudget` (~10% max extra retry load).
5. **Idempotency key on writes:** `match_id` (UUID) is generated **once** before the retry loop and reused across all retries; PostgreSQL `PRIMARY KEY (event_date, match_id, player_id)` guarantees `ON CONFLICT DO NOTHING`.
6. **Per-call metrics:** Client tracks latency histogram buckets, status code counters, and retry count (`CallMetrics`); server exposes `/metrics`.

---

## 5. Measured Benchmark Results (Honest Local Numbers)

Measured on a single laptop (1 Python ASGI process, 50 concurrent gRPC writers + 20 concurrent REST readers, 100,000 seeded players in Postgres & Redis on the same machine):

| Endpoint | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| `SubmitScore` (gRPC) | ~810 – 1,400 RPS | 34 – 55 ms | 43 – 101 ms | 89 – 167 ms |
| `GET /top?limit=100` (Django REST) | ~210 – 255 RPS | 31 – 37 ms | 272 – 354 ms | 492 – 729 ms |

*Why is this lower than the SIS #1 cluster target (5,000 write RPS, p99 < 50 ms)?*
In the SIS #1 design, traffic is served by 4 multi-core pods, an Envoy L7 load balancer, PgBouncer, Redis replicas, and a CDN that offloads 99.9% of `/top` reads. Here, the load generator, Django ASGI server, gRPC server, PostgreSQL, and Redis all share a single laptop and a single Python GIL. Under heavy contention, excess requests fail fast with `RESOURCE_EXHAUSTED` / `503 Overloaded` (bulkhead working as designed) rather than crashing the database.

---

## 6. Oral Defense Q&A Cheat-Sheet (How to Explain Every Line to Arnur)

- **Why both PostgreSQL and Redis?**
  PostgreSQL is our durable source of truth. Redis `ZSET` is our fast in-memory ranking index ($O(\log N)$ via skip-list). If Redis crashes, we rebuild it from PostgreSQL (`rebuild_season`).
- **Why commit to PostgreSQL first and update Redis second (outside the transaction)?**
  If we updated Redis first and PostgreSQL rolled back, the leaderboard would show phantom scores. And if we called Redis *inside* the PostgreSQL transaction, a slow Redis call would hold one of our 13 DB connections and starve the pool.
- **What if PostgreSQL commits, and then Redis fails or times out?**
  `submit_score` catches `RedisError`, increments `redis_publish_failed_total`, and still returns `OK` (`rank=0` unknown) to the caller because the score is safely saved in PostgreSQL. Within 10 seconds, `reconcile_once()` scans `score_events` and pushes `ZADD GT` to Redis.
- **Why `ZADD GT` in Redis and `WHERE player_scores.best_score < EXCLUDED.best_score` in SQL?**
  Both guarantee monotonicity: a player's best score can only ever increase. Late, duplicate, or out-of-order packets can never overwrite a higher score with a lower one.
- **How does tie-breaking work when two players have the same score?**
  In [scoring.py](app/scoring.py), we pack score and timestamp into one 53-bit exact float: `(score << 32) + (2^32 - 1 - unix_seconds)`. Earlier timestamps produce a larger lower-32-bit value, so the player who reached the score first ranks higher.
- **How does idempotency work on retries?**
  The client generates `match_id = uuid4()` **once** before the retry loop ([submit_client.py](client/submit_client.py)). In PostgreSQL, `score_events` has `PRIMARY KEY (event_date, match_id, player_id)` and uses `INSERT ... ON CONFLICT DO NOTHING RETURNING created_at`. If `created_at` is `None`, it's a duplicate retry and we skip updating `player_scores`.
- **How do you prevent Cache Stampede on `/top`?**
  In [cache.py](app/cache.py), `CoalescingTtlCache` stores an in-flight `asyncio.Task` per key. When the 1-second TTL expires and 250 requests arrive at once, only the first request queries Redis; the other 249 `await` the same task. We also add $\pm 10\%$ random jitter to TTLs so keys don't expire together.
- **How do you avoid the N+1 query problem for usernames?**
  `UsernameCache.get_many` collects all missing `player_id`s from a top-100 list and runs **one** SQL query: `SELECT player_id, username FROM players WHERE player_id = ANY(%s)`.
- **Why partition `score_events` by day?**
  Deleting millions of old rows with `DELETE` creates dead tuples and vacuum bloat. With daily range partitions, `DROP TABLE score_events_YYYYMMDD` frees disk space instantaneously.
