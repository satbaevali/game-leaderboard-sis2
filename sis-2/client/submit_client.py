"""Production-style gRPC client for SubmitScore. It implements the SIS #2 checklist:

  1. ONE reused channel (connection pool of size 1; HTTP/2 multiplexes calls on it)
  2. explicit connect timeout and per-call request timeout
  3. a total deadline; remaining time is checked before every attempt and every sleep
  4. retries: max 3 attempts, only retryable errors, exponential backoff with full jitter and a cap,
     plus a retry budget so retries never exceed ~10% extra load
  5. idempotency key (match_id) created ONCE per logical operation, reused on every retry
  6. metrics: latency histogram, results by status, retry count
"""

import asyncio
import random
import time
import uuid
from collections import Counter
from typing import Optional

import grpc

from app.generated import leaderboard_pb2, leaderboard_pb2_grpc

MAX_ATTEMPTS = 3
BACKOFF_BASE_S = 0.05
BACKOFF_CAP_S = 2.0
CONNECT_TIMEOUT_S = 2.0
REQUEST_TIMEOUT_S = 0.2
TOTAL_DEADLINE_S = 1.0
RETRYABLE_STATUSES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}
LATENCY_BUCKETS_MS = [5, 10, 20, 50, 100, 250, 1000]


def backoff_with_full_jitter(attempt: int) -> float:
    """Random wait between 0 and min(cap, base * 2^attempt). Randomness stops clients retrying in sync."""
    ceiling = min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2**attempt))
    return random.uniform(0, ceiling)


class RetryBudget:
    """Every request adds a little credit, every retry spends one. Under a full outage the credit
    runs out and retries stop, so we do not multiply the load on a struggling server."""

    def __init__(self, credit_per_request: float = 0.1, start: float = 10.0, cap: float = 100.0):
        self._credit_per_request = credit_per_request
        self._tokens = start
        self._cap = cap

    def record_request(self) -> None:
        self._tokens = min(self._cap, self._tokens + self._credit_per_request)

    def try_spend(self) -> bool:
        if self._tokens < 1.0:
            return False
        self._tokens -= 1.0
        return True


class CallMetrics:
    def __init__(self):
        self.status_counts: Counter = Counter()
        self.latency_buckets: Counter = Counter()
        self.retries = 0

    def record_attempt(self, status_name: str, latency_ms: float) -> None:
        self.status_counts[status_name] += 1
        for upper_bound in LATENCY_BUCKETS_MS:
            if latency_ms <= upper_bound:
                self.latency_buckets[upper_bound] += 1
                return
        self.latency_buckets["+Inf"] += 1

    def record_retry(self) -> None:
        self.retries += 1


class ScoreClient:
    def __init__(self, target: str):
        self._channel = grpc.aio.insecure_channel(target)
        self._stub = leaderboard_pb2_grpc.ScoreServiceStub(self._channel)
        self._budget = RetryBudget()
        self.metrics = CallMetrics()

    async def connect(self) -> None:
        await asyncio.wait_for(self._channel.channel_ready(), CONNECT_TIMEOUT_S)

    async def close(self) -> None:
        await self._channel.close()

    def _build_request(self, match_id: str, player_id: int, season_id: int, score: int):
        return leaderboard_pb2.SubmitScoreRequest(
            match_id=match_id,
            player_id=player_id,
            season_id=season_id,
            score=score,
            match_started_at=int(time.time()),
        )

    async def _attempt_once(self, request, timeout_s: float):
        started = time.perf_counter()
        try:
            response = await self._stub.SubmitScore(request, timeout=timeout_s)
            self.metrics.record_attempt("OK", (time.perf_counter() - started) * 1000)
            return response, None
        except grpc.aio.AioRpcError as error:
            self.metrics.record_attempt(error.code().name, (time.perf_counter() - started) * 1000)
            return None, error

    def _should_retry(self, error: grpc.aio.AioRpcError, attempt: int) -> bool:
        if error.code() not in RETRYABLE_STATUSES:
            return False
        if attempt + 1 >= MAX_ATTEMPTS:
            return False
        return self._budget.try_spend()

    async def submit_score(
        self,
        player_id: int,
        season_id: int,
        score: int,
        match_id: Optional[str] = None,
        total_deadline_s: float = TOTAL_DEADLINE_S,
    ):
        match_id = match_id or str(uuid.uuid4())  # created ONCE, outside the retry loop
        request = self._build_request(match_id, player_id, season_id, score)
        deadline = time.monotonic() + total_deadline_s
        self._budget.record_request()
        for attempt in range(MAX_ATTEMPTS):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("total deadline used up before the call")
            response, error = await self._attempt_once(request, min(REQUEST_TIMEOUT_S, remaining))
            if error is None:
                return response
            if not self._should_retry(error, attempt):
                raise error
            pause = backoff_with_full_jitter(attempt)
            if pause >= deadline - time.monotonic():
                raise error  # sleeping would burn the whole remaining budget
            self.metrics.record_retry()
            await asyncio.sleep(pause)
        raise TimeoutError("unreachable")
