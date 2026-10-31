"""Fast tests that need no database or Redis."""

import asyncio
import time

import pytest

from app.cache import CoalescingTtlCache, UsernameCache
from app.deadline import Deadline
from app.errors import DeadlineExceeded
from app.scoring import MAX_SAFE_SCORE, decode_score, encode_score
from client.submit_client import BACKOFF_CAP_S, RetryBudget, backoff_with_full_jitter

NOW = 1_800_000_000


def test_higher_score_ranks_higher():
    assert encode_score(200, NOW) > encode_score(100, NOW)


def test_equal_score_earlier_player_ranks_higher():
    earlier = encode_score(100, NOW)
    later = encode_score(100, NOW + 60)
    assert earlier > later


def test_score_wins_over_time():
    slow_but_better = encode_score(101, NOW + 10_000)
    fast_but_worse = encode_score(100, NOW)
    assert slow_but_better > fast_but_worse


def test_decode_returns_original_score():
    assert decode_score(encode_score(123_456, NOW)) == 123_456


def test_max_score_is_exact_in_float():
    composite = encode_score(MAX_SAFE_SCORE, NOW)
    assert int(composite) == (MAX_SAFE_SCORE << 32) + (2**32 - 1 - NOW)


def test_deadline_expires():
    deadline = Deadline.after_seconds(0.01)
    time.sleep(0.02)
    with pytest.raises(DeadlineExceeded):
        deadline.check()


def test_cache_runs_loader_once_for_many_concurrent_callers():
    calls = []

    async def slow_loader():
        calls.append(1)
        await asyncio.sleep(0.05)
        return "value"

    async def scenario():
        cache = CoalescingTtlCache(ttl_s=5)
        results = await asyncio.gather(*[cache.get("k", slow_loader) for _ in range(250)])
        return results

    results = asyncio.run(scenario())
    assert results == ["value"] * 250
    assert len(calls) == 1


def test_cache_serves_second_call_from_memory():
    calls = []

    async def loader():
        calls.append(1)
        return 7

    async def scenario():
        cache = CoalescingTtlCache(ttl_s=5)
        await cache.get("k", loader)
        return await cache.get("k", loader)

    assert asyncio.run(scenario()) == 7
    assert len(calls) == 1


def test_usernames_loaded_in_one_batch():
    batches = []

    async def batch_loader(ids):
        batches.append(list(ids))
        return {pid: f"name{pid}" for pid in ids}

    async def scenario():
        names = UsernameCache(ttl_s=60)
        first = await names.get_many([1, 2, 3], batch_loader)
        await names.get_many([2, 3, 4], batch_loader)
        return first

    assert asyncio.run(scenario()) == {1: "name1", 2: "name2", 3: "name3"}
    assert batches == [[1, 2, 3], [4]]


def test_backoff_never_exceeds_cap():
    for attempt in range(20):
        assert 0 <= backoff_with_full_jitter(attempt) <= BACKOFF_CAP_S


def test_retry_budget_runs_out():
    budget = RetryBudget(credit_per_request=0.1, start=2.0, cap=10.0)
    assert budget.try_spend()
    assert budget.try_spend()
    assert not budget.try_spend()


def test_retry_budget_refills_with_traffic():
    budget = RetryBudget(credit_per_request=0.1, start=0.0, cap=10.0)
    for _ in range(11):  # 10 * 0.1 is 0.9999999999999999 in floating point, so use 11
        budget.record_request()
    assert budget.try_spend()
