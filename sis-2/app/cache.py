"""In-process caches (the "L1 Caffeine" box of the design).

CoalescingTtlCache: when a key is missing or expired and 250 requests ask at once,
only ONE of them runs the loader; the others await the same result (anti-stampede).
"""

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

Loader = Callable[[], Awaitable[Any]]

MAX_ENTRIES = 10_000


def jittered_ttl(ttl_s: float) -> float:
    """+-10% randomness so many keys do not all expire at the same moment."""
    return ttl_s * (0.9 + 0.2 * random.random())


class CoalescingTtlCache:
    def __init__(self, ttl_s: float):
        self._ttl_s = ttl_s
        self._values: Dict[str, Tuple[float, Any]] = {}
        self._in_flight: Dict[str, asyncio.Task] = {}

    def _fresh_value(self, key: str) -> Optional[Tuple[Any]]:
        entry = self._values.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at <= time.monotonic():
            return None
        return (value,)

    def _store(self, key: str, value: Any) -> None:
        if len(self._values) >= MAX_ENTRIES:
            self._values.clear()
        self._values[key] = (time.monotonic() + jittered_ttl(self._ttl_s), value)

    async def _load_and_store(self, key: str, loader: Loader) -> Any:
        try:
            value = await loader()
            self._store(key, value)
            return value
        finally:
            self._in_flight.pop(key, None)

    async def get(self, key: str, loader: Loader) -> Any:
        fresh = self._fresh_value(key)
        if fresh is not None:
            return fresh[0]
        task = self._in_flight.get(key)
        if task is None:
            task = asyncio.ensure_future(self._load_and_store(key, loader))
            self._in_flight[key] = task
        # shield: one waiter being cancelled must not cancel the shared load
        return await asyncio.shield(task)


class UsernameCache:
    """player_id -> username. Missing ids are loaded with ONE batched query, not one per id."""

    def __init__(self, ttl_s: float):
        self._ttl_s = ttl_s
        self._names: Dict[int, Tuple[float, str]] = {}

    def _cached_name(self, player_id: int) -> Optional[str]:
        entry = self._names.get(player_id)
        if entry is None or entry[0] <= time.monotonic():
            return None
        return entry[1]

    def _remember(self, player_id: int, name: str) -> None:
        if len(self._names) >= MAX_ENTRIES * 10:
            self._names.clear()
        self._names[player_id] = (time.monotonic() + jittered_ttl(self._ttl_s), name)

    def _missing_ids(self, player_ids: Iterable[int]) -> List[int]:
        return [pid for pid in player_ids if self._cached_name(pid) is None]

    async def get_many(
        self,
        player_ids: List[int],
        batch_loader: Callable[[List[int]], Awaitable[Dict[int, str]]],
    ) -> Dict[int, str]:
        missing = self._missing_ids(player_ids)
        if missing:
            loaded = await batch_loader(missing)
            for player_id, name in loaded.items():
                self._remember(player_id, name)
        return {pid: self._cached_name(pid) or "unknown" for pid in player_ids}
