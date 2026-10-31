"""Tiny in-process metrics: counters and a latency histogram. Exposed at /metrics as plain text."""

from collections import Counter
from typing import Dict, List

LATENCY_BUCKETS_MS = [1, 2, 5, 10, 20, 50, 100, 250, 1000]


class Metrics:
    def __init__(self):
        self._counters: Counter = Counter()
        self._latency_buckets: Dict[str, Counter] = {}

    def count(self, name: str, amount: int = 1) -> None:
        self._counters[name] += amount

    def observe_latency_ms(self, name: str, latency_ms: float) -> None:
        buckets = self._latency_buckets.setdefault(name, Counter())
        for upper_bound in LATENCY_BUCKETS_MS:
            if latency_ms <= upper_bound:
                buckets[upper_bound] += 1
                return
        buckets["+Inf"] += 1

    def render_text(self) -> str:
        lines: List[str] = []
        for name in sorted(self._counters):
            lines.append(f"{name} {self._counters[name]}")
        for name in sorted(self._latency_buckets):
            for bucket, value in self._latency_buckets[name].items():
                lines.append(f'{name}_latency_ms_bucket{{le="{bucket}"}} {value}')
        return "\n".join(lines) + "\n"
