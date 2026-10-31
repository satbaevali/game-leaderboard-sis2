"""Load test: many concurrent gRPC submits + REST top reads. Prints real measured numbers.

Usage: .venv/bin/python -m scripts.loadtest --seconds 10 --writers 50 --readers 20
Note: one Python process on a laptop will NOT reach the design target of 5,000 RPS;
the printed numbers are what this machine really achieves.
"""

import argparse
import asyncio
import random
import time
from collections import Counter
from typing import List

import grpc
import httpx

from client.submit_client import ScoreClient

TOP_URL = "http://localhost:8099/api/v1/leaderboards/1/top?limit=100"
GRPC_TARGET = "localhost:50051"
PLAYER_COUNT = 100_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--writers", type=int, default=50)
    parser.add_argument("--readers", type=int, default=20)
    return parser.parse_args()


def percentile(sorted_values: List[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(len(sorted_values) * fraction))
    return sorted_values[index]


async def write_worker(client: ScoreClient, stop_at: float, latencies: List[float], errors: Counter):
    while time.monotonic() < stop_at:
        player_id = random.randint(1, PLAYER_COUNT)
        started = time.perf_counter()
        try:
            await client.submit_score(player_id, 1, random.randint(0, 1_000_000))
            latencies.append((time.perf_counter() - started) * 1000)
        except grpc.aio.AioRpcError as error:
            errors[error.code().name] += 1
        except TimeoutError:
            errors["TOTAL_DEADLINE"] += 1


async def read_worker(http: httpx.AsyncClient, stop_at: float, latencies: List[float], errors: Counter):
    while time.monotonic() < stop_at:
        started = time.perf_counter()
        response = await http.get(TOP_URL)
        if response.status_code == 200:
            latencies.append((time.perf_counter() - started) * 1000)
        else:
            errors[f"HTTP_{response.status_code}"] += 1


def print_report(name: str, latencies: List[float], errors: Counter, seconds: float) -> None:
    ordered = sorted(latencies)
    print(f"{name}: ok={len(ordered)} rps={len(ordered) / seconds:.0f} errors={dict(errors)}")
    print(f"   p50={percentile(ordered, 0.50):.1f} ms  p95={percentile(ordered, 0.95):.1f} ms  "
          f"p99={percentile(ordered, 0.99):.1f} ms")


async def main() -> None:
    args = parse_args()
    client = ScoreClient(GRPC_TARGET)
    await client.connect()
    write_latencies, write_errors = [], Counter()
    read_latencies, read_errors = [], Counter()
    stop_at = time.monotonic() + args.seconds
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=args.readers)) as http:
        writers = [write_worker(client, stop_at, write_latencies, write_errors) for _ in range(args.writers)]
        readers = [read_worker(http, stop_at, read_latencies, read_errors) for _ in range(args.readers)]
        await asyncio.gather(*writers, *readers)
    print_report("submit (gRPC)", write_latencies, write_errors, args.seconds)
    print_report("top-100 (REST)", read_latencies, read_errors, args.seconds)
    print(f"client retries={client.metrics.retries} statuses={dict(client.metrics.status_counts)}")
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
