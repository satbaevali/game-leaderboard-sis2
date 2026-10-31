"""Smoke demo: submit scores over gRPC, read them back over REST, show idempotency.

Usage: .venv/bin/python -m scripts.demo
"""

import asyncio
import uuid

import httpx

from client.submit_client import ScoreClient

REST_URL = "http://localhost:8099/api/v1/leaderboards/1"
GRPC_TARGET = "localhost:50051"
DEMO_PLAYER = 42


def print_response(title: str, response) -> None:
    print(f"{title}: {response.status_code} {response.text[:300]}")


async def submit_and_show(client: ScoreClient, score: int, match_id: str) -> None:
    response = await client.submit_score(DEMO_PLAYER, 1, score, match_id=match_id)
    print(f"  submit score={score}: improved={response.improved} best={response.best_score} "
          f"rank={response.rank} duplicate={response.duplicate}")


async def show_reads(http: httpx.AsyncClient) -> None:
    print_response("top 3", await http.get(f"{REST_URL}/top", params={"limit": 3}))
    print_response("rank", await http.get(f"{REST_URL}/players/{DEMO_PLAYER}/rank"))
    print_response("around", await http.get(f"{REST_URL}/players/{DEMO_PLAYER}/around", params={"radius": 1}))


async def main() -> None:
    client = ScoreClient(GRPC_TARGET)
    await client.connect()
    first_match = str(uuid.uuid4())
    print("1) first submit, then the SAME match_id again (retry) -> duplicate")
    await submit_and_show(client, 1_500_000, first_match)
    await submit_and_show(client, 1_500_000, first_match)
    print("2) lower score from a new match does not reduce the best")
    await submit_and_show(client, 10, str(uuid.uuid4()))
    async with httpx.AsyncClient() as http:
        await show_reads(http)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
