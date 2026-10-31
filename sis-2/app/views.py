"""Django async views for the public REST API (reads only).

Boundary rule from the SIS #1 design:
  - Internal game servers write scores over gRPC (app/grpc_server.py).
  - Public clients read leaderboards over REST (/top, /rank, /around).
"""

import hashlib
import json
from dataclasses import asdict
from typing import List

from django.http import HttpRequest, HttpResponse, JsonResponse

from app.errors import InvalidArgument, Overloaded, PlayerNotFound
from app.runtime import get_runtime
from app.service import LeaderboardEntry

TOP_CACHE_CONTROL = "public, max-age=1, s-maxage=2, stale-while-revalidate=5"
PRIVATE_CACHE_CONTROL = "private, no-store"


def entries_to_list(entries: List[LeaderboardEntry]) -> list:
    return [asdict(entry) for entry in entries]


def compute_etag(payload) -> str:
    raw_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    digest = hashlib.sha1(raw_bytes).hexdigest()[:16]
    return f'"{digest}"'


def build_cached_json_response(request: HttpRequest, payload, cache_control: str) -> HttpResponse:
    """Returns JSON with Cache-Control and ETag; returns 304 Not Modified if If-None-Match matches."""
    etag = compute_etag(payload)
    if request.headers.get("If-None-Match") == etag:
        response = HttpResponse(status=304)
    else:
        response = JsonResponse(payload, safe=False)
    response["Cache-Control"] = cache_control
    response["ETag"] = etag
    return response


def map_domain_error(error: Exception) -> JsonResponse:
    if isinstance(error, InvalidArgument):
        return JsonResponse({"error": str(error)}, status=422)
    if isinstance(error, PlayerNotFound):
        return JsonResponse({"error": str(error)}, status=404)
    if isinstance(error, Overloaded):
        response = JsonResponse({"error": str(error)}, status=503)
        response["Retry-After"] = "1"
        return response
    raise error


def parse_int_param(request: HttpRequest, name: str, default_value: int) -> int:
    raw_value = request.GET.get(name)
    if raw_value is None:
        return default_value
    try:
        return int(raw_value)
    except ValueError:
        raise InvalidArgument(f"{name} must be an integer") from None


async def top_view(request: HttpRequest, season_id: int) -> HttpResponse:
    try:
        limit = parse_int_param(request, "limit", 100)
        runtime = await get_runtime()
        entries = await runtime.service.get_top(season_id, limit)
        return build_cached_json_response(request, entries_to_list(entries), TOP_CACHE_CONTROL)
    except (InvalidArgument, PlayerNotFound, Overloaded) as error:
        return map_domain_error(error)


async def rank_view(request: HttpRequest, season_id: int, player_id: int) -> HttpResponse:
    try:
        runtime = await get_runtime()
        entry = await runtime.service.get_rank(season_id, player_id)
        return build_cached_json_response(request, asdict(entry), PRIVATE_CACHE_CONTROL)
    except (InvalidArgument, PlayerNotFound, Overloaded) as error:
        return map_domain_error(error)


async def around_view(request: HttpRequest, season_id: int, player_id: int) -> HttpResponse:
    try:
        radius = parse_int_param(request, "radius", 5)
        runtime = await get_runtime()
        entries = await runtime.service.get_around(season_id, player_id, radius)
        return build_cached_json_response(request, entries_to_list(entries), PRIVATE_CACHE_CONTROL)
    except (InvalidArgument, PlayerNotFound, Overloaded) as error:
        return map_domain_error(error)


async def health_view(_request: HttpRequest) -> JsonResponse:
    return JsonResponse({"status": "ok"})


async def metrics_view(_request: HttpRequest) -> HttpResponse:
    runtime = await get_runtime()
    return HttpResponse(runtime.metrics.render_text(), content_type="text/plain")
