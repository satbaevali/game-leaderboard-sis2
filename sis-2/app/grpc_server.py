"""gRPC server for the internal SubmitScore call. Translates domain errors into gRPC status codes."""

from datetime import datetime, timezone
from typing import Optional

import grpc

from app.deadline import Deadline
from app.errors import DeadlineExceeded, InvalidArgument, Overloaded, PlayerNotFound
from app.generated import leaderboard_pb2, leaderboard_pb2_grpc
from app.service import LeaderboardService, SubmitCommand, SubmitResult

ERROR_TO_STATUS = [
    (InvalidArgument, grpc.StatusCode.INVALID_ARGUMENT),
    (PlayerNotFound, grpc.StatusCode.NOT_FOUND),
    (Overloaded, grpc.StatusCode.RESOURCE_EXHAUSTED),
    (DeadlineExceeded, grpc.StatusCode.DEADLINE_EXCEEDED),
]


def status_for(error: Exception) -> grpc.StatusCode:
    for error_type, status in ERROR_TO_STATUS:
        if isinstance(error, error_type):
            return status
    return grpc.StatusCode.INTERNAL


def to_command(request) -> SubmitCommand:
    started_at = datetime.fromtimestamp(request.match_started_at, tz=timezone.utc)
    return SubmitCommand(
        match_id=request.match_id,
        player_id=request.player_id,
        season_id=request.season_id,
        score=request.score,
        match_started_at=started_at,
    )


def to_response(result: SubmitResult):
    return leaderboard_pb2.SubmitScoreResponse(
        improved=result.improved,
        best_score=result.best_score,
        rank=result.rank or 0,
        duplicate=result.duplicate,
    )


def deadline_from_context(context, default_seconds: float) -> Deadline:
    """The client's deadline travels with the call; time_remaining() is None when the client set none."""
    return Deadline.from_optional_seconds(context.time_remaining(), default_seconds)


class ScoreServicer(leaderboard_pb2_grpc.ScoreServiceServicer):
    def __init__(self, service: LeaderboardService, default_deadline_s: float):
        self._service = service
        self._default_deadline_s = default_deadline_s

    async def SubmitScore(self, request, context):
        deadline = deadline_from_context(context, self._default_deadline_s)
        try:
            result = await self._service.submit_score(to_command(request), deadline)
        except (InvalidArgument, PlayerNotFound, Overloaded, DeadlineExceeded) as error:
            await context.abort(status_for(error), str(error))
        return to_response(result)


async def start_grpc_server(
    service: LeaderboardService, port: int, default_deadline_s: float
) -> grpc.aio.Server:
    server = grpc.aio.server()
    leaderboard_pb2_grpc.add_ScoreServiceServicer_to_server(
        ScoreServicer(service, default_deadline_s), server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    await server.start()
    return server
