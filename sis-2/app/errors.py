"""Domain errors. Each transport (HTTP, gRPC) maps them to its own status codes."""


class LeaderboardError(Exception):
    """Base class for all expected errors."""


class PlayerNotFound(LeaderboardError):
    """Player does not exist, or has no score in this season."""


class InvalidArgument(LeaderboardError):
    """Request data is wrong. Retrying will never help."""


class Overloaded(LeaderboardError):
    """No free DB connection in time. Client may retry later."""


class DeadlineExceeded(LeaderboardError):
    """The caller's time budget is used up."""
