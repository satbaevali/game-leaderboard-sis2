"""A deadline is an absolute point in time. It is checked before every expensive step."""

import time
from typing import Optional

from app.errors import DeadlineExceeded


class Deadline:
    def __init__(self, expires_at: float):
        self._expires_at = expires_at

    @classmethod
    def after_seconds(cls, seconds: float) -> "Deadline":
        return cls(time.monotonic() + seconds)

    @classmethod
    def from_optional_seconds(cls, seconds: Optional[float], default_seconds: float) -> "Deadline":
        if seconds is None:
            return cls.after_seconds(default_seconds)
        return cls.after_seconds(seconds)

    def remaining(self) -> float:
        return self._expires_at - time.monotonic()

    def is_expired(self) -> bool:
        return self.remaining() <= 0

    def check(self) -> None:
        if self.is_expired():
            raise DeadlineExceeded("deadline exceeded")
