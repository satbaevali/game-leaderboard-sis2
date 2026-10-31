"""Score encoding for the Redis sorted set.

Redis sorts by one number. We want: higher score first, and for equal scores the
player who reached it EARLIER first. So we pack both into one number:

    composite = score * 2^32 + (2^32 - 1 - unix_seconds)

Bigger composite = better. A float holds integers exactly up to 2^53, so score
must stay below 2^21 (about 2.09 million). The config limit is 2,000,000.
"""

TIME_BITS = 32
TIME_MASK = (1 << TIME_BITS) - 1
MAX_SAFE_SCORE = (1 << 21) - 1


def encode_score(score: int, achieved_at_unix_seconds: int) -> float:
    earlier_is_better = TIME_MASK - achieved_at_unix_seconds
    composite = (score << TIME_BITS) + earlier_is_better
    return float(composite)


def decode_score(composite: float) -> int:
    return int(composite) >> TIME_BITS
