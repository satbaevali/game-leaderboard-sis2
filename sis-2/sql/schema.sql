-- Schema from the SIS #1 design.

CREATE TABLE IF NOT EXISTS players (
    player_id  BIGINT PRIMARY KEY,
    username   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Best score per player per season. This is the source of truth for the leaderboard.
-- No index on best_score: ranking is done by Redis, so every write stays a cheap in-place update.
CREATE TABLE IF NOT EXISTS player_scores (
    season_id   INT NOT NULL,
    player_id   BIGINT NOT NULL REFERENCES players(player_id),
    best_score  BIGINT NOT NULL,
    achieved_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (season_id, player_id)
) WITH (fillfactor = 90);

-- Append-only log of every match result. PK (event_date, match_id, player_id) gives idempotency:
-- the same match reported twice is stored once. Partitioned by day so old days can be dropped fast.
CREATE TABLE IF NOT EXISTS score_events (
    event_date DATE NOT NULL,
    match_id   TEXT NOT NULL,
    player_id  BIGINT NOT NULL,
    season_id  INT NOT NULL,
    score      BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date, match_id, player_id)
) PARTITION BY RANGE (event_date);

CREATE INDEX IF NOT EXISTS score_events_created_at_brin
    ON score_events USING BRIN (created_at);
