"""Shared PostgreSQL connection and the tables used by the stream (Part 2) and quality (Part 4).

Postgres runs in Docker (see docker-compose.yml). The settings below match it; any of
them can be changed with an environment variable, for example PGPORT=5433.

Part 3 (src/serve/) uses the same database and the same connect() helper.
"""
import os

DB_SETTINGS = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "cps853"),
    "user": os.getenv("PGUSER", "cps853"),
    "password": os.getenv("PGPASSWORD", "cps853"),
}

# Tables owned by Parts 2 and 4. Part 3's tables (pitches, q1/q2/q3 results, ...) live in src/serve/.
SCHEMA_SQL = """
-- Part 2: the stream's running result, one row per game, updated as pitches arrive.
CREATE TABLE IF NOT EXISTS stream_pitch_counts (
    game_pk      BIGINT PRIMARY KEY,
    game_date    DATE NOT NULL,
    home_team    TEXT,
    away_team    TEXT,
    pitches      INTEGER NOT NULL,
    strikeouts   INTEGER NOT NULL DEFAULT 0,
    max_speed    REAL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Part 2: pitches per game per inning. The data has no pitch time, so the inning is the
-- game's clock (it stands in for "pitches per hour").
CREATE TABLE IF NOT EXISTS stream_inning_counts (
    game_pk   BIGINT NOT NULL,
    inning    INTEGER NOT NULL,
    pitches   INTEGER NOT NULL,
    PRIMARY KEY (game_pk, inning)
);

-- Part 2/4: every pitch the stream has accepted, so a replayed pitch is caught as a
-- duplicate even after the consumer restarts.
CREATE TABLE IF NOT EXISTS stream_seen_pitches (
    game_pk        BIGINT NOT NULL,
    at_bat_number  INTEGER NOT NULL,
    pitch_number   INTEGER NOT NULL,
    PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- Part 4: rows in and rows out at every stage of every run.
CREATE TABLE IF NOT EXISTS stage_counts (
    run_id     TEXT NOT NULL,
    stage      TEXT NOT NULL,
    rows_in    BIGINT NOT NULL,
    rows_out   BIGINT NOT NULL,
    rows_set_aside BIGINT NOT NULL DEFAULT 0,
    logged_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage)
);

-- Part 4: how many rows failed each check, per run and stage.
CREATE TABLE IF NOT EXISTS quality_checks (
    run_id        TEXT NOT NULL,
    stage         TEXT NOT NULL,
    check_name    TEXT NOT NULL,
    rows_checked  BIGINT NOT NULL,
    rows_failed   BIGINT NOT NULL,
    pct_failed    NUMERIC(7, 3) NOT NULL,
    alert         BOOLEAN NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage, check_name)
);

-- Part 4: bad rows are set aside here instead of being dropped.
CREATE TABLE IF NOT EXISTS bad_rows (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT NOT NULL,
    stage          TEXT NOT NULL,
    failed_checks  TEXT NOT NULL,
    game_pk        BIGINT,
    at_bat_number  INTEGER,
    pitch_number   INTEGER,
    row_data       JSONB NOT NULL,
    caught_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def connect():
    """Open a connection to the project database. Raises SystemExit with a hint if it is not up."""
    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("psycopg is not installed. Run: pip install -r requirements.txt") from exc
    try:
        return psycopg.connect(**DB_SETTINGS, connect_timeout=5)
    except psycopg.OperationalError as exc:
        raise SystemExit(
            f"Cannot reach PostgreSQL at {DB_SETTINGS['host']}:{DB_SETTINGS['port']}. "
            f"Is Docker running? Start it with: docker compose up -d\n({exc})"
        ) from exc


def reset_stream_tables(conn):
    """Empty the stream's running result (run_all.py does this so each run starts clean)."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stream_pitch_counts, stream_inning_counts, stream_seen_pitches")
    conn.commit()


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
