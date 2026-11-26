<<<<<<< Updated upstream
=======
"""Consume pitch messages from Kafka, check each one, and keep a running result in PostgreSQL.

Running result (Part 2): pitches, strikeouts and top speed per game, in stream_pitch_counts.
Quality (Part 4): every message goes through src/quality/checks.py. Bad ones are set aside in
the bad_rows table (not dropped), each check's failure count goes to quality_checks, and rows
in / rows out go to stage_counts. An ALERT is printed when a check fails on too many rows.

Restarts: offsets are committed to Kafka only after the database has been updated, so a
consumer that is stopped and started again with the same --group-id carries on where it stopped
without losing rows.

If PostgreSQL is not running, the consumer still works and keeps its counts in memory only.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from quality import checks  # noqa: E402

STAGE = "6_stream_consume"
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topic", default="pitches",
                        help="Kafka topic (default: %(default)s)")
    parser.add_argument("--bootstrap-server", default="localhost:9092",
                        help="Kafka address (default: %(default)s)")
    parser.add_argument("--group-id", default="pitch-counter",
                        help="consumer group; use a new name to replay from the beginning")
    parser.add_argument("--max-messages", type=int,
                        help="stop after consuming this many messages")
    parser.add_argument("--idle-timeout", type=float,
                        help="stop after this many seconds without a message")
    parser.add_argument("--report-every", type=int, default=100,
                        help="print progress every N messages (default: %(default)s)")
    parser.add_argument("--flush-every", type=int, default=200,
                        help="write to the database every N messages (default: %(default)s)")
    parser.add_argument("--quiet", action="store_true",
                        help="do not print every pitch (use for long replays)")
    parser.add_argument("--no-db", action="store_true",
                        help="keep counts in memory only, without PostgreSQL")
    args = parser.parse_args()
    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages must be greater than zero")
    if args.idle_timeout is not None and args.idle_timeout <= 0:
        parser.error("--idle-timeout must be greater than zero")
    if args.report_every <= 0:
        parser.error("--report-every must be greater than zero")
    if args.flush_every <= 0:
        parser.error("--flush-every must be greater than zero")
    return args


def decode_pitch(raw_value):
    """Bytes -> dict. Returns (pitch, None) or (None, reason) for a message that is not JSON."""
    try:
        pitch = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        return None, "schema"
    if not isinstance(pitch, dict):
        return None, "schema"
    return pitch, None


def normalise_date(pitch):
    """'2024-03-20T00:00:00' -> '2024-03-20' (only when the date is valid)."""
    gd = pitch.get("game_date")
    if isinstance(gd, str):
        try:
            pitch["game_date"] = date.fromisoformat(gd[:10]).isoformat()
        except ValueError:
            pass


class StreamState:
    """Everything the consumer keeps between messages, plus what is waiting to be written."""

    def __init__(self, run_id):
        self.run_id = run_id
        self.consumed = 0
        self.passed = 0
        self.set_aside = 0
        self.counts_by_game = Counter()
        self.counts_by_date = Counter()
        self.check_failures = Counter({name: 0 for name in checks.CHECKS})
        self.seen_keys = set()
        self.alerted = set()
        # waiting for the next database write
        self.pending_games = {}     # game_pk -> dict of increments
        self.pending_bad = []       # (failed_checks, pitch or raw text)
        self.pending_keys = []      # accepted pitches, for stream_seen_pitches

    def process(self, raw_value):
        """Check one message and update the running result. Returns (pitch, failed checks)."""
        self.consumed += 1
        pitch, reason = decode_pitch(raw_value)
        if pitch is None:
            failed = [reason]
            text = raw_value.decode("utf-8", "replace") if isinstance(raw_value, bytes) else str(raw_value)
            bad_row = {"raw_message": text}
        else:
            normalise_date(pitch)
            failed = checks.check_pitch(pitch, self.seen_keys)
            bad_row = pitch

        if failed:
            self.set_aside += 1
            for name in failed:
                self.check_failures[name] += 1
            self.pending_bad.append((",".join(failed), bad_row))
            return bad_row, failed

        self.passed += 1
        self.pending_keys.append(tuple(pitch[k] for k in checks.KEY))
        game = pitch["game_pk"]
        self.counts_by_game[game] += 1
        self.counts_by_date[pitch["game_date"]] += 1
        g = self.pending_games.setdefault(game, {
            "game_date": pitch["game_date"], "home_team": pitch.get("home_team"),
            "away_team": pitch.get("away_team"), "pitches": 0, "strikeouts": 0, "max_speed": None,
        })
        g["pitches"] += 1
        if pitch.get("events") in STRIKEOUT_EVENTS:
            g["strikeouts"] += 1
        speed = pitch.get("release_speed")
        if speed is not None and (g["max_speed"] is None or speed > g["max_speed"]):
            g["max_speed"] = speed
        return pitch, []

    def new_alerts(self):
        """Checks that have just gone over the alert threshold."""
        out = []
        for name, failed in self.check_failures.items():
            if name not in self.alerted and checks.is_alert(self.consumed, failed,
                                                             checks.ALERT_MIN_ROWS):
                self.alerted.add(name)
                out.append((name, failed))
        return out


def flush_to_db(conn, state):
    """Write pending increments, bad rows and counts, then commit (one transaction)."""
    with conn.cursor() as cur:
        for game_pk, g in state.pending_games.items():
            cur.execute(
                """
                INSERT INTO stream_pitch_counts
                    (game_pk, game_date, home_team, away_team, pitches, strikeouts, max_speed, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (game_pk) DO UPDATE SET
                    pitches    = stream_pitch_counts.pitches + EXCLUDED.pitches,
                    strikeouts = stream_pitch_counts.strikeouts + EXCLUDED.strikeouts,
                    max_speed  = GREATEST(stream_pitch_counts.max_speed, EXCLUDED.max_speed),
                    updated_at = now()
                """,
                (game_pk, g["game_date"], g["home_team"], g["away_team"],
                 g["pitches"], g["strikeouts"], g["max_speed"]),
            )
        cur.executemany(
            """INSERT INTO stream_seen_pitches (game_pk, at_bat_number, pitch_number)
               VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
            state.pending_keys,
        )
        for failed, row in state.pending_bad:
            cur.execute(
                """
                INSERT INTO bad_rows (run_id, stage, failed_checks, game_pk, at_bat_number,
                                      pitch_number, row_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (state.run_id, STAGE, failed, _int_or_none(row.get("game_pk")),
                 _int_or_none(row.get("at_bat_number")), _int_or_none(row.get("pitch_number")),
                 json.dumps(row, default=str)),
            )
        for name, failed in state.check_failures.items():
            pct = 100.0 * failed / state.consumed if state.consumed else 0.0
            cur.execute(
                """
                INSERT INTO quality_checks (run_id, stage, check_name, rows_checked, rows_failed,
                                            pct_failed, alert, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (run_id, stage, check_name) DO UPDATE SET
                    rows_checked = EXCLUDED.rows_checked, rows_failed = EXCLUDED.rows_failed,
                    pct_failed = EXCLUDED.pct_failed, alert = EXCLUDED.alert,
                    checked_at = now()
                """,
                (state.run_id, STAGE, name, state.consumed, failed, round(pct, 3),
                 checks.is_alert(state.consumed, failed, checks.ALERT_MIN_ROWS)),
            )
        cur.execute(
            """
            INSERT INTO stage_counts (run_id, stage, rows_in, rows_out, rows_set_aside, logged_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (run_id, stage) DO UPDATE SET
                rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out,
                rows_set_aside = EXCLUDED.rows_set_aside, logged_at = now()
            """,
            (state.run_id, STAGE, state.consumed, state.passed, state.set_aside),
        )
    conn.commit()
    state.pending_games.clear()
    state.pending_bad.clear()
    state.pending_keys.clear()


def load_seen_keys(conn, state):
    """Pitches accepted by earlier runs, so replaying them counts as duplicates."""
    with conn.cursor() as cur:
        cur.execute("SELECT game_pk, at_bat_number, pitch_number FROM stream_seen_pitches")
        state.seen_keys.update(tuple(r) for r in cur.fetchall())
    if state.seen_keys:
        print(f"{len(state.seen_keys):,} pitches already in the database; repeats will be "
              f"set aside as duplicates.")


def _int_or_none(v):
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def open_db(no_db):
    if no_db:
        return None
    try:
        from common import db
        conn = db.connect()
        db.create_schema(conn)
        print(f"Writing results to PostgreSQL at {db.DB_SETTINGS['host']}:{db.DB_SETTINGS['port']}")
        return conn
    except SystemExit as exc:
        print(f"WARNING: no database, keeping counts in memory only. {str(exc).splitlines()[0]}")
        return None


def main():
    args = parse_args()
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise SystemExit(
            "Kafka client is not installed. Run: python -m pip install -r requirements.txt"
        ) from exc

    conn = open_db(args.no_db)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,          # we commit after the database write
    )
    state = StreamState(checks.run_id_default())
    if conn is not None:
        load_seen_keys(conn, state)
    since_flush = 0
    last_message_at = time.monotonic()

    def flush():
        nonlocal since_flush
        if conn is not None:
            flush_to_db(conn, state)
        consumer.commit()
        since_flush = 0
        for name, failed in state.new_alerts():
            print(f"ALERT: check '{name}' failed on {failed:,} of {state.consumed:,} rows "
                  f"(more than {checks.ALERT_PCT}%)")

    try:
        print(f"Listening to {args.topic} at {args.bootstrap_server}")
        while args.max_messages is None or state.consumed < args.max_messages:
            remaining = (
                args.max_messages - state.consumed if args.max_messages is not None else 100
            )
            records = consumer.poll(timeout_ms=1000, max_records=min(100, remaining))
            received = False
            for messages in records.values():
                for message in messages:
                    row, failed = state.process(message.value)
                    received = True
                    since_flush += 1
                    if failed:
                        print(f"[{state.consumed:,}] SET ASIDE ({', '.join(failed)}): "
                              f"{json.dumps(row, default=str)}")
                    elif not args.quiet:
                        print(f"[{state.consumed:,}] {json.dumps(row)}")   # the row just consumed
                    if state.consumed % args.report_every == 0:
                        print(
                            f"Consumed {state.consumed:,} pitches "
                            f"({state.passed:,} passed, {state.set_aside:,} set aside); "
                            f"games so far: {len(state.counts_by_game):,}"
                        )
                    if since_flush >= args.flush_every:
                        flush()
                    if args.max_messages is not None and state.consumed >= args.max_messages:
                        break
                if args.max_messages is not None and state.consumed >= args.max_messages:
                    break

            if received:
                last_message_at = time.monotonic()
            else:
                if since_flush:
                    flush()            # quiet moment: write what we have
                if (args.idle_timeout is not None
                        and time.monotonic() - last_message_at >= args.idle_timeout):
                    print(f"No messages received for {args.idle_timeout:g} seconds; stopping.")
                    break
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        if since_flush:
            flush()
        consumer.close()
        if conn is not None:
            conn.close()

    print(
        f"Consumed {state.consumed:,} pitches; final counts by game date: "
        f"{dict(sorted(state.counts_by_date.items()))}; "
        f"counts by game: {dict(sorted(state.counts_by_game.items()))}"
    )
    checks.print_check_table(
        [(name, state.consumed, n) for name, n in state.check_failures.items()],
        f"Quality checks on the stream ({state.passed:,} passed, {state.set_aside:,} set aside)",
    )


if __name__ == "__main__":
    main()
>>>>>>> Stashed changes
