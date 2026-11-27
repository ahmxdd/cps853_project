"""Part 4: print the quality report from PostgreSQL (for the video and the report).

    python src/quality/show_quality.py              # latest run of each stage
    python src/quality/show_quality.py --bad 10     # also show the 10 newest bad rows
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common import db  # noqa: E402


# Columns that are ids, printed without thousands separators.
ID_COLUMNS = {"id", "game_pk", "ab", "pitch"}


def print_rows(title, columns, rows):
    print(f"\n=== {title} ===")
    if not rows:
        print("(none yet)")
        return
    def fmt(col, v):
        if v is None:
            return ""
        if isinstance(v, int) and col not in ID_COLUMNS:
            return f"{v:,}"
        return str(v)
    table = [columns] + [[fmt(c, v) for c, v in zip(columns, row)] for row in rows]
    widths = [max(len(r[i]) for r in table) for i in range(len(columns))]
    for r in table:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bad", type=int, default=10, help="how many bad rows to show")
    args = ap.parse_args()
    conn = db.connect()
    db.create_schema(conn)
    with conn.cursor() as cur:
        # Latest run for each stage, in pipeline order.
        cur.execute("""
            SELECT DISTINCT ON (stage) stage, rows_in, rows_out, rows_set_aside, run_id
            FROM stage_counts ORDER BY stage, logged_at DESC
        """)
        print_rows("Rows in and out at every stage (latest run of each)",
                   ["stage", "rows_in", "rows_out", "set_aside", "run_id"], cur.fetchall())

        cur.execute("""
            SELECT q.stage, q.check_name, q.rows_checked, q.rows_failed, q.pct_failed,
                   CASE WHEN q.alert THEN 'ALERT' ELSE '' END
            FROM quality_checks q
            WHERE q.run_id = (SELECT run_id FROM quality_checks q2
                              WHERE q2.stage = q.stage ORDER BY checked_at DESC LIMIT 1)
            ORDER BY q.stage, q.check_name
        """)
        print_rows("Quality checks (latest run of each stage)",
                   ["stage", "check", "checked", "failed", "% failed", "alert"], cur.fetchall())

        cur.execute("SELECT stage, count(*) FROM bad_rows GROUP BY stage ORDER BY stage")
        print_rows("Rows set aside in bad_rows", ["stage", "rows"], cur.fetchall())

        cur.execute("""
            SELECT id, stage, failed_checks, game_pk, at_bat_number, pitch_number,
                   left(row_data::text, 70), caught_at::timestamp(0)
            FROM bad_rows ORDER BY id DESC LIMIT %s
        """, (args.bad,))
        print_rows(f"Newest {args.bad} bad rows",
                   ["id", "stage", "failed", "game_pk", "ab", "pitch", "row", "caught_at"],
                   cur.fetchall())

        cur.execute("""
            SELECT game_date, game_pk, away_team || ' @ ' || home_team, pitches, strikeouts,
                   max_speed, updated_at::timestamp(0)
            FROM stream_pitch_counts ORDER BY updated_at DESC, game_pk LIMIT 10
        """)
        print_rows("Stream running result: stream_pitch_counts (10 most recently updated games)",
                   ["date", "game_pk", "game", "pitches", "Ks", "max mph", "updated"],
                   cur.fetchall())
    conn.close()


if __name__ == "__main__":
    main()
