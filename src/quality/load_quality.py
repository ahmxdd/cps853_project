"""Part 4: load the batch-side quality results into PostgreSQL.

prepare_data.py, spark_jobs.py and producer.py write their counts to CSV files in data/quality/
(so they run without the database). This script copies them, and the rows set aside by the batch
checks, into the stage_counts, quality_checks and bad_rows tables. It is safe to run again.

    python src/quality/load_quality.py
"""
import csv
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common import db  # noqa: E402
from quality import checks  # noqa: E402

BATCH_STAGE = "2_quality_batch"


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    conn = db.connect()
    db.create_schema(conn)
    stages = read_csv(checks.STAGE_COUNTS_CSV)
    results = read_csv(checks.CHECK_RESULTS_CSV)

    with conn.cursor() as cur:
        for r in stages:
            cur.execute(
                """
                INSERT INTO stage_counts (run_id, stage, rows_in, rows_out, rows_set_aside, logged_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, stage) DO UPDATE SET rows_in = EXCLUDED.rows_in,
                    rows_out = EXCLUDED.rows_out, rows_set_aside = EXCLUDED.rows_set_aside,
                    logged_at = EXCLUDED.logged_at
                """,
                (r["run_id"], r["stage"], int(r["rows_in"]), int(r["rows_out"]),
                 int(r["rows_set_aside"]), r["logged_at"]),
            )
        for r in results:
            cur.execute(
                """
                INSERT INTO quality_checks (run_id, stage, check_name, rows_checked, rows_failed,
                                            pct_failed, alert, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, stage, check_name) DO UPDATE SET
                    rows_checked = EXCLUDED.rows_checked, rows_failed = EXCLUDED.rows_failed,
                    pct_failed = EXCLUDED.pct_failed, alert = EXCLUDED.alert,
                    checked_at = EXCLUDED.checked_at
                """,
                (r["run_id"], r["stage"], r["check_name"], int(r["rows_checked"]),
                 int(r["rows_failed"]), float(r["pct_failed"]), r["alert"] == "True",
                 r["checked_at"]),
            )

        bad_loaded = 0
        batch_runs = [r["run_id"] for r in stages if r["stage"] == BATCH_STAGE]
        if os.path.exists(checks.BAD_ROWS_BATCH) and batch_runs:
            import pyarrow.parquet as pq
            run_id = batch_runs[-1]          # the file holds the latest prepare run's bad rows
            cur.execute("DELETE FROM bad_rows WHERE stage = %s", (BATCH_STAGE,))
            for row in pq.read_table(checks.BAD_ROWS_BATCH).to_pylist():
                failed = row.pop("failed_checks")
                cur.execute(
                    """
                    INSERT INTO bad_rows (run_id, stage, failed_checks, game_pk, at_bat_number,
                                          pitch_number, row_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, BATCH_STAGE, failed, row.get("game_pk"), row.get("at_bat_number"),
                     row.get("pitch_number"), json.dumps(row, default=str)),
                )
                bad_loaded += 1
    conn.commit()
    conn.close()
    print(f"Loaded {len(stages)} stage counts, {len(results)} check results and "
          f"{bad_loaded:,} batch bad rows into PostgreSQL.")


if __name__ == "__main__":
    main()
