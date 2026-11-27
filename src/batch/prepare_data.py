"""
Part 1 data prep: slice the big Statcast file down to a few seasons,
keep only the columns we need, sort into event-time order, and save as
(a) Parquet partitioned by game_year and (b) one CSV for comparison.

Usage (from the repo root):
    python src/batch/prepare_data.py
    python src/batch/prepare_data.py --raw path/to/file.parquet --min-year 2022 --max-year 2024
"""
import argparse
import os
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from quality import checks  # noqa: E402  (Part 4)

# The ~22 columns the three questions and the stream need.
COLUMNS = [
    "game_pk", "game_date", "game_year", "at_bat_number", "pitch_number",  # ordering
    "pitcher", "player_name", "batter",                                   # who
    "pitch_type", "pitch_name", "release_speed",                          # Q1
    "balls", "strikes", "events", "description", "type",                  # Q2
    "inning", "inning_topbot", "outs_when_up",                            # Q3
    "home_team", "away_team", "game_type",
]

# Event-time order: game_date has no time of day, so break ties with these.
SORT_KEYS = [
    ("game_date", "ascending"),
    ("game_pk", "ascending"),
    ("at_bat_number", "ascending"),
    ("pitch_number", "ascending"),
]


def dir_size_mb(path):
    """Total size of a file or folder in MB."""
    if os.path.isfile(path):
        return os.path.getsize(path) / 1e6
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total / 1e6


def run_quality_checks(table, run_id):
    """Run the Part 4 checks. Returns (good rows, bad rows) and writes the bad rows to
    data/quality/bad_rows_batch.parquet with a failed_checks column saying why."""
    t0 = time.time()
    masks, any_failed = checks.check_table(table)
    results = []
    for name in checks.CHECKS:
        failed = int(masks[name].sum()) if name in masks else 0
        results.append((name, table.num_rows, failed))
        checks.log_check(run_id, "2_quality_batch", name, table.num_rows, failed)
    checks.print_check_table(results, f"Quality checks on the batch ({time.time() - t0:.1f}s)")
    if masks["schema"].any():
        for problem in checks.schema_problems(table):
            print(f"  schema problem: {problem}")
        sys.exit("ERROR: the data does not have the expected schema; stopping.")

    bad_idx = pa.array(any_failed.nonzero()[0])
    good = table.filter(pa.array(~any_failed))
    bad = table.take(bad_idx)
    reasons = [checks.failed_check_names(masks, i) for i in bad_idx.to_pylist()]
    bad = bad.append_column("failed_checks", pa.array(reasons, pa.string()))
    os.makedirs(checks.QUALITY_DIR, exist_ok=True)
    pq.write_table(bad, checks.BAD_ROWS_BATCH)
    checks.log_stage(run_id, "2_quality_batch", table.num_rows, good.num_rows, bad.num_rows)
    print(f"{bad.num_rows:,} bad rows set aside in {checks.BAD_ROWS_BATCH}")
    alerts = [r[0] for r in results if checks.is_alert(r[1], r[2])]
    if alerts:
        print(f"ALERT: more than {checks.ALERT_PCT}% of rows failed: {', '.join(alerts)}")
    return good, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw/data/statcast_era_pitches.parquet")
    ap.add_argument("--out", default="data")
    ap.add_argument("--min-year", type=int, default=2022)
    ap.add_argument("--max-year", type=int, default=2024)
    args = ap.parse_args()

    parquet_out = os.path.join(args.out, "parquet")
    csv_out = os.path.join(args.out, "csv", "pitches.csv")
    os.makedirs(os.path.dirname(csv_out), exist_ok=True)

    print(f"Raw file: {args.raw} ({dir_size_mb(args.raw):.0f} MB)")
    run_id = checks.run_id_default()
    raw_rows = pq.ParquetFile(args.raw).metadata.num_rows

    # 1. Read only the needed columns and years (pyarrow skips the rest).
    t0 = time.time()
    table = pq.read_table(
        args.raw,
        columns=COLUMNS,
        filters=[
    ("game_year", ">=", args.min_year),
    ("game_year", "<=", args.max_year),
    ("game_type", "in", ["R", "F", "D", "L", "W"]),  # regular season + postseason
],
    )
    print(f"Read {table.num_rows:,} rows x {table.num_columns} cols in {time.time() - t0:.1f}s")
    checks.log_stage(run_id, "1_slice_filter", raw_rows, table.num_rows)

    # 2. Sort into event-time order.
    table = table.sort_by(SORT_KEYS)

    # 2b. Part 4: quality checks. Bad rows are set aside, not dropped.
    table, bad = run_quality_checks(table, run_id)

    # 3. Rows per year (check the 1,000,000 minimum!).
    years = pc.value_counts(table["game_year"]).to_pylist()
    for row in sorted(years, key=lambda r: r["values"]):
        print(f"  {row['values']}: {row['counts']:,} rows")
    if table.num_rows < 1_000_000:
        print("WARNING: under 1,000,000 rows. Widen --min-year.")

    # 4. Save as Parquet, partitioned by game_year.
    #    Start from an empty folder: write_to_dataset adds files rather than replacing them.
    if os.path.isdir(parquet_out):
        shutil.rmtree(parquet_out)
    t0 = time.time()
    pq.write_to_dataset(table, parquet_out, partition_cols=["game_year"])
    print(f"Parquet written in {time.time() - t0:.1f}s")

    # 5. Save the same data as one CSV (the 'before' format for the comparison).
    t0 = time.time()
    pacsv.write_csv(table, csv_out)
    print(f"CSV written in {time.time() - t0:.1f}s")
    checks.log_stage(run_id, "3_store_parquet_csv", table.num_rows, table.num_rows)

    # 6. Size comparison for the report and the video.
    print("\n--- File sizes ---")
    print(f"Raw (all years, all columns): {dir_size_mb(args.raw):8.1f} MB")
    print(f"CSV (slice):                  {dir_size_mb(csv_out):8.1f} MB")
    print(f"Parquet (slice, partitioned): {dir_size_mb(parquet_out):8.1f} MB")
    print(f"first row : {table['game_date'][0]} game_pk={table['game_pk'][0]}")
    print(f"last row  : {table['game_date'][-1]} game_pk={table['game_pk'][-1]}")


if __name__ == "__main__":
    main()