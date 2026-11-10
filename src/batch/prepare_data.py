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
import time

import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

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

    # 2. Sort into event-time order.
    table = table.sort_by(SORT_KEYS)

    # 3. Rows per year (check the 1,000,000 minimum!).
    years = pc.value_counts(table["game_year"]).to_pylist()
    for row in sorted(years, key=lambda r: r["values"]):
        print(f"  {row['values']}: {row['counts']:,} rows")
    if table.num_rows < 1_000_000:
        print("WARNING: under 1,000,000 rows. Widen --min-year.")

    # 4. Save as Parquet, partitioned by game_year.
    t0 = time.time()
    pq.write_to_dataset(table, parquet_out, partition_cols=["game_year"])
    print(f"Parquet written in {time.time() - t0:.1f}s")

    # 5. Save the same data as one CSV (the 'before' format for the comparison).
    t0 = time.time()
    pacsv.write_csv(table, csv_out)
    print(f"CSV written in {time.time() - t0:.1f}s")

    # 6. Size comparison for the report and the video.
    print("\n--- File sizes ---")
    print(f"Raw (all years, all columns): {dir_size_mb(args.raw):8.1f} MB")
    print(f"CSV (slice):                  {dir_size_mb(csv_out):8.1f} MB")
    print(f"Parquet (slice, partitioned): {dir_size_mb(parquet_out):8.1f} MB")
    print(f"first row : {table['game_date'][0]} game_pk={table['game_pk'][0]}")
    print(f"last row  : {table['game_date'][-1]} game_pk={table['game_pk'][-1]}")


if __name__ == "__main__":
    main()