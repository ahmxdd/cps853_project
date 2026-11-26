"""
Part 1: one Spark job that answers our three questions from the stored data.

    Q1. Who are the top 10 pitchers with the fastest average fastball velocity?
    Q2. Based on the count (ex. 0-2, 3-2), what is the strikeout rate percentage?
    Q3. How many innings/pitches did pitcher "XYZ" pitch in the 202X season?

Usage (from the repo root, after download_data.py and prepare_data.py):
    python src/batch/spark_jobs.py
    python src/batch/spark_jobs.py --pitcher "Cole, Gerrit" --year 2023
    python src/batch/spark_jobs.py --format csv      # same job on the CSV copy, to compare run time

Each answer is printed and also saved as a small CSV in data/output/.
"""
import argparse
import csv
import os
import sys
import time

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from quality import checks  # noqa: E402  (Part 4: rows in / rows out)
from pyspark.sql import functions as F

PARQUET_PATH = "data/parquet"
CSV_PATH = "data/csv/pitches.csv"
OUTPUT_DIR = "data/output"

# Q1 settings
FASTBALL_TYPES = ["FF", "SI"]   # four-seam fastball and sinker (two-seam)
MIN_FASTBALLS = 200             # ignore pitchers with only a handful of fastballs

# Q2 settings: values of `events` that mean the plate appearance ended in a strikeout
STRIKEOUT_EVENTS = ["strikeout", "strikeout_double_play"]

# Q3 settings: how many outs each value of `events` puts on the board
ONE_OUT_EVENTS = [
    "strikeout", "field_out", "force_out", "fielders_choice_out", "sac_fly", "sac_bunt",
    "other_out", "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home",
    "pickoff_1b", "pickoff_2b", "pickoff_3b",
    "pickoff_caught_stealing_2b", "pickoff_caught_stealing_3b", "pickoff_caught_stealing_home",
]
TWO_OUT_EVENTS = [
    "double_play", "grounded_into_double_play", "strikeout_double_play",
    "sac_fly_double_play", "sac_bunt_double_play",
]
THREE_OUT_EVENTS = ["triple_play"]


def use_hadoop_helpers_on_windows():
    """On Windows, Spark needs winutils.exe and hadoop.dll to read local folders.
    Point Spark at them here so it works in any terminal (see the README for the download)."""
    if os.name != "nt":
        return
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    for name in ["winutils.exe", "hadoop.dll"]:
        path = os.path.join(hadoop_bin, name)
        if not os.path.isfile(path) or os.path.getsize(path) < 10_000:
            sys.exit(f"Missing or incomplete {path}. Download it as described in the README "
                     f'("If Spark does not start").')
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]


def load_pitches(spark, fmt):
    """Read the pitch data that prepare_data.py wrote, as Parquet or as CSV."""
    if fmt == "parquet":
        # game_year comes from the folder names (game_year=2022, ...)
        return spark.read.parquet(PARQUET_PATH)
    return spark.read.csv(CSV_PATH, header=True, inferSchema=True)


def q1_fastest_fastballs(pitches):
    """Top 10 pitchers by average fastball velocity (mph)."""
    return (
        pitches
        .filter(F.col("pitch_type").isin(FASTBALL_TYPES))
        .filter(F.col("release_speed").isNotNull())
        .groupBy("pitcher", "player_name")
        .agg(
            F.round(F.avg("release_speed"), 2).alias("avg_fastball_mph"),
            F.count("*").alias("fastballs_thrown"),
        )
        .filter(F.col("fastballs_thrown") >= MIN_FASTBALLS)
        .orderBy(F.desc("avg_fastball_mph"))
        .limit(10)
    )


def q2_strikeout_rate_by_count(pitches):
    """For each count, the percent of plate appearances reaching it that ended in a strikeout."""
    plate_appearance = ["game_pk", "at_bat_number"]

    # 1 if the plate appearance ended in a strikeout, else 0
    ended_in_strikeout = (
        pitches
        .groupBy(plate_appearance)
        .agg(F.max(F.when(F.col("events").isin(STRIKEOUT_EVENTS), 1).otherwise(0)).alias("strikeout"))
    )

    # One row per (plate appearance, count). distinct() matters because a foul ball
    # with two strikes leaves the count unchanged, so the same count appears twice.
    counts_reached = (
        pitches
        .filter((F.col("balls") <= 3) & (F.col("strikes") <= 2))   # drop the odd bad row
        .select(*plate_appearance, "balls", "strikes")
        .distinct()
    )

    return (
        counts_reached
        .join(ended_in_strikeout, plate_appearance)
        .groupBy("balls", "strikes")
        .agg(
            F.count("*").alias("plate_appearances"),
            F.sum("strikeout").alias("strikeouts"),
        )
        .withColumn("strikeout_pct", F.round(100 * F.col("strikeouts") / F.col("plate_appearances"), 1))
        .withColumn("count", F.concat_ws("-", "balls", "strikes"))
        .orderBy("balls", "strikes")
        .select("count", "plate_appearances", "strikeouts", "strikeout_pct")
    )


def q3_pitcher_seasons(pitches):
    """Pitches thrown and innings pitched for every pitcher in every regular season."""
    outs_on_play = (
        F.when(F.col("events").isin(ONE_OUT_EVENTS), 1)
        .when(F.col("events").isin(TWO_OUT_EVENTS), 2)
        .when(F.col("events").isin(THREE_OUT_EVENTS), 3)
        .otherwise(0)
    )
    return (
        pitches
        .filter(F.col("game_type") == "R")          # regular season only
        .groupBy("game_year", "pitcher", "player_name")
        .agg(
            F.countDistinct("game_pk").alias("games"),
            F.count("*").alias("pitches"),
            F.sum(outs_on_play).alias("outs"),
        )
        # 3 outs = 1 inning, so 20 outs = 6.67 innings (written 6.2 in a box score)
        .withColumn("innings", F.round(F.col("outs") / 3, 2))
        .orderBy("game_year", F.desc("pitches"))
    )


def print_table(columns, rows, limit=20):
    """Print rows as a simple aligned table."""
    table = [list(columns)] + [[str(value) for value in row] for row in rows[:limit]]
    widths = [max(len(line[i]) for line in table) for i in range(len(columns))]
    for line in table:
        print("  ".join(value.ljust(width) for value, width in zip(line, widths)))
    if len(rows) > limit:
        print(f"... {len(rows) - limit:,} more rows in the saved file")


def run(title, df, filename):
    """Run one query, print the result and how long it took, and save it as a CSV."""
    start = time.time()
    rows = df.collect()                      # this is where Spark does the work
    seconds = time.time() - start

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows(rows)

    print(f"\n=== {title} ===")
    print_table(df.columns, rows)
    print(f"{len(rows):,} rows in {seconds:.1f}s, saved to {path}")
    return len(rows), seconds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet",
                    help="which stored copy of the data to read")
    ap.add_argument("--pitcher", default="Skenes, Paul", help='Q3 pitcher, written "Last, First"')
    ap.add_argument("--year", type=int, default=2024, help="Q3 season")
    args = ap.parse_args()

    use_hadoop_helpers_on_windows()      # must run before Spark starts
    spark = (
        SparkSession.builder
        .appName("cps853-part1")
        .master("local[*]")                              # use every core on this laptop
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")     # default of 200 is too many for one laptop
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    pitches = load_pitches(spark, args.format)
    rows_read = pitches.count()
    print(f"Reading {args.format}: {rows_read:,} rows")
    checks.log_stage(checks.run_id_default(), f"4_spark_batch_{args.format}", rows_read, rows_read)

    seasons = q3_pitcher_seasons(pitches)
    one_pitcher = seasons.filter(
        (F.col("player_name") == args.pitcher) & (F.col("game_year") == args.year)
    )

    times = {}
    _, times["Q1"] = run("Q1: top 10 pitchers by average fastball velocity",
                         q1_fastest_fastballs(pitches), "q1_fastest_fastballs.csv")
    _, times["Q2"] = run("Q2: strikeout rate by count",
                         q2_strikeout_rate_by_count(pitches), "q2_strikeout_rate_by_count.csv")
    found, times["Q3"] = run(f"Q3: innings and pitches for {args.pitcher} in {args.year}",
                             one_pitcher, "q3_one_pitcher.csv")
    if found == 0:
        print(f'No rows for "{args.pitcher}" in {args.year}. Names are written "Last, First"; '
              f"look one up in {OUTPUT_DIR}/q3_pitcher_seasons.csv")
    _, times["Q3 (all pitchers)"] = run("Q3 table: every pitcher, every season",
                                        seasons, "q3_pitcher_seasons.csv")

    print(f"\n--- Run times ({args.format}) ---")
    for name, seconds in times.items():
        print(f"{name:<18} {seconds:5.1f}s")
    print(f"{'Total':<18} {sum(times.values()):5.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()
