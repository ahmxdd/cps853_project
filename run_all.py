"""Run the whole platform from a fresh copy, in order:

  0. start Kafka and PostgreSQL in Docker, and wait until both answer
  1. download the raw file (skipped if already there)
  2. build the slice: Parquet + CSV, with the Part 4 quality checks (skipped if already built)
  3. Spark batch job: answers Q1-Q3 from the Parquet copy
  4. Part 3: load batch results into PostgreSQL and time the three questions (src/serve/)
  5. stream: replay pitches through Kafka; the consumer checks each one and updates PostgreSQL
  6. load the batch quality results into PostgreSQL and print the quality report

Usage (from the repo root):
    python run_all.py                              # everything, streaming one game
    python run_all.py --stream-date 2024-03-28     # stream a whole day (Opening Day 2024)
    python run_all.py --rebuild                    # rebuild data/parquet even if it exists
    python run_all.py --compare-csv                # also run the Spark job on the CSV copy
    python run_all.py --skip-docker                # Kafka and PostgreSQL are already running
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable
# Part 3 scripts, run in this order if they exist (src/serve/ is owned by Part 3).
SERVE_STEPS = ["src/serve/load_db.py", "src/serve/queries.py"]


def banner(text):
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}", flush=True)


def run(args, env, check=True):
    print(f"$ {' '.join(str(a) for a in args)}", flush=True)
    result = subprocess.run(args, cwd=ROOT, env=env)
    if check and result.returncode != 0:
        sys.exit(f"Step failed (exit code {result.returncode}): {' '.join(str(a) for a in args)}")
    return result.returncode


def wait_for_postgres(timeout=90):
    sys.path.insert(0, str(ROOT / "src"))
    import psycopg
    from common import db
    deadline = time.time() + timeout
    while True:
        try:
            psycopg.connect(**db.DB_SETTINGS, connect_timeout=3).close()
            print("PostgreSQL is up.")
            return
        except psycopg.OperationalError:
            if time.time() > deadline:
                sys.exit("PostgreSQL did not start. Check: docker compose logs postgres")
            time.sleep(2)


def wait_for_kafka(server, timeout=90):
    from kafka.admin import KafkaAdminClient
    deadline = time.time() + timeout
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=server, request_timeout_ms=3000)
            admin.list_topics()
            admin.close()
            print("Kafka is up.")
            return
        except Exception:      # kafka-python raises several kinds while the broker starts
            if time.time() > deadline:
                sys.exit("Kafka did not start. Check: docker compose logs kafka")
            time.sleep(2)


def reset_stream(server, topic="pitches"):
    """Start each run clean: delete the topic and empty the stream's tables."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from common import db
    admin = KafkaAdminClient(bootstrap_servers=server, request_timeout_ms=10000)
    if topic in admin.list_topics():
        admin.delete_topics([topic])
        for _ in range(30):                 # deletion finishes in the background
            if topic not in admin.list_topics():
                break
            time.sleep(1)
    # Create it now, so the consumer finds it straight away.
    admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    admin.close()
    conn = db.connect()
    db.create_schema(conn)
    db.reset_stream_tables(conn)
    conn.close()
    print(f"Emptied topic '{topic}' and the stream tables.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--skip-docker", action="store_true")
    ap.add_argument("--rebuild", action="store_true", help="rebuild data/parquet and data/csv")
    ap.add_argument("--compare-csv", action="store_true", help="also time Spark on the CSV copy")
    ap.add_argument("--stream-year", type=int, default=2024)
    ap.add_argument("--stream-game", type=int, default=745444,
                    help="game to stream (default: first game of 2024, 300 pitches)")
    ap.add_argument("--stream-date", help="stream a whole day instead of one game, e.g. 2024-03-28")
    ap.add_argument("--delay", type=float, default=0.01,
                    help="seconds between pitches, so the stream can be watched (default 0.01)")
    ap.add_argument("--bootstrap-server", default="localhost:9092")
    args = ap.parse_args()

    run_id = datetime.now().strftime("run-%Y%m%d-%H%M%S")
    env = dict(os.environ, CPS853_RUN_ID=run_id, PYTHONUNBUFFERED="1")
    print(f"Run id: {run_id}")

    banner("0. Start Kafka and PostgreSQL (Docker)")
    if not args.skip_docker:
        run(["docker", "compose", "up", "-d"], env)
    wait_for_postgres()
    wait_for_kafka(args.bootstrap_server)

    banner("1. Download the raw data")
    run([PY, "src/batch/download_data.py"], env)

    banner("2. Build the slice (Parquet + CSV) with quality checks")
    # Rebuild if asked, if there is no slice yet, or if it was built before the quality checks
    # existed (no bad_rows file), so the checks always run on the data we use.
    built = ((ROOT / "data" / "parquet").is_dir()
             and (ROOT / "data" / "quality" / "bad_rows_batch.parquet").exists())
    if args.rebuild or not built:
        run([PY, "src/batch/prepare_data.py"], env)
    else:
        print("data/parquet already built with quality checks; skipping (use --rebuild to redo).")
    run([PY, "src/batch/download_data.py", "--check-slice"], env)

    banner("3. Spark batch job: Q1-Q3 from Parquet")
    run([PY, "src/batch/spark_jobs.py"], env)
    if args.compare_csv:
        run([PY, "src/batch/spark_jobs.py", "--format", "csv"], env)

    banner("4. Serve: load results into PostgreSQL and time the questions (Part 3)")
    found = False
    for step in SERVE_STEPS:
        if (ROOT / step).exists():
            found = True
            run([PY, step], env)
    if not found:
        print("Part 3 scripts not added yet (expected: " + ", ".join(SERVE_STEPS) + ").")

    banner("5. Stream: producer -> Kafka -> consumer -> PostgreSQL")
    reset_stream(args.bootstrap_server)
    consumer = subprocess.Popen(
        [PY, "src/stream/consumer.py", "--group-id", f"run-all-{run_id}", "--idle-timeout", "20",
         "--quiet", "--report-every", "100", "--bootstrap-server", args.bootstrap_server],
        cwd=ROOT, env=env,
    )
    time.sleep(3)          # let the consumer join before the first pitch arrives
    producer = [PY, "src/stream/producer.py", "--year", str(args.stream_year),
                "--delay-seconds", str(args.delay), "--bootstrap-server", args.bootstrap_server]
    if args.stream_date:
        producer += ["--date", args.stream_date]
    else:
        producer += ["--game-pk", str(args.stream_game)]
    code = run(producer, env, check=False)
    consumer.wait()
    if code != 0:
        sys.exit("The producer failed; see the message above.")

    banner("6. Quality report (Part 4)")
    run([PY, "src/quality/load_quality.py"], env)
    run([PY, "src/quality/show_quality.py", "--bad", "5"], env)

    banner(f"Done. Run id {run_id}.")
    print("Try next:  python src/quality/inject_bad_rows.py   (with a consumer running)")


if __name__ == "__main__":
    main()
