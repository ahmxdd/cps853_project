<<<<<<< Updated upstream
=======
"""Publish prepared Statcast pitches to a Kafka topic in game order."""
import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pyarrow.dataset as ds

PITCH_COLUMNS = [
    "game_pk", "game_date", "game_year", "at_bat_number", "pitch_number",
    "pitcher", "player_name", "batter", "pitch_type", "pitch_name",
    "release_speed", "balls", "strikes", "events", "description", "type",
    "inning", "inning_topbot", "outs_when_up", "home_team", "away_team",
    "game_type",
]
ORDER_COLUMNS = [
    ("game_date", "ascending"),
    ("game_pk", "ascending"),
    ("at_bat_number", "ascending"),
    ("pitch_number", "ascending"),
]
BATCH_SIZE = 1000

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from quality import checks  # noqa: E402  (Part 4: rows in / rows out)


def load_pitches(data_dir, year, game_pk=None, game_date=None):
    """Load and order one season, optionally narrowed to a single game or a single day."""
    dataset = ds.dataset(data_dir, format="parquet", partitioning="hive")
    predicate = ds.field("game_year") == year
    if game_pk is not None:
        predicate = predicate & (ds.field("game_pk") == game_pk)

    pitches = dataset.to_table(columns=PITCH_COLUMNS, filter=predicate)
    if game_date is not None:
        import pyarrow.compute as pc
        day = pc.utf8_slice_codeunits(pc.cast(pitches["game_date"], "string"), 0, 10)
        pitches = pitches.filter(pc.equal(day, game_date))
    if pitches.num_rows == 0:
        selection = f" for game {game_pk}" if game_pk is not None else ""
        selection += f" on {game_date}" if game_date is not None else ""
        raise ValueError(f"No pitches found for {year}{selection} in {data_dir}")
    return pitches.sort_by(ORDER_COLUMNS)


def serialize_message(record):
    return json.dumps(record, default=_json_default, allow_nan=False).encode("utf-8")


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data/parquet",
                        help="prepared Parquet dataset directory (default: %(default)s)")
    parser.add_argument("--year", type=int, default=2024,
                        help="season to stream (default: %(default)s)")
    parser.add_argument("--game-pk", type=int,
                        help="limit the stream to one game")
    parser.add_argument("--date",
                        help="limit the stream to one day, e.g. 2024-03-28 (its year must be --year)")
    parser.add_argument("--topic", default="pitches",
                        help="Kafka topic (default: %(default)s)")
    parser.add_argument("--bootstrap-server", default="localhost:9092",
                        help="Kafka address (default: %(default)s)")
    parser.add_argument("--delay-seconds", type=float, default=0,
                        help="optional delay between pitches to simulate a live feed")
    parser.add_argument("--dry-run", action="store_true",
                        help="check the selected data and message format without Kafka")
    args = parser.parse_args()
    if args.delay_seconds < 0:
        parser.error("--delay-seconds must be non-negative")
    return args


def main():
    args = parse_args()
    if not Path(args.data_dir).is_dir():
        raise FileNotFoundError(f"Parquet dataset directory not found: {args.data_dir}")

    pitches = load_pitches(args.data_dir, args.year, args.game_pk, args.date)
    if args.dry_run:
        first, last = pitches.slice(0, 1).to_pylist()[0], pitches.slice(
            pitches.num_rows - 1, 1
        ).to_pylist()[0]
        serialize_message(first)
        serialize_message(last)
        print(f"Dry run: {pitches.num_rows:,} pitches for {args.year}")
        print(f"First game_pk: {first['game_pk']}; last game_pk: {last['game_pk']}")
        print(f"First pitch: {first['game_date']}, game {first['game_pk']}, "
              f"at-bat {first['at_bat_number']}, pitch {first['pitch_number']}")
        return

    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "Kafka client is not installed. Run: python -m pip install -r requirements.txt"
        ) from exc

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        key_serializer=lambda game_pk: str(game_pk).encode("utf-8"),
        value_serializer=serialize_message,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    pending = []
    sent = 0
    try:
        for batch in pitches.to_batches(max_chunksize=BATCH_SIZE):
            for record in batch.to_pylist():
                pending.append(producer.send(
                    args.topic, key=record["game_pk"], value=record
                ))
                sent += 1
                if args.delay_seconds:
                    time.sleep(args.delay_seconds)

            if len(pending) >= BATCH_SIZE:
                for future in pending:
                    future.get(timeout=30)
                pending.clear()
                print(f"Sent {sent:,}/{pitches.num_rows:,} pitches")

        for future in pending:
            future.get(timeout=30)
    finally:
        producer.close(timeout=30)

    print(f"Sent {sent:,} pitches to {args.topic} at {args.bootstrap_server}")
    checks.log_stage(checks.run_id_default(), "5_stream_produce", pitches.num_rows, sent)


if __name__ == "__main__":
    main()
>>>>>>> Stashed changes
