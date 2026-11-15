"""Consume pitch messages from Kafka and report running counts by game and date."""
import argparse
import json
import time
from collections import Counter
from datetime import date


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
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
    args = parser.parse_args()
    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages must be greater than zero")
    if args.idle_timeout is not None and args.idle_timeout <= 0:
        parser.error("--idle-timeout must be greater than zero")
    if args.report_every <= 0:
        parser.error("--report-every must be greater than zero")
    return args


def decode_pitch(message):
    try:
        pitch = json.loads(message.value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Invalid JSON pitch at {message.topic} partition "
            f"{message.partition} offset {message.offset}"
        ) from exc
    if not isinstance(pitch, dict) or not isinstance(pitch.get("game_pk"), int):
        raise ValueError(
            f"Pitch at {message.topic} partition {message.partition} "
            f"offset {message.offset} must contain an integer game_pk"
        )
    game_date = pitch.get("game_date")
    try:
        pitch["game_date"] = date.fromisoformat(game_date[:10]).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Pitch at {message.topic} partition {message.partition} "
            f"offset {message.offset} must contain a valid ISO game_date"
        ) from exc
    return pitch


def main():
    args = parse_args()
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise SystemExit(
            "Kafka client is not installed. Run: python -m pip install -r requirements.txt"
        ) from exc

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    counts_by_game = Counter()
    counts_by_date = Counter()
    consumed = 0
    last_message_at = time.monotonic()
    try:
        print(f"Listening to {args.topic} at {args.bootstrap_server}")
        while args.max_messages is None or consumed < args.max_messages:
            remaining = (
                args.max_messages - consumed if args.max_messages is not None else 100
            )
            records = consumer.poll(timeout_ms=1000, max_records=min(100, remaining))
            received = False
            for messages in records.values():
                for message in messages:
                    pitch = decode_pitch(message)
                    counts_by_game[pitch["game_pk"]] += 1
                    counts_by_date[pitch["game_date"]] += 1
                    consumed += 1
                    received = True
                    if consumed % args.report_every == 0:
                        print(
                            f"Consumed {consumed:,} pitches; counts by game date: "
                            f"{dict(sorted(counts_by_date.items()))}; "
                            f"counts by game: {dict(sorted(counts_by_game.items()))}"
                        )
                    if args.max_messages is not None and consumed >= args.max_messages:
                        break
                if args.max_messages is not None and consumed >= args.max_messages:
                    break

            if received:
                last_message_at = time.monotonic()
            elif (args.idle_timeout is not None
                  and time.monotonic() - last_message_at >= args.idle_timeout):
                print(f"No messages received for {args.idle_timeout:g} seconds; stopping.")
                break
    finally:
        consumer.close()

    print(
        f"Consumed {consumed:,} pitches; final counts by game date: "
        f"{dict(sorted(counts_by_date.items()))}; "
        f"counts by game: {dict(sorted(counts_by_game.items()))}"
    )


if __name__ == "__main__":
    main()
