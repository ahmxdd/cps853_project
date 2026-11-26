"""Part 4 demo: send bad pitches into the Kafka topic by hand, so a quality check catches them.

Start the consumer first (python src/stream/consumer.py), then run one of:

    python src/quality/inject_bad_rows.py                      # one of each kind of bad row
    python src/quality/inject_bad_rows.py --speed 250          # one pitch thrown at 250 mph
    python src/quality/inject_bad_rows.py --balls 5            # an impossible count (5 balls)
    python src/quality/inject_bad_rows.py --kind duplicate     # resend a real pitch twice

The consumer prints "SET ASIDE (...)" for each one, puts it in the bad_rows table, and the
counts appear in quality_checks. Then: python src/quality/show_quality.py
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# A valid pitch with the same fields as the real data, clearly marked as a test row.
GOOD_PITCH = {
    "game_pk": 999999, "game_date": "2024-06-01", "game_year": 2024,
    "at_bat_number": 1, "pitch_number": 1, "pitcher": 999001, "player_name": "Test, Pitcher",
    "batter": 999002, "pitch_type": "FF", "pitch_name": "4-Seam Fastball",
    "release_speed": 95.1, "balls": 0, "strikes": 0, "events": None,
    "description": "called_strike", "type": "S", "inning": 1, "inning_topbot": "Top",
    "outs_when_up": 0, "home_team": "TST", "away_team": "TST", "game_type": "R",
}
# Keys far away from any real pitch, so these test rows never collide with real ones.
TEST_GAME_PK = 999999


def bad_rows(kind, speed, balls):
    base = dict(GOOD_PITCH, game_pk=TEST_GAME_PK, at_bat_number=1)
    rows = {
        "speed": [dict(base, pitch_number=1, release_speed=speed)],
        "count": [dict(base, pitch_number=2, balls=balls)],
        "missing": [dict(base, pitch_number=3, pitcher=None)],
        "date": [dict(base, pitch_number=4, game_date="2019-07-04")],
        "schema": [dict(base, pitch_number=5, release_speed="fast")],
        # the same pitch twice: the first copy passes, the second is caught
        "duplicate": [dict(base, pitch_number=6), dict(base, pitch_number=6)],
    }
    if kind == "all":
        return [r for group in rows.values() for r in group] + ["this is not json"]
    return rows[kind]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kind", default=None,
                    choices=["all", "speed", "count", "missing", "date", "schema", "duplicate"])
    ap.add_argument("--speed", type=float, help="send one pitch at this speed (mph)")
    ap.add_argument("--balls", type=int, help="send one pitch with this many balls")
    ap.add_argument("--topic", default="pitches")
    ap.add_argument("--bootstrap-server", default="localhost:9092")
    args = ap.parse_args()

    kind = args.kind or ("speed" if args.speed is not None else
                         "count" if args.balls is not None else "all")
    rows = bad_rows(kind, args.speed if args.speed is not None else 250.0,
                    args.balls if args.balls is not None else 5)

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_server, acks="all")
    for row in rows:
        value = row if isinstance(row, str) else json.dumps(row)
        producer.send(args.topic, key=str(TEST_GAME_PK).encode(), value=value.encode("utf-8"))
        print(f"sent: {value}")
    producer.flush()
    producer.close()
    print(f"\nSent {len(rows)} hand-made rows to '{args.topic}'. Watch the consumer catch them.")


if __name__ == "__main__":
    main()
