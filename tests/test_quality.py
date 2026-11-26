"""Part 4 unit tests: each quality check catches its kind of bad row, in batch and in stream.
No Kafka or database needed:  python -m pytest tests/test_quality.py -v
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream"))

from quality import checks  # noqa: E402
from quality.inject_bad_rows import GOOD_PITCH  # noqa: E402


def pitch(**changes):
    return dict(GOOD_PITCH, **changes)


# ---- stream ---------------------------------------------------------------------------

def test_good_pitch_passes():
    assert checks.check_pitch(pitch(), set()) == []


def test_each_stream_check_catches_its_bad_row():
    cases = {
        "speed_range": pitch(release_speed=250.0),
        "count_range": pitch(balls=4),
        "missing_values": pitch(pitcher=None),
        "inning_range": pitch(inning=0),
        "date_matches_year": pitch(game_date="2019-07-04"),
        "schema": pitch(release_speed="fast"),
    }
    for check, bad in cases.items():
        assert check in checks.check_pitch(bad, set()), check


def test_missing_field_is_a_schema_failure():
    bad = pitch()
    del bad["balls"]
    assert "schema" in checks.check_pitch(bad, set())


def test_duplicate_only_flags_the_second_copy():
    seen = set()
    assert checks.check_pitch(pitch(), seen) == []
    assert checks.check_pitch(pitch(), seen) == ["duplicate_pitch"]


def test_empty_speed_is_allowed():
    assert checks.check_pitch(pitch(release_speed=None), set()) == []


def test_consumer_sets_bad_rows_aside_and_counts_good_ones():
    import consumer
    state = consumer.StreamState("test")
    msgs = [pitch(pitch_number=1), pitch(pitch_number=2, release_speed=250.0),
            pitch(pitch_number=1)]
    for m in msgs:
        state.process(json.dumps(m).encode())
    state.process(b"not json")
    assert (state.consumed, state.passed, state.set_aside) == (4, 1, 3)
    assert state.check_failures["speed_range"] == 1
    assert state.check_failures["duplicate_pitch"] == 1
    assert state.check_failures["schema"] == 1
    assert state.counts_by_game == {GOOD_PITCH["game_pk"]: 1}
    assert len(state.pending_bad) == 3


def test_alert_threshold():
    assert not checks.is_alert(1000, 10)          # exactly 1% is not over 1%
    assert checks.is_alert(1000, 11)
    assert not checks.is_alert(10, 5, min_rows=50)  # too few rows seen yet


# ---- batch ----------------------------------------------------------------------------

def batch_table(rows):
    for r in rows:
        r["game_date"] = datetime.fromisoformat(r["game_date"])
    return pa.Table.from_pylist(rows)


def test_batch_checks_match_stream_checks():
    rows = [
        pitch(pitch_number=1),
        pitch(pitch_number=2, release_speed=250.0),
        pitch(pitch_number=3, balls=4),
        pitch(pitch_number=4, pitcher=None),
        pitch(pitch_number=5, game_date="2019-07-04"),
        pitch(pitch_number=6),
        pitch(pitch_number=6),                    # duplicate of the row above
    ]
    masks, any_failed = checks.check_table(batch_table(rows))
    assert any_failed.tolist() == [False, True, True, True, True, False, True]
    assert masks["speed_range"].tolist()[1]
    assert masks["count_range"].tolist()[2]
    assert masks["missing_values"].tolist()[3]
    assert masks["date_matches_year"].tolist()[4]
    assert masks["duplicate_pitch"].tolist() == [False] * 6 + [True]


def test_batch_schema_check_stops_on_missing_column():
    table = batch_table([pitch()]).drop_columns(["balls"])
    masks, any_failed = checks.check_table(table)
    assert any_failed.all()
    assert any("balls" in p for p in checks.schema_problems(table))
