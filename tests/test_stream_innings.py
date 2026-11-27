"""Part 2 unit test: the consumer's running count of pitches per game per inning.
No Kafka or database needed:  python -m pytest tests/test_stream_innings.py -v
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream"))

from consumer import StreamState  # noqa: E402
from quality.inject_bad_rows import GOOD_PITCH  # noqa: E402


def message(**changes):
    return json.dumps(dict(GOOD_PITCH, **changes)).encode("utf-8")


def test_pitches_are_counted_per_game_per_inning():
    state = StreamState("test")
    pitches = [
        dict(game_pk=1, inning=1, at_bat_number=1, pitch_number=1),
        dict(game_pk=1, inning=1, at_bat_number=1, pitch_number=2),
        dict(game_pk=1, inning=2, at_bat_number=2, pitch_number=1),
        dict(game_pk=2, inning=1, at_bat_number=1, pitch_number=1),
    ]
    for p in pitches:
        state.process(message(**p))

    assert state.innings_for(1) == {1: 2, 2: 1}
    assert state.innings_for(2) == {1: 1}
    assert state.pending_innings == {(1, 1): 2, (1, 2): 1, (2, 1): 1}


def test_inning_stored_as_decimal_counts_as_whole_number():
    state = StreamState("test")
    state.process(message(game_pk=1, inning=3.0))
    assert state.innings_for(1) == {3: 1}


def test_set_aside_pitch_is_not_counted_in_an_inning():
    state = StreamState("test")
    state.process(message(game_pk=1, inning=99))    # fails inning_range
    assert state.set_aside == 1
    assert state.innings_for(1) == {}
