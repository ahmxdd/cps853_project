"""Part 4: data quality checks and row counting.

The same checks run in two places:
  * batch  - check_table() on the whole slice in prepare_data.py (vectorised with pyarrow)
  * stream - check_pitch() on each message in src/stream/consumer.py

Bad rows are never dropped silently: they are set aside (data/quality/bad_rows_batch.parquet
for the batch, the bad_rows table for the stream) and every check reports how many rows failed.
If a check fails on more than ALERT_PCT percent of rows, an ALERT is raised.

Batch-side scripts append their counts to CSV files in data/quality/ (so they work without the
database); src/quality/load_quality.py loads them into PostgreSQL.
"""
import csv
import os
from datetime import date, datetime

import numpy as np

QUALITY_DIR = os.path.join("data", "quality")
STAGE_COUNTS_CSV = os.path.join(QUALITY_DIR, "stage_counts.csv")
CHECK_RESULTS_CSV = os.path.join(QUALITY_DIR, "check_results.csv")
BAD_ROWS_BATCH = os.path.join(QUALITY_DIR, "bad_rows_batch.parquet")

# A check that fails on more than this percent of the rows it saw raises an alert.
ALERT_PCT = float(os.getenv("CPS853_ALERT_PCT", "1.0"))
# The stream only alerts once it has seen this many rows, so one early bad row is not "100%".
ALERT_MIN_ROWS = 50

# ---- The rules (shared by batch and stream) ------------------------------------------

# Column -> kind. Used by the schema check.
EXPECTED_SCHEMA = {
    "game_pk": "int", "game_date": "date", "game_year": "int",
    "at_bat_number": "int", "pitch_number": "int", "pitcher": "int",
    "player_name": "str", "batter": "int", "pitch_type": "str", "pitch_name": "str",
    "release_speed": "float", "balls": "int", "strikes": "int", "events": "str",
    "description": "str", "type": "str", "inning": "int", "inning_topbot": "str",
    "outs_when_up": "int", "home_team": "str", "away_team": "str", "game_type": "str",
}
# These must never be empty: without them a pitch cannot be ordered or attributed.
REQUIRED = ["game_pk", "game_date", "at_bat_number", "pitch_number", "pitcher",
            "balls", "strikes", "inning"]
SPEED_MIN, SPEED_MAX = 40.0, 110.0      # mph; the fastest pitch ever recorded is ~105.8
MAX_BALLS, MAX_STRIKES, MAX_OUTS = 3, 2, 2   # the count *before* the pitch
MIN_INNING, MAX_INNING = 1, 30
KEY = ("game_pk", "at_bat_number", "pitch_number")   # one pitch

CHECKS = [
    "schema",             # 1. every expected field is present with the right type
    "missing_values",     # 2. no empty values in REQUIRED columns
    "speed_range",        # 3. release_speed between 40 and 110 mph (empty is allowed)
    "count_range",        # 4. balls 0-3, strikes 0-2, outs 0-2
    "inning_range",       # 5. inning between 1 and 30
    "date_matches_year",  # 6. the year of game_date equals game_year
    "duplicate_pitch",    # 7. (game_pk, at_bat_number, pitch_number) seen only once
]


def run_id_default():
    """One id per pipeline run. run_all.py sets CPS853_RUN_ID so every stage shares it."""
    return os.getenv("CPS853_RUN_ID") or datetime.now().strftime("run-%Y%m%d-%H%M%S")


def is_alert(rows_checked, rows_failed, min_rows=0):
    if rows_checked < max(min_rows, 1):
        return False
    return 100.0 * rows_failed / rows_checked > ALERT_PCT


# ---- Stream: one pitch (a dict) at a time --------------------------------------------

def _kind_ok(value, kind):
    if value is None:
        return True                      # emptiness is the missing_values check's job
    if kind == "int":
        # A whole number stored as a decimal (inning 1.0) counts as an int: the Statcast file
        # stores some integer columns, such as inning, as doubles.
        if isinstance(value, float):
            return value.is_integer()
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "str":
        return isinstance(value, str)
    if kind == "date":
        if isinstance(value, (date, datetime)):
            return True
        if isinstance(value, str):
            try:
                date.fromisoformat(value[:10])
                return True
            except ValueError:
                return False
        return False
    return False


def check_pitch(pitch, seen_keys=None):
    """Return the names of the checks this pitch fails (an empty list means it is good).

    seen_keys is a set the caller keeps between calls, for the duplicate check.
    """
    if not isinstance(pitch, dict):
        return ["schema"]
    failed = []

    if any(col not in pitch or not _kind_ok(pitch[col], kind)
           for col, kind in EXPECTED_SCHEMA.items()):
        failed.append("schema")

    if any(pitch.get(col) is None for col in REQUIRED):
        failed.append("missing_values")

    speed = pitch.get("release_speed")
    if _kind_ok(speed, "float") and speed is not None and not SPEED_MIN <= speed <= SPEED_MAX:
        failed.append("speed_range")

    def out_of(col, low, high):
        v = pitch.get(col)
        return _kind_ok(v, "int") and v is not None and not low <= v <= high

    if out_of("balls", 0, MAX_BALLS) or out_of("strikes", 0, MAX_STRIKES) \
            or out_of("outs_when_up", 0, MAX_OUTS):
        failed.append("count_range")

    if out_of("inning", MIN_INNING, MAX_INNING):
        failed.append("inning_range")

    gd, gy = pitch.get("game_date"), pitch.get("game_year")
    if gd is not None and gy is not None and _kind_ok(gd, "date") and _kind_ok(gy, "int"):
        year = gd.year if isinstance(gd, (date, datetime)) else int(gd[:4])
        if year != gy:
            failed.append("date_matches_year")

    if seen_keys is not None and all(pitch.get(k) is not None for k in KEY):
        key = tuple(pitch[k] for k in KEY)
        if key in seen_keys:
            failed.append("duplicate_pitch")
        else:
            seen_keys.add(key)
    return failed


# ---- Batch: a whole pyarrow table at once --------------------------------------------

def _kind_of_arrow(dtype):
    import pyarrow.types as pat
    if pat.is_integer(dtype):
        return "int"
    if pat.is_floating(dtype):
        return "float"
    if pat.is_string(dtype) or pat.is_large_string(dtype) or pat.is_dictionary(dtype):
        return "str"
    if pat.is_date(dtype) or pat.is_timestamp(dtype):
        return "date"
    return str(dtype)


def _all_whole(column):
    """True if every non-empty value of a float column is a whole number."""
    import pyarrow.compute as pc
    values = pc.drop_null(column)
    if len(values) == 0:
        return True
    return pc.all(pc.equal(values, pc.floor(values))).as_py()


def schema_problems(table):
    """List of 'column: problem' strings; empty when the schema is as expected."""
    problems = []
    for col, kind in EXPECTED_SCHEMA.items():
        if col not in table.column_names:
            problems.append(f"{col}: missing")
            continue
        dtype = table.schema.field(col).type
        if dtype == "null":
            continue                     # an all-empty column has no type; missing_values decides
        actual = _kind_of_arrow(dtype)
        if kind == "int" and actual == "float" and _all_whole(table[col]):
            continue                     # e.g. inning stored as 1.0, 2.0, ...
        if actual != kind and not (kind == "float" and actual == "int"):
            problems.append(f"{col}: expected {kind}, got {table.schema.field(col).type}")
    return problems


def _np(arr, fill):
    """A pyarrow column as a numpy array with nulls replaced by `fill`."""
    import pyarrow.compute as pc
    return pc.fill_null(arr, fill).to_numpy(zero_copy_only=False)


def check_table(table):
    """Run every check on a table that is already sorted by event time.

    Returns (masks, any_failed): masks maps check name -> numpy bool array (True = row failed).
    The schema check is all-or-nothing for a table: if it fails, every row fails it.
    """
    import pyarrow.compute as pc
    n = table.num_rows
    masks = {}

    masks["schema"] = np.full(n, bool(schema_problems(table)))
    if masks["schema"].any():
        # The other checks assume the expected columns exist, so stop here.
        return masks, masks["schema"].copy()

    missing = np.zeros(n, dtype=bool)
    for col in REQUIRED:
        missing |= _np(pc.is_null(table[col]), False)
    masks["missing_values"] = missing

    speed = _np(table["release_speed"].cast("float64"), np.nan)
    masks["speed_range"] = ~np.isnan(speed) & ((speed < SPEED_MIN) | (speed > SPEED_MAX))

    def out_of(col, low, high):
        v = _np(table[col].cast("int64"), low)        # empty counts as in range here
        return (v < low) | (v > high)

    masks["count_range"] = (out_of("balls", 0, MAX_BALLS) | out_of("strikes", 0, MAX_STRIKES)
                            | out_of("outs_when_up", 0, MAX_OUTS))
    masks["inning_range"] = out_of("inning", MIN_INNING, MAX_INNING)

    gd = table["game_date"]
    if _kind_of_arrow(gd.type) == "str":
        gd = pc.strptime(pc.utf8_slice_codeunits(gd, 0, 10), format="%Y-%m-%d", unit="s",
                         error_is_null=True)
    year = _np(pc.year(gd), -1)
    gy = _np(table["game_year"].cast("int64"), -1)
    masks["date_matches_year"] = (year != -1) & (gy != -1) & (year != gy)

    # Duplicates: the table is sorted, so repeated pitches sit next to each other.
    # The first copy is kept; only the extra copies fail.
    keys = [_np(table[k].cast("int64"), -1) for k in KEY]
    dup = np.zeros(n, dtype=bool)
    if n > 1:
        same = np.ones(n - 1, dtype=bool)
        for k in keys:
            same &= k[1:] == k[:-1]
        same &= np.all([k[1:] != -1 for k in keys], axis=0)
        dup[1:] = same
    masks["duplicate_pitch"] = dup

    any_failed = np.zeros(n, dtype=bool)
    for m in masks.values():
        any_failed |= m
    return masks, any_failed


def failed_check_names(masks, row_index):
    return ",".join(name for name, m in masks.items() if m[row_index])


# ---- Recording counts (CSV files for the batch side) ---------------------------------

def _append_csv(path, header, row):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerow(row)


def log_stage(run_id, stage, rows_in, rows_out, rows_set_aside=0, quiet=False):
    """Record rows in and rows out for one stage (data/quality/stage_counts.csv)."""
    _append_csv(STAGE_COUNTS_CSV,
                ["run_id", "stage", "rows_in", "rows_out", "rows_set_aside", "logged_at"],
                [run_id, stage, rows_in, rows_out, rows_set_aside,
                 datetime.now().isoformat(timespec="seconds")])
    if not quiet:
        print(f"[rows] {stage:<22} in {rows_in:>10,}  out {rows_out:>10,}  "
              f"set aside {rows_set_aside:>6,}")


def log_check(run_id, stage, check_name, rows_checked, rows_failed):
    """Record one check's result (data/quality/check_results.csv). Returns True on alert."""
    pct = 100.0 * rows_failed / rows_checked if rows_checked else 0.0
    alert = is_alert(rows_checked, rows_failed)
    _append_csv(CHECK_RESULTS_CSV,
                ["run_id", "stage", "check_name", "rows_checked", "rows_failed",
                 "pct_failed", "alert", "checked_at"],
                [run_id, stage, check_name, rows_checked, rows_failed, f"{pct:.3f}",
                 alert, datetime.now().isoformat(timespec="seconds")])
    return alert


def print_check_table(results, title):
    """results: list of (check_name, rows_checked, rows_failed)."""
    print(f"\n--- {title} ---")
    print(f"{'check':<20} {'checked':>12} {'failed':>9} {'% failed':>9}")
    for name, checked, failed in results:
        pct = 100.0 * failed / checked if checked else 0.0
        flag = "  ALERT" if is_alert(checked, failed) else ""
        print(f"{name:<20} {checked:>12,} {failed:>9,} {pct:>8.3f}%{flag}")