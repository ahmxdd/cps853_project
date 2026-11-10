"""
Part 1: download the Statcast pitch file from Hugging Face and verify it.

Usage (from the repo root):
    python src/batch/download_data.py                # download + verify raw file
    python src/batch/download_data.py --check-slice  # verify data/parquet after prepare_data.py

The raw file lands at data/raw/data/statcast_era_pitches.parquet (826 MB).
It is skipped if it is already there and has the right number of rows.
"""
import argparse
import os
import sys

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

REPO_ID = "Jensen-holm/statcast-era-pitches"
FILENAME = "data/statcast_era_pitches.parquet"
RAW_DIR = "data/raw"
RAW_PATH = os.path.join(RAW_DIR, FILENAME)
SLICE_DIR = "data/parquet"

EXPECTED_RAW_ROWS = 8_478_976
# 2022-2024, regular season + postseason (game_type R, F, D, L, W), spring training dropped.
EXPECTED_SLICE_ROWS = 2_177_472


def raw_row_count(path):
    """Row count from the Parquet footer (does not load the data)."""
    return pq.ParquetFile(path).metadata.num_rows


def download_and_verify():
    if os.path.exists(RAW_PATH) and raw_row_count(RAW_PATH) == EXPECTED_RAW_ROWS:
        print(f"Already downloaded: {RAW_PATH}")
    else:
        print(f"Downloading {REPO_ID} (about 826 MB)...")
        path = hf_hub_download(
            repo_id=REPO_ID,
            filename=FILENAME,
            repo_type="dataset",
            local_dir=RAW_DIR,
        )
        print(f"Saved to {path}")

    rows = raw_row_count(RAW_PATH)
    print(f"Rows in raw file: {rows:,} (expected {EXPECTED_RAW_ROWS:,})")
    if rows != EXPECTED_RAW_ROWS:
        print("ERROR: row count does not match. The file may be incomplete or the dataset changed.")
        print("Delete data/raw and run this script again.")
        return False
    print("OK: raw file verified.")
    return True


def check_slice():
    """Verify the output of prepare_data.py."""
    if not os.path.isdir(SLICE_DIR):
        print(f"ERROR: {SLICE_DIR} not found. Run prepare_data.py first.")
        return False

    d = ds.dataset(SLICE_DIR, partitioning="hive")
    rows = d.count_rows()
    print(f"Rows in {SLICE_DIR}: {rows:,} (expected {EXPECTED_SLICE_ROWS:,})")

    game_types = pc.value_counts(d.to_table(columns=["game_type"])["game_type"]).to_pylist()
    print("Rows per game_type:")
    for row in sorted(game_types, key=lambda r: r["values"]):
        print(f"  {row['values']}: {row['counts']:,}")

    ok = True
    if rows != EXPECTED_SLICE_ROWS:
        print("ERROR: slice row count is off. Delete data/parquet and rerun prepare_data.py.")
        ok = False
    if any(r["values"] == "S" for r in game_types):
        print("ERROR: spring training rows (S) are still in the slice.")
        ok = False
    if ok:
        print("OK: slice verified.")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check-slice", action="store_true",
                    help="verify data/parquet instead of downloading")
    args = ap.parse_args()

    ok = check_slice() if args.check_slice else download_and_verify()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()