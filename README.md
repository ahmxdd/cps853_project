# CPS853 Project: MLB (Group 16)


## Dataset

- **Source:** [Jensen-holm/statcast-era-pitches](https://huggingface.co/datasets/Jensen-holm/statcast-era-pitches) on Hugging Face (MIT license)
- **Raw size:** 8,478,976 rows, 826 MB, one Parquet file
- **Event time:** `game_date` (date only), with `game_pk`, `at_bat_number` and `pitch_number` to order pitches within a game

### The slice we use

The full file is much more than we need, so we cut it down:

- Seasons: 2022-2024
- Games: regular season and postseason (`game_type` R, F, D, L, W). Spring training (`S`) is removed.
- Columns: 22 of the original 118
- Result: 2,177,472 rows (720,272 in 2022, 729,774 in 2023, 727,426 in 2024)
- Order: sorted by `game_date, game_pk, at_bat_number, pitch_number` so the rows are in the order the pitches were thrown
<!-- 
## Requirements

- Python 3.9+
- Java 11 or newer (needed by Spark; check with `java -version`)
- Docker Desktop (needed for Kafka in Part 2)
- About 2 GB of free disk space for the data

```
pip install -r requirements.txt
``` -->

## Getting the data

The data is not stored in this repo (it is too big for GitHub, and `data/` is in `.gitignore`). Each person builds it locally with two commands.

**1. Download the raw file (826 MB):**

```
python src/batch/download_data.py
```

This saves the file to `data/raw/data/statcast_era_pitches.parquet` and checks that it has 8,478,976 rows. If the file is already there, it skips the download.

**2. Build the slice (Parquet and CSV):**

```
python src/batch/prepare_data.py
```

This reads only the columns and years we need, sorts the rows, and writes:

| Output | Location | Purpose |
|---|---|---|
| Parquet, partitioned by `game_year` | `data/parquet/` | Main storage format; used by Spark and the stream |
| CSV, one file | `data/csv/pitches.csv` | Comparison format for file size and run time |

It also prints the row count per year and a file size comparison (CSV is about 318 MB, Parquet about 15 MB for the same rows).

To use a different range of years, run `python src/batch/prepare_data.py --min-year 2023 --max-year 2024`.

**3. Check that everything is correct (optional):**

```
python src/batch/download_data.py --check-slice
```

This should report 2,177,472 rows and no spring training games. If `data/parquet/` ever looks wrong, delete it and rerun step 2. Do not rerun step 2 on top of an existing `data/parquet/` folder, because it adds files instead of replacing them.

## Project layout

```
.
├── README.md
├── requirements.txt
├── docker-compose.yml        Kafka for Part 2
├── data/                     local only, never committed
│   ├── raw/                  downloaded file
│   ├── parquet/              slice, partitioned by game_year
│   ├── csv/                  slice, single CSV
│   └── output/               Spark results
└── src/
    ├── batch/                Part 1: download, prepare, Spark jobs
    ├── stream/               Part 2: Kafka producer and consumer
    ├── serve/                Part 3: database (later)
    └── quality/              Part 4: checks (later)
```

## Part 1: Store and batch


## Part 2: Stream


## Running everything

## AI acknowledgement
