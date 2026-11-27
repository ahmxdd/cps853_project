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
## Quick start (from a fresh copy)

With Python, Java and Docker Desktop installed and running (see Setup):

```
pip install -r requirements.txt
python run_all.py
```

`run_all.py` starts Kafka and PostgreSQL in Docker, downloads the data, builds the Parquet and CSV
copies with the quality checks, runs the Spark job, loads the database (Part 3), streams a game
through Kafka into PostgreSQL, and prints the quality report. Steps that are already done (the
download, the slice) are skipped on later runs. `python run_all.py --help` lists the options,
for example `--stream-date 2024-03-28` to stream all of Opening Day 2024.

## Setup (once)

You need:

- **Python 3.10 or newer** (`python --version`)
- **Java 17 or 21**, which Spark runs on (`java -version`). On Windows: `winget install EclipseAdoptium.Temurin.17.JDK`, then open a new terminal. On macOS: `brew install openjdk@17`.
- **Docker Desktop**, running (Kafka and PostgreSQL run in it)
- About 2 GB of free disk space for the data

Then, from the repo root (the virtual environment is optional but recommended):

```
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

`pip install` brings in Spark itself (the `pyspark` package).

**Windows only:** Spark needs two Hadoop helper files to read local folders. Run this once in PowerShell:

```
mkdir C:\hadoop\bin
curl.exe -L -o C:\hadoop\bin\winutils.exe https://github.com/kontext-tech/winutils/raw/master/hadoop-3.4.0-win10-x64/bin/winutils.exe
curl.exe -L -o C:\hadoop\bin\hadoop.dll https://github.com/kontext-tech/winutils/raw/master/hadoop-3.4.0-win10-x64/bin/hadoop.dll
```

`spark_jobs.py` looks in `C:\hadoop\bin` by itself, so no environment variables are needed. If the downloads fail, get the two files by hand from the [kontext-tech/winutils bin folder](https://github.com/kontext-tech/winutils/tree/master/hadoop-3.4.0-win10-x64/bin).

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

This should report 2,177,472 rows (the rows in `data/parquet/` plus the rows the quality checks set aside) and no spring training games. Rerunning step 2 replaces `data/parquet/` with a fresh copy.

## Project layout

```
.
├── README.md
├── requirements.txt
├── run_all.py                runs the whole platform, in order
├── docker-compose.yml        Kafka and PostgreSQL
├── tests/                    quality unit tests; stream integration test (needs Kafka)
├── data/                     local only, never committed
│   ├── raw/                  downloaded file
│   ├── parquet/              slice, partitioned by game_year
│   ├── csv/                  slice, single CSV
│   ├── output/               Spark results
│   └── quality/              Part 4: bad rows set aside, check results, rows per stage
└── src/
    ├── common/db.py          PostgreSQL connection and the stream/quality tables
    ├── batch/                Part 1: download, prepare, Spark jobs
    ├── stream/               Part 2: Kafka producer and consumer
    ├── serve/                Part 3: database loading and the three questions
    └── quality/              Part 4: checks, bad-row demo, quality report
```

## Part 1: Store and batch

One Spark job, `src/batch/spark_jobs.py`, reads the Parquet slice and answers all three questions:

```
python src/batch/spark_jobs.py
```

| Question | How the job answers it | Saved to |
|---|---|---|
| Q1. Who are the top 10 pitchers with the fastest average fastball velocity? | Average `release_speed` of four-seam fastballs and sinkers (`pitch_type` FF, SI) per pitcher, for pitchers with at least 200 of them | `data/output/q1_fastest_fastballs.csv` |
| Q2. Based on the count (ex. 0-2, 3-2), what is the strikeout rate? | For each count, the percent of plate appearances that reached it and ended in a strikeout | `data/output/q2_strikeout_rate_by_count.csv` |
| Q3. How many innings/pitches did a pitcher throw in a season? | Pitches and outs recorded per pitcher per regular season; innings = outs / 3 | `data/output/q3_one_pitcher.csv` (the pitcher asked for) and `q3_pitcher_seasons.csv` (everyone) |

Q3 defaults to Paul Skenes in 2024. To ask about someone else, write the name as "Last, First":

```
python src/batch/spark_jobs.py --pitcher "Gausman, Kevin" --year 2024
```

**Parquet compared with CSV.** The same job runs on the CSV copy, and prints its run times at the end, so the two formats can be compared:

```
python src/batch/spark_jobs.py --format csv
```

File sizes for the two formats are printed by `prepare_data.py`.

**If Spark does not start**

- `JAVA_HOME is not set` or `java is not recognized`: install Java 17 (see Setup) and open a new terminal.
- `UnsatisfiedLinkError: ... NativeIO$Windows.access0`, or `Missing or incomplete C:\hadoop\bin\...`: the Windows-only step in Setup was skipped or a download failed. Check that both files in `C:\hadoop\bin` are larger than a few KB.
- `UnsupportedClassVersionError`: your Java is older than 17.

## Part 2: Stream

Kafka and PostgreSQL run in Docker; the producer and consumer are plain Python.

**1. Start Kafka and PostgreSQL** (Docker Desktop must be running; the first start downloads the images):

```
docker compose up -d
```

**2. Start the consumer** in one terminal. It checks every pitch (Part 4) and keeps a running result per game (pitches, strikeouts, top speed) in the PostgreSQL table `stream_pitch_counts`. It prints each pitch and a progress line every 100 pitches:

```
python src/stream/consumer.py
```

**3. Replay a game** from a second terminal (activate the venv there too). The producer reads `data/parquet`, sorts by `game_date, game_pk, at_bat_number, pitch_number`, and sends one JSON message per pitch to the `pitches` topic:

```
python src/stream/producer.py --game-pk 745444 --delay-seconds 0.05
```

`745444` is the first game of 2024 (300 pitches). `--delay-seconds` slows the replay down so it can be watched; leave it out to send as fast as possible. Stop the consumer with Ctrl+C.

Other options:

| Command | What it does |
|---|---|
| `python src/stream/producer.py --date 2024-03-28` | Replays one whole day (Opening Day 2024) |
| `python src/stream/producer.py --dry-run` | Checks the data and message format without Kafka |
| `python src/stream/producer.py --year 2023` | Replays a whole season (about 730,000 pitches). Run the consumer with `--report-every 50000`, because each report lists every game |
| `python src/stream/consumer.py --group-id demo2` | Reads the topic again from the beginning. A group name that was used before continues where it stopped |
| `python src/stream/consumer.py --idle-timeout 10` | Stops by itself after 10 seconds without messages |
| `python src/stream/consumer.py --quiet` | Does not print every pitch (use for long replays) |
| `python src/stream/consumer.py --no-db` | Keeps counts in memory only, without PostgreSQL |
| `python -m pytest tests -v` | Sends one game through Kafka and checks that all 300 pitches arrive (skipped if Kafka is not running) |

**Restarts.** The consumer commits its Kafka position only after the database has been updated. If it is stopped and started again with the same `--group-id`, it carries on where it stopped without losing rows. Every accepted pitch is recorded in `stream_seen_pitches`, so a pitch that is replayed is set aside as a duplicate instead of being counted twice.

**Notes on the stream.**

- Each message is keyed by `game_pk`, so Kafka keeps the pitches of one game in order.
- Kafka does not deduplicate messages: running the producer again appends another copy to the topic. For demos, use a separate topic, for example `--topic pitches-demo` on both the producer and the consumer.
- The dataset has a game date but no pitch time, so the consumer reports totals by date, not per hour. `--delay-seconds` only simulates arrival pacing.
- `tests/test_stream_integration.py` creates its own temporary topic, streams the 300-pitch sample game through it with `--no-db`, and deletes the topic afterwards. It never writes to `pitches`.

Stop Kafka and PostgreSQL with `docker compose down`. This deletes the Kafka topic; the database is kept. `docker compose down -v` deletes the database too.

## Part 4: Quality and monitoring

The same seven checks (`src/quality/checks.py`) run on the batch in `prepare_data.py` and on every message in the consumer:

| Check | A row fails if |
|---|---|
| `schema` | a field is missing or has the wrong type (for example a speed of `"fast"`), or the message is not JSON |
| `missing_values` | `game_pk`, `game_date`, `at_bat_number`, `pitch_number`, `pitcher`, `balls`, `strikes` or `inning` is empty |
| `speed_range` | `release_speed` is outside 40-110 mph (an empty speed is allowed) |
| `count_range` | `balls` is outside 0-3, `strikes` outside 0-2, or `outs_when_up` outside 0-2 |
| `inning_range` | `inning` is outside 1-30 |
| `date_matches_year` | the year of `game_date` is not `game_year` |
| `duplicate_pitch` | the same `(game_pk, at_bat_number, pitch_number)` was already seen; the first copy is kept |

- **Bad rows are set aside, not dropped**: in `data/quality/bad_rows_batch.parquet` for the batch and in the `bad_rows` table for both, with the names of the checks they failed.
- **Every check reports how many rows failed**, in the `quality_checks` table.
- **Rows in and rows out are counted at every stage** (slice, quality, storage, Spark, producer, consumer), in the `stage_counts` table.
- **Alert**: if a check fails on more than 1% of rows, the pipeline prints `ALERT` and marks it in `quality_checks`. Change the threshold with the environment variable `CPS853_ALERT_PCT`.

```
python src/quality/show_quality.py        # the quality report, from PostgreSQL
python src/quality/load_quality.py        # copy the batch-side counts into PostgreSQL (run_all.py does this)
python -m pytest tests/test_quality.py -v # unit tests for every check, no Docker needed
```

**Demo: a bad row added by hand.** With a consumer running in one terminal (`python src/stream/consumer.py`), send bad rows from another:

```
python src/quality/inject_bad_rows.py --speed 250    # one pitch at 250 mph
python src/quality/inject_bad_rows.py --balls 5      # an impossible count
python src/quality/inject_bad_rows.py                # one of every kind of bad row
python src/quality/show_quality.py                   # the counts and the rows set aside
```

The test rows use `game_pk` 999999 and the player "Test, Pitcher", so they cannot be mistaken for real pitches.

## Database connection

PostgreSQL runs in Docker at `localhost:5432`, database `cps853`, user `cps853`, password `cps853` (local only). `src/common/db.py` has a `connect()` helper; the settings can be changed with `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGPASSWORD`. To look around: `docker exec -it cps853-postgres psql -U cps853`.

## AI acknowledgement

- Claude (Anthropic) wrote `src/batch/spark_jobs.py`, `requirements.txt`, and the Setup and Part 1 sections of this README.
- Claude (Anthropic) wrote the Part 4 quality code (`src/quality/`, `tests/test_quality.py`), the database helper (`src/common/db.py`), the PostgreSQL part of the consumer, `run_all.py`, the PostgreSQL service in `docker-compose.yml`, the quality and row-count additions to `prepare_data.py`, `download_data.py`, `spark_jobs.py` and `producer.py`, and the Quick start, Part 4 and Database connection sections of this README.
