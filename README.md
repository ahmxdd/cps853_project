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
## Requirements

- Python 3.9+
- Java 11 or newer (needed by Spark; check with `java -version`)
- Docker Desktop (needed for Kafka in Part 2)
- About 2 GB of free disk space for the data

From the repository root, create and activate a virtual environment, then install the Python dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run the project commands while this environment is activated.

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

Start the Kafka broker with Docker Desktop running:

```
docker compose up -d kafka
docker compose ps
```

The broker is available to local producer and consumer scripts at `localhost:9092`.
Use `docker compose logs -f kafka` to inspect startup, and `docker compose down` to stop it.

With the broker running, this command reads the prepared 2024 data and publishes
each pitch as a separate JSON message to Kafka's `pitches` topic. The prepared
2024 season contains 727,426 pitches, so the command can take a while; it exits
when the season has been sent. Each message is keyed by `game_pk`, which lets
Kafka keep messages for the same game in order. Kafka stores the messages until
they expire or are deleted; this command does not calculate statistics or
display pitches.

```
python src/stream/producer.py --year 2024
```

Running the producer again sends the pitches again; Kafka does not deduplicate
them. For a small demo, use a separate topic so you don't add test messages to
`pitches`. First run the consumer in one terminal:

```
python src/stream/consumer.py --topic pitches-demo --group-id demo-1 --max-messages 300 --report-every 10
```

Then in another terminal, publish one game (the sample game has 300 pitches):

```
python src/stream/producer.py --year 2024 --game-pk 745444 --topic pitches-demo --delay-seconds 0.1
```

The consumer should show its counts increasing as the messages arrive. The
producer sends saved historical data; the delay only simulates gradual arrival.
The dataset includes a game date, but not the actual time of each pitch, so it
cannot support actual pitches-per-hour counts.

The consumer reads from the beginning only when its group has no saved offset.
Use a new `--group-id` to replay a topic from its beginning. To read all
727,426 2024 messages from `pitches`, use a new group and set
`--max-messages 727426`. Press Ctrl+C to stop a consumer that has no message
limit.

The source has a game date but no actual pitch time, so the consumer reports
daily totals, not hourly totals. Any replay delay simulates arrival timing and
does not represent when the pitches were actually thrown.

Run the producer/consumer integration test against a running Kafka broker and
the prepared data with:

```
pytest tests/test_stream_integration.py
```

The test uses and deletes its own temporary topic; it does not write to `pitches`.


## Running everything

## AI acknowledgement
