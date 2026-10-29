# ECE Big Data Processing Project: IMDb

## Contributors

| Name | Email | Main parts |
| --- | --- | --- |
| Alex Huang | <alexhuang392@gmail.com> | Environment, data preparation, exploration Q1 to Q3 |
| _TODO_ | _TODO_ | Exploration Q4 to Q8, open question |
| _TODO_ | _TODO_ | Data enrichment, streaming |

## Requirements

- Docker with Docker Compose v2, with at least 8 GB of memory given to Docker
- About 15 GB of free disk space (raw files, bronze and silver Parquet layers)
- A Unix-like shell (Linux, macOS, or Git Bash / WSL on Windows)
- An internet connection (IMDb dataset download)

## Project layout

```text
compose.yaml          Kafka + Spark (Jupyter PySpark image)
scripts/              Helper scripts run from the host
src/common/           Shared code: Spark session, schemas, paths, report helpers
src/preparation/      Download, ingestion, null analysis, quality checks, cleaning
src/exploration/      Exploration questions
src/enrichment/       Data enrichment pipeline
src/streaming/        Kafka producer and Spark Structured Streaming job
notebooks/            Notebooks (mounted as ~/work in Jupyter)
output/               Result tables (CSV and Markdown) used in REPORT.md
```

Data is stored in the `imdb_data` Docker volume, mounted at
`/home/jovyan/data` in the Spark container:

- `raw/`: original `.tsv.gz` files from <https://datasets.imdbws.com>
- `bronze/`: the same data as Parquet, every column kept as a string
- `silver/`: typed, cleaned and validated tables used by every analysis

## Setup

1. Clone the repository and go to the project folder.

   ```bash
   git clone <repo-url>
   cd <repo>/Projet
   ```

2. Start the containers.

   ```bash
   docker compose up -d
   ```

   Spark settings can be changed with environment variables, for example
   `SPARK_DRIVER_MEMORY=4g SPARK_MASTER=local[4] docker compose up -d`:

   | Variable | Default | Meaning |
   | --- | --- | --- |
   | `SPARK_DRIVER_MEMORY` | `6g` | Memory of the local Spark driver |
   | `SPARK_MASTER` | `local[8]` | Number of parallel tasks (keep about 1 GB per core) |
   | `SPARK_SHUFFLE_PARTITIONS` | `200` | Partitions used by joins and aggregations |

3. (Optional) Open Jupyter: run `docker logs imdb_spark` and open the
   `http://127.0.0.1:8888/lab?token=...` link.

## Running the project

Every job runs inside the Spark container with `docker exec`.
On Git Bash (Windows), run `export MSYS_NO_PATHCONV=1` first.

### 1. Data preparation

Run the full pipeline (about 30 to 60 minutes the first time):

```bash
bash scripts/run_preparation.sh
```

Or run the steps one by one:

```bash
docker exec imdb_spark python -m preparation.download_imdb            # raw files
docker exec imdb_spark python -m preparation.ingest                   # bronze layer
docker exec imdb_spark python -m preparation.null_report              # null percentages
docker exec imdb_spark python -m preparation.quality_checks --stage typed
docker exec imdb_spark python -m preparation.clean                    # silver layer
docker exec imdb_spark python -m preparation.quality_checks --stage silver
```

Results are written to `output/preparation/`.

### 2. Data exploration

Requires the silver layer (step 1). Results are written to `output/exploration/`.

```bash
docker exec imdb_spark python -m exploration.q1_runtime_outliers           # Q1 (z-score, threshold 2)
docker exec imdb_spark python -m exploration.q1_runtime_outliers --method iqr
docker exec imdb_spark python -m exploration.q2_q3_genre_popularity        # Q2 and Q3
```

_TODO: Q4 to Q8_

### 3. Data enrichment

_TODO_

### 4. Streaming

_TODO_

### 5. Open question

_TODO_

## Stopping

```bash
docker compose down        # keeps the data volume
docker compose down -v     # also deletes the downloaded and processed data
```

## AI / LLM tools used

| Tool | Used by | Tasks |
| --- | --- | --- |
| Claude Code (Claude Opus 5.5) | Alex Huang | Docker Compose setup, shared code (`src/common`), data preparation code (`src/preparation`), exploration Q1 to Q3 code, this guide |
