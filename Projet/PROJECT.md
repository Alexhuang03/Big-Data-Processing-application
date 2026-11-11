# ECE Big Data Processing Project: IMDb

## Contributors

| Name | Email | Main parts |
| --- | --- | --- |
| Alex Huang | <alexhuang392@gmail.com> | Environment, data preparation, exploration Q1 to Q3 |
| Bryan Tachi | <jessy.tachi@edu.ece.fr> | Exploration Q4 to Q6, open question |
| Bryan Tachi | jessy.tachi@edu.ece.fr | Exploration Q7 & Q8, data enrichment, streaming |

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
src/exploration/      Exploration questions (Q1 to Q8)
src/enrichment/       Data enrichment pipeline
src/streaming/        Kafka producer and Spark Structured Streaming job
src/open_question/    Open research question (creative duo synergy)
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
# Q1: Runtime outliers (z-score and IQR)
docker exec imdb_spark python -m exploration.q1_runtime_outliers
docker exec imdb_spark python -m exploration.q1_runtime_outliers --method iqr

# Q2 & Q3: Genre popularity and comparison
docker exec imdb_spark python -m exploration.q2_q3_genre_popularity

# Q4: One-hit wonder directors
docker exec imdb_spark python -m exploration.q4_one_hit_directors

# Q5: TV series declining in final season
docker exec imdb_spark python -m exploration.q5_declining_tv_series

# Q6: Multilingual successful actors/actresses
docker exec imdb_spark python -m exploration.q6_multilingual_actors

# Q7: Actors whose popularity peaked early in their career
docker exec imdb_spark python -m exploration.q7_career_arc

# Q8: Multi-talented film professionals
docker exec imdb_spark python -m exploration.q8_multi_talented
```

Or run all at once:

```bash
bash scripts/run_exploration.sh
```

### 3. Data enrichment

Enrich the top 10 feature films per genre per year (1980–2025) with Wikipedia synopses and
biographies of at least 3 principals per film:

```bash
bash scripts/run_enrichment.sh
```

Or step by step:

```bash
docker exec imdb_spark python -m enrichment.pipeline
```

Results are written to `output/enrichment/` and enriched Parquet tables to the
`imdb_data` volume under `enrichment/`.

### 4. Streaming

Start the Wikipedia SSE producer and the Spark Structured Streaming job:

```bash
bash scripts/run_streaming.sh           # runs for 30 minutes by default
bash scripts/run_streaming.sh 900       # run for 15 minutes
```

Or manually:

```bash
# Start producer in background
docker exec -d imdb_spark python -m streaming.producer
# Start Spark Structured Streaming job (Ctrl-C to stop)
docker exec imdb_spark python -m streaming.streaming_job
```

Streaming output (CSV micro-batches) is written to `output/streaming/`.
Checkpoints are stored in the `imdb_data` volume under `streaming/checkpoints/`.

### 5. Open question

Requires the silver layer (step 1). Results are written to `output/open_question/`.

```bash
docker exec imdb_spark python -m open_question.director_actor_synergy
```

## Stopping

```bash
docker compose down        # keeps the data volume
docker compose down -v     # also deletes the downloaded and processed data
```

## AI / LLM tools used

| Tool | Used by | Tasks |
| --- | --- | --- |
| Claude Code (Claude Opus 5.5) | Alex Huang | Docker Compose setup, shared code (`src/common`), data preparation code (`src/preparation`), exploration Q1 to Q3 code, initial guide |
| Google DeepMind Antigravity (Gemini / Claude Sonnet 4.6) | Bryan Tachi | Exploration Q4, Q5, Q6, Q7, Q8 code (`src/exploration`), Open Question investigation (`src/open_question`), Data Enrichment pipeline (`src/enrichment`), Streaming producer and job (`src/streaming`), helper scripts, guide updates |
