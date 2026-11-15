# ECE Big Data Processing Project: IMDb

## Contributors

| Name | Email | Main parts |
| --- | --- | --- |
| Alex Huang | <alex.huang@edu.ece.fr> | Environment, data preparation, exploration Q1 to Q3, data enrichment, streaming |
| Bryan Tachi | <jessy.tachi@edu.ece.fr> | Exploration Q4 to Q6, open question |
| Hicham Rkaibi | <hicham.rkaibi@edu.ece.fr> | Exploration Q1, Q2, Q3, and Q7 to Q8, open question |

## Requirements

- Docker with Docker Compose v2, with at least 8 GB of memory given to Docker
- About 15 GB of free disk space (raw files, bronze and silver Parquet layers)
- A Unix-like shell (Linux, macOS, or Git Bash / WSL on Windows)
- An internet connection (IMDb dataset download)

## Project layout

```text
compose.yaml          Kafka + Spark (Jupyter PySpark image)
Dockerfile            Spark image + Python libraries from requirements.txt
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

2. Build the Spark image and start the containers.

   ```bash
   docker compose up -d --build
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

Enriches the 10 most voted feature films per genre and year (1980 to 2025)
with a Wikipedia plot synopsis, and the first 3 principals of each film with a
Wikipedia biography and Wikidata facts (birth date, birth place, death date).
Films and people are matched to their exact Wikipedia article through their
IMDb id on Wikidata (property P345). Requires the silver layer (step 1) and
an internet connection. The full run sends about 1,000 HTTP requests, one per
second (about 20 to 30 minutes).

```bash
bash scripts/run_enrichment.sh                      # full run
bash scripts/run_enrichment.sh --limit-films 30     # quick test
```

The pipeline has three stages that can be run alone with `--stage`:

```bash
docker exec imdb_spark python -m enrichment.pipeline --stage select   # Spark: films and principals
docker exec imdb_spark python -m enrichment.pipeline --stage gather   # download, resumable
docker exec imdb_spark python -m enrichment.pipeline --stage build    # Spark: clean, join, write
```

If the download stops (network error, rate limit), run the command again:
records already downloaded are kept and not requested again. To start from
scratch, delete the folder: `docker exec imdb_spark rm -rf /home/jovyan/data/enrichment`.

Every scope limit is a parameter (`--top-n`, `--principals`, `--first-year`,
`--last-year`), so the same code runs on the whole dataset.

Outputs:

- `output/enrichment/`: coverage tables and samples (CSV and Markdown)
- Docker volume, `enrichment/raw/*.jsonl`: raw downloaded records
- Docker volume, `enrichment/enriched_films` and `enrichment/enriched_principals`:
  final Parquet tables. Every row has a `lineage` column (exact Wikipedia
  revision URL, or Wikidata item URI) and an `enrichment_note` column when
  information is missing.

### 4. Streaming

A producer reads English Wikipedia article edits from Wikimedia EventStreams
and publishes them to the Kafka topic `wikipedia-edits`. A Spark Structured
Streaming job matches each edited article with IMDb films (at least 1,000
votes) and the people credited on them. Requires the silver layer (step 1).
The first run downloads the Spark Kafka connector (internet needed).

```bash
bash scripts/run_streaming.sh           # 20-minute test window (default)
bash scripts/run_streaming.sh 1800      # 30-minute test window
```

Or manually, in two terminals:

```bash
docker exec imdb_spark python -m streaming.producer --duration 1320
docker exec imdb_spark python -m streaming.streaming_job --duration 1200
```

Job options: `--window` (default `5 minutes`), `--watermark` (default
`2 minutes`), `--trigger` (default `30 seconds`), `--min-votes` (default 1000).

Outputs:

- `output/streaming/`: streaming metrics, matches by type, top matched films
  and people, window counts, delay summary (CSV and Markdown)
- Docker volume, `streaming/`: Parquet sinks, checkpoints and the raw
  progress reports of each query (`progress_*.json`). Each run starts from
  an empty folder.

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
| Claude Code (Claude Opus 5.5) | Alex Huang | Docker Compose setup, shared code (`src/common`), data preparation code (`src/preparation`), exploration Q1 to Q3 code, enrichment, streaming, initial guide |
| Google DeepMind Antigravity (Gemini) | Bryan Tachi | Exploration Q4 to Q8 code (`src/exploration`), Open Question investigation (`src/open_question`), helper scripts, guide updates |
| ChatGPT (GPT-6) | Hicham RKAIBI |  methodological explanations, critical discussion of findings, and report writing for exploration questions Q1, Q2, Q3, Q7 and Q8 |
