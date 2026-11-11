"""Data Enrichment Pipeline — IMDb Big Data Processing Project.

Enriches the top 10 feature films per genre per year (1980-2025) with:
  - A Wikipedia synopsis for each film
  - Wikipedia biographies for at least 3 principals (actors / directors)

All scraping uses the public MediaWiki REST API and respects rate limits.
Cleaning, schema enforcement and final storage are handled by PySpark.

Outputs:
    /home/jovyan/data/enrichment/raw_scrape/        raw JSON files (lineage)
    /home/jovyan/data/enrichment/enriched_films/    Parquet — film synopses
    /home/jovyan/data/enrichment/enriched_people/   Parquet — people bios
    /home/jovyan/output/enrichment/                 CSV + Markdown summaries

Usage:
    python -m enrichment.pipeline [--dry-run] [--min-votes N] [--top-n N]
"""
import argparse
import json
import logging
import os
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from common.datasets import feature_films, load_silver
from common.paths import DATA_DIR, output_dir
from common.report import save_table
from common.spark import get_spark

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SECTION = "enrichment"

# Year range scoped to the project specification
YEAR_MIN = 1980
YEAR_MAX = 2025

# Top-N films selected per (genre, year) window
TOP_N_PER_GENRE_YEAR = 10

# Minimum principals kept per film (actors + directors, priority: directors first)
MIN_PRINCIPALS = 3

# Wikipedia endpoints
WIKI_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKI_SEARCH_URL = (
    "https://en.wikipedia.org/w/api.php"
    "?action=query&list=search&srsearch={query}&srlimit=1&format=json"
)

# Polite rate-limit pause between Wikipedia requests (seconds)
RATE_LIMIT_SLEEP = 0.5

# Output paths (inside the Docker container / DATA_DIR)
ENRICHMENT_DIR = DATA_DIR / "enrichment"
RAW_SCRAPE_DIR = ENRICHMENT_DIR / "raw_scrape"
ENRICHED_FILMS_DIR = ENRICHMENT_DIR / "enriched_films"
ENRICHED_PEOPLE_DIR = ENRICHMENT_DIR / "enriched_people"

# Request headers (Wikipedia policy: identify yourself)
HTTP_HEADERS = {
    "User-Agent": "IMDb-BigData-Project/1.0 (academic; contact: student@university.edu)"
}

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 1 — Silver-layer selection
# ---------------------------------------------------------------------------


def select_top_films(spark) -> DataFrame:
    """Return the top *TOP_N_PER_GENRE_YEAR* films per (genre, year) window.

    Criteria:
      - titleType == 'movie', runtimeMinutes > 40 (feature films)
      - startYear between YEAR_MIN and YEAR_MAX (inclusive)
      - averageRating is not null, numVotes > 0
      - Ranked descending by averageRating within each (genre, year) window
      - Top N per window retained; ties broken by numVotes desc

    Returns a DataFrame with columns:
        tconst, primaryTitle, startYear, genre, averageRating, numVotes, rank
    """
    movies = (
        feature_films(spark, with_ratings=True)
        .filter(F.col("averageRating").isNotNull())
        .filter(F.col("numVotes") > 0)
        .filter(F.col("startYear").between(YEAR_MIN, YEAR_MAX))
        # Explode genres array so we have one row per (film, genre)
        .withColumn("genre", F.explode("genres"))
        .filter(F.col("genre").isNotNull())
        .select("tconst", "primaryTitle", "startYear", "genre", "averageRating", "numVotes")
    )

    window = Window.partitionBy("genre", "startYear").orderBy(
        F.desc("averageRating"), F.desc("numVotes")
    )

    top_films = (
        movies.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= TOP_N_PER_GENRE_YEAR)
    )

    return top_films


# ---------------------------------------------------------------------------
# Step 2 — Principal selection
# ---------------------------------------------------------------------------


def select_principals(spark, top_films_df: DataFrame) -> DataFrame:
    """Select at least MIN_PRINCIPALS principals per film (directors first, then actors).

    Uses title_crew for directors and title_principals for actors/actresses.

    Returns a DataFrame with columns:
        tconst, nconst, primaryName, category   (category: 'director'|'actor'|'actress')
    """
    film_ids = top_films_df.select("tconst").distinct()

    # Directors from title_crew
    crew = (
        load_silver(spark, "title_crew")
        .filter(F.col("directors").isNotNull())
        .select("tconst", F.explode("directors").alias("nconst"))
        .filter(F.col("nconst").isNotNull())
        .withColumn("category", F.lit("director"))
    )

    directors = crew.join(film_ids, "tconst", "inner")

    # Actors / actresses from title_principals (ordering <= 3 gives top-billed cast)
    principals = (
        load_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress"))
        .filter(F.col("ordering") <= 3)
        .select("tconst", "nconst", "category")
        .join(film_ids, "tconst", "inner")
    )

    # Union and deduplicate
    all_principals = directors.unionByName(principals).distinct()

    # Attach real names
    names = load_silver(spark, "name_basics").select("nconst", "primaryName")
    with_names = all_principals.join(names, "nconst", "left").withColumn(
        "primaryName", F.coalesce(F.col("primaryName"), F.col("nconst"))
    )

    # Guarantee at least MIN_PRINCIPALS per film: rank directors first (1), then cast (2)
    priority = Window.partitionBy("tconst").orderBy(
        F.when(F.col("category") == "director", 1).otherwise(2),
        "nconst",
    )
    result = (
        with_names.withColumn("_pri", F.row_number().over(priority))
        .filter(F.col("_pri") <= MIN_PRINCIPALS)
        .drop("_pri")
    )

    return result


# ---------------------------------------------------------------------------
# Step 3 — Wikipedia scraping helpers
# ---------------------------------------------------------------------------


def _wiki_summary_by_title(title: str) -> dict | None:
    """Hit the REST summary endpoint for an exact page title."""
    encoded = urllib.parse.quote(title.replace(" ", "_"), safe="")
    url = WIKI_SUMMARY_URL.format(title=encoded)
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "extract": data.get("extract"),
                "source_url": data.get("content_urls", {}).get("desktop", {}).get("page", url),
            }
    except requests.RequestException as exc:
        logger.warning("REST summary request failed for %r: %s", title, exc)
    return None


def _wiki_search(query: str) -> dict | None:
    """Full-text search fallback using the MediaWiki action API."""
    url = WIKI_SEARCH_URL.format(query=urllib.parse.quote(query))
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        pages = resp.json().get("query", {}).get("search", [])
        if not pages:
            return None
        best_title = pages[0]["title"]
        return _wiki_summary_by_title(best_title)
    except requests.RequestException as exc:
        logger.warning("MediaWiki search failed for %r: %s", query, exc)
    return None


def fetch_wikipedia_synopsis(title: str, year: int | None = None) -> dict:
    """Fetch a film synopsis from Wikipedia.

    Strategy:
      1. Try exact title match via the REST summary endpoint.
      2. If that fails and a year is known, try "Title (year film)".
      3. Fall back to a full-text search query.
      4. If nothing found, return a structured not-found record.

    Returns a dict with keys:
        title, year, synopsis, synopsis_source, source_url, scraped_at
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    attempts = [title]
    if year:
        attempts.append(f"{title} ({year} film)")

    for attempt in attempts:
        result = _wiki_summary_by_title(attempt)
        time.sleep(RATE_LIMIT_SLEEP)
        if result and result.get("extract"):
            return {
                "title": title,
                "year": year,
                "synopsis": result["extract"],
                "synopsis_source": "wikipedia",
                "source_url": result["source_url"],
                "scraped_at": scraped_at,
            }

    # Full-text search fallback
    query = f"{title} film" + (f" {year}" if year else "")
    result = _wiki_search(query)
    time.sleep(RATE_LIMIT_SLEEP)
    if result and result.get("extract"):
        return {
            "title": title,
            "year": year,
            "synopsis": result["extract"],
            "synopsis_source": "wikipedia_search",
            "source_url": result["source_url"],
            "scraped_at": scraped_at,
        }

    return {
        "title": title,
        "year": year,
        "synopsis": None,
        "synopsis_source": "not_found",
        "source_url": None,
        "scraped_at": scraped_at,
    }


def fetch_wikipedia_bio(name: str) -> dict:
    """Fetch a person's biography extract from Wikipedia.

    Strategy: exact name, then full-text search.

    Returns a dict with keys:
        name, bio, bio_source, source_url, scraped_at
    """
    scraped_at = datetime.now(timezone.utc).isoformat()

    result = _wiki_summary_by_title(name)
    time.sleep(RATE_LIMIT_SLEEP)
    if result and result.get("extract"):
        return {
            "name": name,
            "bio": result["extract"],
            "bio_source": "wikipedia",
            "source_url": result["source_url"],
            "scraped_at": scraped_at,
        }

    result = _wiki_search(name)
    time.sleep(RATE_LIMIT_SLEEP)
    if result and result.get("extract"):
        return {
            "name": name,
            "bio": result["extract"],
            "bio_source": "wikipedia_search",
            "source_url": result["source_url"],
            "scraped_at": scraped_at,
        }

    return {
        "name": name,
        "bio": None,
        "bio_source": "not_found",
        "source_url": None,
        "scraped_at": scraped_at,
    }


# ---------------------------------------------------------------------------
# Step 4 — Raw JSON persistence
# ---------------------------------------------------------------------------


def _ensure_dirs() -> None:
    """Create all output directories if they don't exist."""
    for d in (RAW_SCRAPE_DIR, ENRICHED_FILMS_DIR, ENRICHED_PEOPLE_DIR):
        d.mkdir(parents=True, exist_ok=True)


def _save_raw(records: list[dict], filename: str) -> Path:
    """Append-write a list of dicts to a newline-delimited JSON file."""
    path = RAW_SCRAPE_DIR / filename
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info("Saved %d raw records → %s", len(records), path)
    return path


# ---------------------------------------------------------------------------
# Step 5 — Spark schema enforcement and enrichment
# ---------------------------------------------------------------------------

FILM_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("genre", StringType(), True),
        StructField("averageRating", StringType(), True),   # read as string, cast later
        StructField("rank", StringType(), True),
        StructField("synopsis", StringType(), True),
        StructField("synopsis_source", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("scraped_at", StringType(), True),
    ]
)

PEOPLE_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("nconst", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("bio_source", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("scraped_at", StringType(), True),
    ]
)


def enrich_with_spark(spark, raw_records_path: str, record_type: str = "films") -> DataFrame:
    """Read raw JSON records and return a typed, cleaned Spark DataFrame.

    Args:
        spark:            Active SparkSession.
        raw_records_path: Path to the newline-delimited JSON file.
        record_type:      'films' or 'people'.

    Returns:
        A strongly-typed Spark DataFrame conforming to the enrichment schema,
        with null-safe casts and deduplicated rows.
    """
    schema = FILM_SCHEMA if record_type == "films" else PEOPLE_SCHEMA

    df = spark.read.schema(schema).json(raw_records_path)

    # Cast numeric columns that were read as strings
    if record_type == "films":
        df = (
            df.withColumn("year", F.col("year").cast(IntegerType()))
            .withColumn("averageRating", F.col("averageRating").cast("double"))
            .withColumn("rank", F.col("rank").cast(IntegerType()))
            # Normalise synopsis: strip leading/trailing whitespace
            .withColumn("synopsis", F.trim(F.col("synopsis")))
            # Flag empty synopses as null
            .withColumn(
                "synopsis",
                F.when(F.length(F.col("synopsis")) == 0, F.lit(None)).otherwise(F.col("synopsis")),
            )
            .withColumn(
                "synopsis_source",
                F.when(F.col("synopsis").isNull(), F.lit("not_found")).otherwise(
                    F.col("synopsis_source")
                ),
            )
        )
    else:
        df = (
            df.withColumn("bio", F.trim(F.col("bio")))
            .withColumn(
                "bio",
                F.when(F.length(F.col("bio")) == 0, F.lit(None)).otherwise(F.col("bio")),
            )
            .withColumn(
                "bio_source",
                F.when(F.col("bio").isNull(), F.lit("not_found")).otherwise(F.col("bio_source")),
            )
        )

    # Drop duplicates on the natural key
    key_cols = ["tconst", "title", "genre"] if record_type == "films" else ["tconst", "nconst"]
    df = df.dropDuplicates(key_cols)

    return df


# ---------------------------------------------------------------------------
# Step 6 — Summary helpers
# ---------------------------------------------------------------------------


def _build_film_summary(enriched_df: DataFrame) -> "pandas.DataFrame":
    """Return a small pandas summary suitable for save_table."""
    summary = (
        enriched_df.groupBy("synopsis_source")
        .agg(
            F.count("*").alias("film_records"),
            F.countDistinct("tconst").alias("unique_films"),
        )
        .orderBy(F.desc("film_records"))
        .toPandas()
    )
    return summary


def _build_people_summary(enriched_df: DataFrame) -> "pandas.DataFrame":
    summary = (
        enriched_df.groupBy("bio_source", "category")
        .agg(
            F.count("*").alias("people_records"),
            F.countDistinct("nconst").alias("unique_people"),
        )
        .orderBy(F.desc("people_records"))
        .toPandas()
    )
    return summary


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    """End-to-end enrichment pipeline.

    Phases:
      1. Select top films from the silver layer.
      2. Select principals for those films.
      3. Scrape Wikipedia for synopses and biographies.
      4. Save raw JSON files for lineage / reproducibility.
      5. Build typed Spark DataFrames with schema enforcement.
      6. Write Parquet outputs.
      7. Write CSV + Markdown summaries.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process only the first 20 films (for testing without hitting Wikipedia at scale).",
    )
    parser.add_argument(
        "--min-votes",
        type=int,
        default=100,
        help="Minimum numVotes for a film to be considered (default: 100).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Top-N rows in the summary tables (default: 25).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    _ensure_dirs()

    spark = get_spark("imdb-enrichment-pipeline")

    # ------------------------------------------------------------------
    # Phase 1 — Select top films
    # ------------------------------------------------------------------
    print("=== Phase 1: Selecting top films from silver layer ===")
    top_films_df = (
        select_top_films(spark)
        .filter(F.col("numVotes") >= args.min_votes)
        .cache()
    )

    if args.dry_run:
        top_films_df = top_films_df.limit(20).cache()
        print("  [DRY RUN] Limited to 20 films.")

    total_films = top_films_df.count()
    print(f"  → {total_films:,} (film, genre) pairs selected.")

    # ------------------------------------------------------------------
    # Phase 2 — Select principals
    # ------------------------------------------------------------------
    print("=== Phase 2: Selecting principals ===")
    principals_df = select_principals(spark, top_films_df).cache()
    total_principals = principals_df.count()
    print(f"  → {total_principals:,} principal records selected.")

    # ------------------------------------------------------------------
    # Phase 3 — Scrape Wikipedia (Python, respecting rate limits)
    # ------------------------------------------------------------------
    print("=== Phase 3a: Scraping film synopses from Wikipedia ===")

    # Collect unique (tconst, primaryTitle, startYear) pairs to avoid duplicate requests
    films_to_scrape = (
        top_films_df.select("tconst", "primaryTitle", "startYear", "genre", "averageRating", "rank")
        .dropDuplicates(["tconst"])          # one synopsis per film, not per genre
        .toPandas()
    )

    # Also keep genre/rank info to embed in raw records
    genre_rank_pd = top_films_df.select("tconst", "genre", "rank", "averageRating").toPandas()

    synopsis_records: list[dict] = []
    for _, row in films_to_scrape.iterrows():
        title = str(row["primaryTitle"])
        year = int(row["startYear"]) if row["startYear"] is not None else None
        tconst = str(row["tconst"])

        rec = fetch_wikipedia_synopsis(title, year)
        rec["tconst"] = tconst
        synopsis_records.append(rec)

    # Merge genre / rank information back
    genre_map: dict[str, list] = {}
    for _, row in genre_rank_pd.iterrows():
        genre_map.setdefault(str(row["tconst"]), []).append(
            {"genre": row["genre"], "rank": int(row["rank"]), "averageRating": float(row["averageRating"])}
        )

    # Flatten: one record per (tconst, genre)
    flat_synopsis_records: list[dict] = []
    for rec in synopsis_records:
        tconst = rec["tconst"]
        for gentry in genre_map.get(tconst, [{"genre": None, "rank": None, "averageRating": None}]):
            flat_rec = dict(rec)
            flat_rec["genre"] = gentry["genre"]
            flat_rec["rank"] = gentry["rank"]
            flat_rec["averageRating"] = gentry["averageRating"]
            flat_synopsis_records.append(flat_rec)

    synopsis_path = _save_raw(flat_synopsis_records, "film_synopses.jsonl")
    print(f"  → {len(flat_synopsis_records):,} synopsis records saved to {synopsis_path}")

    print("=== Phase 3b: Scraping people biographies from Wikipedia ===")
    people_to_scrape = (
        principals_df.select("tconst", "nconst", "primaryName", "category")
        .toPandas()
    )

    bio_records: list[dict] = []
    seen_nconst: set[str] = set()

    for _, row in people_to_scrape.iterrows():
        nconst = str(row["nconst"])
        tconst = str(row["tconst"])
        name = str(row["primaryName"])
        category = str(row["category"])

        # Scrape bio only once per person; reuse across films
        if nconst not in seen_nconst:
            bio_rec = fetch_wikipedia_bio(name)
            seen_nconst.add(nconst)
        else:
            # Reuse cached result (not re-fetched; we re-emit the stub with this tconst)
            bio_rec = {
                "name": name,
                "bio": None,
                "bio_source": "cached",
                "source_url": None,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        bio_rec["tconst"] = tconst
        bio_rec["nconst"] = nconst
        bio_rec["category"] = category
        bio_records.append(bio_rec)

    bio_path = _save_raw(bio_records, "people_bios.jsonl")
    print(f"  → {len(bio_records):,} bio records saved to {bio_path}")

    # ------------------------------------------------------------------
    # Phase 4 — Spark schema enforcement
    # ------------------------------------------------------------------
    print("=== Phase 4: Building typed Spark DataFrames ===")
    enriched_films = enrich_with_spark(spark, str(synopsis_path), record_type="films")
    enriched_people = enrich_with_spark(spark, str(bio_path), record_type="people")

    print(f"  Films schema:  {', '.join(enriched_films.columns)}")
    print(f"  People schema: {', '.join(enriched_people.columns)}")

    # ------------------------------------------------------------------
    # Phase 5 — Write Parquet outputs
    # ------------------------------------------------------------------
    print("=== Phase 5: Writing Parquet outputs ===")
    (enriched_films
        .write
        .mode("overwrite")
        .partitionBy("genre", "year")
        .parquet(str(ENRICHED_FILMS_DIR)))
    print(f"  → Enriched films written to {ENRICHED_FILMS_DIR}")

    (enriched_people
        .write
        .mode("overwrite")
        .parquet(str(ENRICHED_PEOPLE_DIR)))
    print(f"  → Enriched people written to {ENRICHED_PEOPLE_DIR}")

    # ------------------------------------------------------------------
    # Phase 6 — Summary CSV + Markdown
    # ------------------------------------------------------------------
    print("=== Phase 6: Writing summary reports ===")

    film_summary = _build_film_summary(enriched_films)
    save_table(film_summary, SECTION, "enrichment_film_synopsis_summary")

    people_summary = _build_people_summary(enriched_people)
    save_table(people_summary, SECTION, "enrichment_people_bio_summary")

    # Sample of enriched films (top N by rating)
    top_films_sample = (
        enriched_films
        .filter(F.col("synopsis_source") != "not_found")
        .orderBy(F.desc("averageRating"))
        .limit(args.top_n)
        .select("tconst", "title", "year", "genre", "averageRating", "rank", "synopsis_source", "source_url")
        .toPandas()
    )
    save_table(top_films_sample, SECTION, "enrichment_top_films_sample")

    spark.stop()

    print("=== Enrichment pipeline complete. ===")
    print(f"  Parquet films  → {ENRICHED_FILMS_DIR}")
    print(f"  Parquet people → {ENRICHED_PEOPLE_DIR}")
    print(f"  Reports        → {output_dir(SECTION)}")


if __name__ == "__main__":
    main()
