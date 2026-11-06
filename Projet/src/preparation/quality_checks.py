"""Data quality checks on the IMDb tables.

Each check returns the rows that break a rule. The same checks are run:
    --stage typed    on the bronze data with the schema applied (before cleaning)
    --stage silver   on the cleaned data (to prove the handling worked)

Outputs (in output/preparation/):
    quality_issues_<stage>.csv / .md        one row per check with counts
    quality_samples_<stage>.md              a few example rows per check

Usage:
    python -m preparation.quality_checks --stage typed
    python -m preparation.quality_checks --stage silver
"""
import argparse
from dataclasses import dataclass
from typing import Callable

import pandas as pd
from pyspark.sql import functions as F

from common.paths import bronze_path, output_dir, silver_path
from common.report import save_table, to_markdown
from common.schemas import TABLES
from common.spark import get_spark
from common.typed import load_all_typed
from preparation.rules import (ARCHIVE_CATEGORIES, CURRENT_YEAR, FIRST_FILM_YEAR,
                               MAX_LIFESPAN, MAX_RUNTIME_MINUTES,
                               POSTHUMOUS_MARGIN_YEARS, SERIES_TYPES)

SECTION = "preparation"
SAMPLE_SIZE = 3


@dataclass
class Check:
    name: str
    table: str
    description: str
    handling: str
    rows: Callable  # (tables dict) -> DataFrame of offending rows
    raw_only: bool = False  # only meaningful before cleaning


def orphans(child, child_key, parent, parent_key):
    def build(t):
        parents = t[parent].select(F.col(parent_key).alias("__key")).distinct()
        return t[child].join(parents, t[child][child_key] == parents["__key"], "left_anti")
    return build


def array_orphans(child, child_key, array_column, parent, parent_key):
    def build(t):
        exploded = t[child].select(
            child_key, F.explode(array_column).alias("__ref")
        )
        parents = t[parent].select(F.col(parent_key).alias("__ref")).distinct()
        return exploded.join(parents, "__ref", "left_anti")
    return build


def duplicates(table):
    keys = TABLES[table]["primary_key"]

    def build(t):
        return (t[table].groupBy(*keys).count().filter("count > 1")
                .withColumnRenamed("count", "occurrences"))
    return build


def malformed(table):
    def build(t):
        df = t[f"bronze:{table}"]
        return df.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record")
    return build


def untrimmed(table, column):
    def build(t):
        df = t[f"bronze:{table}"]
        return df.filter(F.col(column) != F.trim(F.col(column))).select(column)
    return build


def principals_with_dates(t):
    names = t["name_basics"].select("nconst", "primaryName", "birthYear", "deathYear")
    titles = t["title_basics"].select("tconst", "primaryTitle", "startYear")
    return (t["title_principals"].select("tconst", "nconst", "category")
            .join(titles, "tconst").join(names, "nconst"))


def episodes_with_parent(t):
    parents = t["title_basics"].select(
        F.col("tconst").alias("parentTconst"),
        F.col("titleType").alias("parentType"),
        F.col("startYear").alias("parentStartYear"),
    )
    episodes = t["title_basics"].select("tconst", "startYear")
    return (t["title_episode"].join(parents, "parentTconst")
            .join(episodes, "tconst", "left"))


CHECKS = [
    # --- file format -------------------------------------------------------
    *[Check(f"malformed_rows_{tb}", tb,
            "rows without the expected number of tab-separated fields",
            "row dropped", malformed(tb), raw_only=True) for tb in TABLES],
    Check("untrimmed_primary_title", "title_basics",
          "primaryTitle with leading/trailing spaces",
          "value trimmed", untrimmed("title_basics", "primaryTitle"), raw_only=True),
    Check("untrimmed_primary_name", "name_basics",
          "primaryName with leading/trailing spaces",
          "value trimmed", untrimmed("name_basics", "primaryName"), raw_only=True),
    # --- primary keys ------------------------------------------------------
    *[Check(f"duplicate_key_{tb}", tb,
            f"duplicate primary key ({', '.join(TABLES[tb]['primary_key'])})",
            "first row kept", duplicates(tb)) for tb in TABLES],
    # --- people ------------------------------------------------------------
    Check("death_before_birth", "name_basics", "deathYear < birthYear",
          "birthYear and deathYear set to null, row flagged",
          lambda t: t["name_basics"].filter("deathYear < birthYear")),
    Check("implausible_lifespan", "name_basics",
          f"deathYear - birthYear > {MAX_LIFESPAN}",
          "kept, row flagged",
          lambda t: t["name_basics"].filter(f"deathYear - birthYear > {MAX_LIFESPAN}")),
    Check("alive_older_than_max_lifespan", "name_basics",
          f"no deathYear but born before {CURRENT_YEAR - MAX_LIFESPAN}",
          "kept, row flagged (death year probably missing)",
          lambda t: t["name_basics"].filter(
              f"deathYear IS NULL AND birthYear < {CURRENT_YEAR - MAX_LIFESPAN}")),
    Check("future_birth_or_death", "name_basics",
          f"birthYear or deathYear after {CURRENT_YEAR}",
          "value set to null, row flagged",
          lambda t: t["name_basics"].filter(
              f"birthYear > {CURRENT_YEAR} OR deathYear > {CURRENT_YEAR}")),
    # --- titles ------------------------------------------------------------
    Check("end_before_start", "title_basics", "endYear < startYear",
          "endYear set to null, row flagged",
          lambda t: t["title_basics"].filter("endYear < startYear")),
    Check("start_year_too_early", "title_basics",
          f"startYear < {FIRST_FILM_YEAR}",
          "startYear set to null, row flagged",
          lambda t: t["title_basics"].filter(f"startYear < {FIRST_FILM_YEAR}")),
    Check("runtime_not_positive", "title_basics", "runtimeMinutes <= 0",
          "runtimeMinutes set to null, row flagged",
          lambda t: t["title_basics"].filter("runtimeMinutes <= 0")),
    Check("runtime_outlier", "title_basics",
          f"runtimeMinutes > {MAX_RUNTIME_MINUTES}",
          "kept, row flagged",
          lambda t: t["title_basics"].filter(f"runtimeMinutes > {MAX_RUNTIME_MINUTES}")),
    Check("is_adult_invalid", "title_basics", "isAdult not in (0, 1)",
          "isAdult set to null, row flagged",
          lambda t: t["title_basics"].filter(
              "isAdult IS NOT NULL AND isAdult NOT IN (0, 1)")),
    # --- ratings -----------------------------------------------------------
    Check("rating_out_of_range", "title_ratings",
          "averageRating outside [1, 10] or numVotes <= 0", "row dropped",
          lambda t: t["title_ratings"].filter(
              "averageRating < 1 OR averageRating > 10 OR numVotes <= 0")),
    # --- episodes ----------------------------------------------------------
    Check("episode_is_own_parent", "title_episode", "tconst = parentTconst",
          "row dropped",
          lambda t: t["title_episode"].filter("tconst = parentTconst")),
    Check("season_or_episode_not_positive", "title_episode",
          "seasonNumber <= 0 or episodeNumber <= 0",
          "value set to null, row flagged",
          lambda t: t["title_episode"].filter(
              "seasonNumber <= 0 OR episodeNumber <= 0")),
    Check("episode_parent_not_series", "title_episode",
          "parent title is not a tvSeries / tvMiniSeries", "kept, row flagged",
          lambda t: episodes_with_parent(t).filter(
              ~F.col("parentType").isin(SERIES_TYPES))),
    Check("episode_before_series_start", "title_episode",
          "episode startYear < series startYear", "kept, row flagged",
          lambda t: episodes_with_parent(t).filter("startYear < parentStartYear")),
    # --- principals vs. people dates ---------------------------------------
    Check("principal_born_after_release", "title_principals",
          "title startYear < person birthYear", "kept, row flagged",
          lambda t: principals_with_dates(t).filter("startYear < birthYear")),
    Check("principal_long_after_death", "title_principals",
          f"title released > {POSTHUMOUS_MARGIN_YEARS} years after death "
          "(not archive footage/sound)", "kept, row flagged",
          lambda t: principals_with_dates(t).filter(
              (F.col("startYear") > F.col("deathYear") + POSTHUMOUS_MARGIN_YEARS)
              & ~F.col("category").isin(ARCHIVE_CATEGORIES))),
    # --- foreign keys ------------------------------------------------------
    Check("orphan_akas_title", "title_akas", "titleId not in title_basics",
          "row dropped",
          orphans("title_akas", "titleId", "title_basics", "tconst")),
    Check("orphan_crew_title", "title_crew", "tconst not in title_basics",
          "row dropped",
          orphans("title_crew", "tconst", "title_basics", "tconst")),
    Check("orphan_crew_director", "title_crew", "director nconst not in name_basics",
          "id removed from the array",
          array_orphans("title_crew", "tconst", "directors", "name_basics", "nconst")),
    Check("orphan_crew_writer", "title_crew", "writer nconst not in name_basics",
          "id removed from the array",
          array_orphans("title_crew", "tconst", "writers", "name_basics", "nconst")),
    Check("orphan_episode_title", "title_episode", "tconst not in title_basics",
          "row dropped",
          orphans("title_episode", "tconst", "title_basics", "tconst")),
    Check("orphan_episode_parent", "title_episode",
          "parentTconst not in title_basics", "row dropped",
          orphans("title_episode", "parentTconst", "title_basics", "tconst")),
    Check("orphan_principal_title", "title_principals",
          "tconst not in title_basics", "row dropped",
          orphans("title_principals", "tconst", "title_basics", "tconst")),
    Check("orphan_principal_name", "title_principals",
          "nconst not in name_basics", "row dropped",
          orphans("title_principals", "nconst", "name_basics", "nconst")),
    Check("orphan_rating_title", "title_ratings", "tconst not in title_basics",
          "row dropped",
          orphans("title_ratings", "tconst", "title_basics", "tconst")),
    Check("orphan_known_for_title", "name_basics",
          "knownForTitles id not in title_basics", "id removed from the array",
          array_orphans("name_basics", "nconst", "knownForTitles",
                        "title_basics", "tconst")),
]

def load_tables(spark, stage):
    """Tables by name; the typed stage also exposes the raw "bronze:<name>"."""
    if stage == "silver":
        return {table: spark.read.parquet(silver_path(table)) for table in TABLES}
    tables = load_all_typed(spark)
    for table in TABLES:
        tables[f"bronze:{table}"] = spark.read.parquet(bronze_path(table))
    return tables


def run_checks(spark, stage):
    tables = load_tables(spark, stage)
    row_counts = {name: tables[name].count() for name in TABLES}
    results, samples = [], []
    for check in CHECKS:
        if check.raw_only and stage != "typed":
            continue
        print(f"  {check.name}...", flush=True)
        offending = check.rows(tables)
        count = offending.count()
        total = row_counts[check.table]
        results.append({
            "check": check.name,
            "table": check.table,
            "rule": check.description,
            "issue_count": count,
            "table_rows": total,
            "issue_pct": 100.0 * count / total if total else 0.0,
            "handling": check.handling,
        })
        if count:
            samples.append((check.name, offending.limit(SAMPLE_SIZE).toPandas()))
    return pd.DataFrame(results), samples


def save_samples(samples, stage):
    parts = [f"# Data quality samples ({stage})\n"]
    for name, pdf in samples:
        parts.append(f"## {name}\n\n{to_markdown(pdf.astype(str))}")
    path = output_dir(SECTION) / f"quality_samples_{stage}.md"
    path.write_text("\n".join(parts), encoding="utf-8")
    print(f"Saved {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["typed", "silver"], default="typed")
    args = parser.parse_args()

    spark = get_spark(f"imdb-quality-{args.stage}")
    report, samples = run_checks(spark, args.stage)
    print(save_table(report, SECTION, f"quality_issues_{args.stage}", float_digits=4))
    save_samples(samples, args.stage)
    spark.stop()


if __name__ == "__main__":
    main()
