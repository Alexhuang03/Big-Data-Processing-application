"""Build the silver layer: typed, de-duplicated and validated IMDb tables.

Each rule applied here matches a check of preparation.quality_checks.
Silver tables keep the same columns as the source plus "quality_flags",
an array listing the issues found on the row (empty when the row is clean),
so that analyses can decide to exclude flagged rows.

Usage:
    python -m preparation.clean
"""
from pyspark.sql import functions as F

from common.paths import silver_path
from common.schemas import TABLES, column_names
from common.spark import get_spark
from common.typed import load_all_typed
from preparation.rules import (ARCHIVE_CATEGORIES, CURRENT_YEAR, FIRST_FILM_YEAR,
                               MAX_LIFESPAN, MAX_RUNTIME_MINUTES,
                               POSTHUMOUS_MARGIN_YEARS, SERIES_TYPES)

FLAGS = "quality_flags"


def with_flags(df, rules):
    """Add the quality_flags column from a list of (condition, flag name)."""
    flags = F.array(*[F.when(cond, F.lit(name)) for cond, name in rules])
    return df.withColumn(FLAGS, F.filter(flags, lambda x: x.isNotNull()))


def null_if(condition, column):
    return F.when(condition, F.lit(None)).otherwise(F.col(column))


def deduplicate(df, table):
    return df.dropDuplicates(TABLES[table]["primary_key"])


def keep_existing(df, key, parent_ids):
    """Drop rows whose foreign key is not in parent_ids (one column 'id')."""
    return df.join(parent_ids.withColumnRenamed("id", key), key, "left_semi")


def remove_unknown_ids(df, key, array_column, valid_ids):
    """Remove from an array column the ids that are not in valid_ids,
    keeping the original order. Works on any table size (no collect)."""
    exploded = df.select(key, F.posexplode(array_column).alias("pos", "id"))
    kept = exploded.join(valid_ids, "id", "left_semi")
    rebuilt = kept.groupBy(key).agg(
        F.sort_array(F.collect_list(F.struct("pos", "id"))).alias("items")
    ).select(key, F.col("items.id").alias(array_column))
    return df.drop(array_column).join(rebuilt, key, "left")


def clean_name_basics(df):
    death_before_birth = F.col("deathYear") < F.col("birthYear")
    rules = [
        (death_before_birth, "death_before_birth"),
        (F.col("birthYear") > CURRENT_YEAR, "future_birth_year"),
        (F.col("deathYear") > CURRENT_YEAR, "future_death_year"),
        (F.col("deathYear") - F.col("birthYear") > MAX_LIFESPAN, "implausible_lifespan"),
        (F.col("deathYear").isNull()
         & (F.col("birthYear") < CURRENT_YEAR - MAX_LIFESPAN), "missing_death_year"),
    ]
    df = with_flags(deduplicate(df, "name_basics"), rules)
    return (df
            .withColumn("birthYear", null_if(
                death_before_birth | (F.col("birthYear") > CURRENT_YEAR), "birthYear"))
            .withColumn("deathYear", null_if(
                death_before_birth | (F.col("deathYear") > CURRENT_YEAR), "deathYear")))


def clean_title_basics(df):
    end_before_start = F.col("endYear") < F.col("startYear")
    too_early = F.col("startYear") < FIRST_FILM_YEAR
    bad_runtime = F.col("runtimeMinutes") <= 0
    bad_adult = F.col("isAdult").isNotNull() & ~F.col("isAdult").isin(0, 1)
    rules = [
        (end_before_start, "end_before_start"),
        (too_early, "start_year_too_early"),
        (bad_runtime, "runtime_not_positive"),
        (F.col("runtimeMinutes") > MAX_RUNTIME_MINUTES, "runtime_outlier"),
        (bad_adult, "is_adult_invalid"),
    ]
    df = with_flags(deduplicate(df, "title_basics"), rules)
    return (df
            .withColumn("endYear", null_if(end_before_start, "endYear"))
            .withColumn("startYear", null_if(too_early, "startYear"))
            .withColumn("runtimeMinutes", null_if(bad_runtime, "runtimeMinutes"))
            .withColumn("isAdult", null_if(bad_adult, "isAdult")))


def clean_title_ratings(df, title_ids):
    valid = F.col("averageRating").between(1, 10) & (F.col("numVotes") > 0)
    df = deduplicate(df, "title_ratings").filter(valid)
    return with_flags(keep_existing(df, "tconst", title_ids), [])


def clean_title_akas(df, title_ids):
    df = deduplicate(df, "title_akas")
    return with_flags(keep_existing(df, "titleId", title_ids), [])


def clean_title_crew(df, title_ids, name_ids):
    df = keep_existing(deduplicate(df, "title_crew"), "tconst", title_ids)
    df = remove_unknown_ids(df, "tconst", "directors", name_ids)
    df = remove_unknown_ids(df, "tconst", "writers", name_ids)
    return with_flags(df, [])


def clean_title_episode(df, titles):
    df = deduplicate(df, "title_episode").filter("tconst <> parentTconst")
    ids = titles.select(F.col("tconst").alias("id"))
    df = keep_existing(keep_existing(df, "tconst", ids), "parentTconst", ids)

    parents = titles.select(
        F.col("tconst").alias("parentTconst"),
        F.col("titleType").alias("_parentType"),
        F.col("startYear").alias("_parentStart"),
    )
    episodes = titles.select("tconst", F.col("startYear").alias("_start"))
    df = df.join(parents, "parentTconst").join(episodes, "tconst", "left")

    bad_season = F.col("seasonNumber") <= 0
    bad_episode = F.col("episodeNumber") <= 0
    rules = [
        (bad_season, "season_not_positive"),
        (bad_episode, "episode_not_positive"),
        (~F.col("_parentType").isin(SERIES_TYPES), "parent_not_series"),
        (F.col("_start") < F.col("_parentStart"), "episode_before_series_start"),
    ]
    df = with_flags(df, rules)
    return (df
            .withColumn("seasonNumber", null_if(bad_season, "seasonNumber"))
            .withColumn("episodeNumber", null_if(bad_episode, "episodeNumber")))


def clean_title_principals(df, titles, names):
    df = deduplicate(df, "title_principals")
    # Inner joins drop principals whose title or person does not exist.
    df = (df
          .join(titles.select("tconst", F.col("startYear").alias("_start")), "tconst")
          .join(names.select("nconst", F.col("birthYear").alias("_birth"),
                             F.col("deathYear").alias("_death")), "nconst"))
    rules = [
        (F.col("_start") < F.col("_birth"), "born_after_release"),
        ((F.col("_start") > F.col("_death") + POSTHUMOUS_MARGIN_YEARS)
         & ~F.col("category").isin(ARCHIVE_CATEGORIES), "long_after_death"),
    ]
    return with_flags(df, rules)


def write(df, table, spark):
    df.select(*column_names(table), FLAGS).write.mode("overwrite").parquet(silver_path(table))
    stored = spark.read.parquet(silver_path(table))
    flagged = stored.filter(F.size(FLAGS) > 0).count()
    print(f"  {table}: {stored.count():,} rows ({flagged:,} flagged)")
    return stored


def main():
    spark = get_spark("imdb-clean")
    typed = load_all_typed(spark, drop_malformed=True)

    # Parent tables first: the other tables are validated against them.
    titles = write(clean_title_basics(typed["title_basics"]), "title_basics", spark)
    title_ids = titles.select(F.col("tconst").alias("id"))
    names = clean_name_basics(typed["name_basics"])
    names = remove_unknown_ids(names, "nconst", "knownForTitles", title_ids)
    names = write(names, "name_basics", spark)
    name_ids = names.select(F.col("nconst").alias("id"))

    write(clean_title_ratings(typed["title_ratings"], title_ids), "title_ratings", spark)
    write(clean_title_akas(typed["title_akas"], title_ids), "title_akas", spark)
    write(clean_title_crew(typed["title_crew"], title_ids, name_ids), "title_crew", spark)
    write(clean_title_episode(typed["title_episode"], titles), "title_episode", spark)
    write(clean_title_principals(typed["title_principals"], titles, names),
          "title_principals", spark)
    spark.stop()


if __name__ == "__main__":
    main()
