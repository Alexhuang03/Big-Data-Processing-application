"""Reusable views on the silver layer, shared by all analyses."""
from pyspark.sql import functions as F

from common.paths import silver_path

# Academy of Motion Picture Arts and Sciences / BFI definition:
# a feature film runs for more than 40 minutes.
FEATURE_MIN_RUNTIME = 40


def load_silver(spark, table):
    return spark.read.parquet(silver_path(table))


def feature_films(spark, with_ratings=True):
    """Movies (titleType = 'movie') running more than 40 minutes.

    Columns: tconst, primaryTitle, originalTitle, startYear, runtimeMinutes,
    genres, isAdult, quality_flags (+ averageRating, numVotes if with_ratings).
    Rows flagged with a runtime outlier are kept: callers decide.
    """
    movies = (load_silver(spark, "title_basics")
              .filter((F.col("titleType") == "movie")
                      & (F.col("runtimeMinutes") > FEATURE_MIN_RUNTIME))
              .select("tconst", "primaryTitle", "originalTitle", "startYear",
                      "runtimeMinutes", "genres", "isAdult", "quality_flags"))
    if with_ratings:
        ratings = load_silver(spark, "title_ratings").select(
            "tconst", "averageRating", "numVotes")
        movies = movies.join(ratings, "tconst", "left")
    return movies


def explode_genres(df):
    """One row per (title, genre). Titles without a genre are dropped."""
    return df.withColumn("genre", F.explode("genres"))
