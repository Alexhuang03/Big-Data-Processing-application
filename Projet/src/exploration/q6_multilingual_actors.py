"""Q6: actors/actresses who have achieved success across films in multiple distinct
languages (>= 2 languages), and those in >= 3 languages.

Outputs (in output/exploration/):
    q6_multilingual_actors_2plus.csv / .md     actors successful in >= 2 languages
    q6_multilingual_actors_3plus.csv / .md     actors successful in >= 3 languages
    q6_multilingual_summary.csv / .md          summary distribution by language count

Usage:
    python -m exploration.q6_multilingual_actors [--min-rating R] [--min-votes V]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import feature_films, load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"


def get_movie_original_languages(spark, movies_df):
    """Extract one original language per movie from title_akas.

    Matches title_akas entries where aka title matches the originalTitle,
    keeping the entry with the lowest ordering (earliest release / primary entry).
    """
    akas = (
        load_silver(spark, "title_akas")
        .filter(F.col("language").isNotNull())
        .select(
            F.col("titleId").alias("tconst"),
            F.col("ordering"),
            F.col("title").alias("aka_title"),
            F.lower(F.col("language")).alias("language"),
        )
    )

    w = Window.partitionBy("tconst").orderBy("ordering")
    matched = (
        movies_df.select("tconst", "originalTitle")
        .join(akas, "tconst", "inner")
        .filter(F.col("aka_title") == F.col("originalTitle"))
        .withColumn("r", F.row_number().over(w))
        .filter("r = 1")
        .select("tconst", "language")
    )

    return matched


def find_multilingual_actors(spark, min_rating=7.0, min_votes=1000):
    """Find actors/actresses with successful films in multiple languages."""
    # 1. Successful feature films
    successful_movies = (
        feature_films(spark, with_ratings=True)
        .filter(
            (F.col("averageRating") >= min_rating)
            & (F.col("numVotes") >= min_votes)
        )
        .select(
            "tconst",
            "primaryTitle",
            "originalTitle",
            "startYear",
            "averageRating",
            "numVotes",
        )
    ).cache()

    total_successful = successful_movies.count()

    # 2. Movie original languages
    movie_languages = get_movie_original_languages(spark, successful_movies).cache()
    movies_with_lang_count = movie_languages.select("tconst").distinct().count()
    print(
        f"Successful films: {total_successful:,} total, "
        f"{movies_with_lang_count:,} have a known original language "
        f"({total_successful - movies_with_lang_count:,} excluded due to missing language)."
    )

    movies_with_lang = successful_movies.join(movie_languages, "tconst", "inner")

    # 3. Actors / Actresses from title_principals
    actors = (
        load_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst", "category")
    )

    # 4. Join actor -> movie -> language
    actor_movies = actors.join(movies_with_lang, "tconst", "inner")

    # 5. Aggregate per actor
    actor_stats = (
        actor_movies.groupBy("nconst")
        .agg(
            F.countDistinct("language").alias("num_languages"),
            F.countDistinct("tconst").alias("successful_films_count"),
            F.array_sort(F.collect_set("language")).alias("languages"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.sum("numVotes").alias("total_votes"),
        )
    )

    # 6. Join actor names
    names = (load_silver(spark, "name_basics")
             .select("nconst", F.col("primaryName").alias("actor_name")))

    results = (actor_stats
               .join(names, "nconst", "left")
               .withColumn("actor_name", F.coalesce(F.col("actor_name"), F.col("nconst")))
               .withColumn("languages_str", F.concat_ws(", ", F.col("languages")))
               .select(
                   "nconst",
                   "actor_name",
                   "num_languages",
                   "languages_str",
                   "successful_films_count",
                   "avg_rating",
                   "total_votes",
               )
               .orderBy(F.desc("num_languages"), F.desc("successful_films_count"), F.desc("avg_rating")))

    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-rating", type=float, default=7.0,
                        help="Minimum averageRating for a film to be considered successful (default: 7.0)")
    parser.add_argument("--min-votes", type=int, default=1000,
                        help="Minimum numVotes for a film to qualify (default: 1000)")
    parser.add_argument("--top-n", type=int, default=25,
                        help="Number of rows for summary tables (default: 25)")
    args = parser.parse_args()

    spark = get_spark("imdb-q6-multilingual-actors")
    all_multilingual = find_multilingual_actors(spark, args.min_rating, args.min_votes).cache()

    # 1. Summary distribution by language count
    dist = (all_multilingual.groupBy("num_languages")
            .agg(F.count("*").alias("actor_count"))
            .filter(F.col("num_languages") >= 2)
            .orderBy("num_languages"))
    save_table(dist.toPandas(), SECTION, "q6_multilingual_summary")

    # 2. Actors with >= 2 languages
    two_plus = all_multilingual.filter(F.col("num_languages") >= 2).cache()
    count_2p = two_plus.count()
    print(f"Found {count_2p:,} actors/actresses with successful films in >= 2 languages.")

    two_plus_full_path = output_dir(SECTION) / "q6_multilingual_actors_2plus.csv"
    two_plus.toPandas().to_csv(two_plus_full_path, index=False)
    save_table(two_plus.limit(args.top_n).toPandas(), SECTION, "q6_multilingual_actors_2plus_top")

    # 3. Actors with >= 3 languages
    three_plus = all_multilingual.filter(F.col("num_languages") >= 3).cache()
    count_3p = three_plus.count()
    print(f"Found {count_3p:,} actors/actresses with successful films in >= 3 languages.")

    three_plus_full_path = output_dir(SECTION) / "q6_multilingual_actors_3plus.csv"
    three_plus_pdf = three_plus.toPandas()
    three_plus_pdf.to_csv(three_plus_full_path, index=False)
    save_table(three_plus.limit(args.top_n).toPandas(), SECTION, "q6_multilingual_actors_3plus_top")

    spark.stop()


if __name__ == "__main__":
    main()
