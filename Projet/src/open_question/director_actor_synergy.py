"""Open Question: The Creative Duo Synergy in Cinema.

Investigates whether recurring collaborations between a director and an actor
produce a statistically significant lift in critical ratings (synergy delta)
and audience engagement compared to when they work independently.

Performs a 5-table distributed join:
    title_basics -> title_ratings -> title_crew -> title_principals -> name_basics

Outputs (in output/open_question/):
    open_question_top_synergy_duos.csv / .md      highest positive synergy pairs
    open_question_negative_synergy_duos.csv / .md partnerships with negative lift
    open_question_macro_summary.csv / .md         global macro performance metrics

Usage:
    python -m open_question.director_actor_synergy [--min-collabs K] [--min-votes V]
"""
import argparse

from pyspark.sql import functions as F

from common.datasets import feature_films, load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "open_question"


def analyze_director_actor_synergy(spark, min_collabs=3, min_votes=250):
    """Compute baseline vs collaborative metrics for recurring director-actor pairs."""
    # 1. Feature films with ratings
    movies = (feature_films(spark, with_ratings=True)
              .filter(F.col("averageRating").isNotNull() & (F.col("numVotes") >= min_votes))
              .select(
                  "tconst",
                  "primaryTitle",
                  "startYear",
                  "averageRating",
                  "numVotes",
              ))

    # 2. Directors (exploded from title_crew)
    directors = (load_silver(spark, "title_crew")
                 .filter(F.col("directors").isNotNull())
                 .select("tconst", F.explode("directors").alias("director_nconst"))
                 .filter(F.col("director_nconst").isNotNull()))

    # 3. Lead Actors/Actresses (from title_principals, lead roles with ordering <= 4)
    actors = (load_silver(spark, "title_principals")
              .filter(
                  F.col("category").isin("actor", "actress")
                  & (F.col("ordering") <= 4)
              )
              .select(
                  "tconst",
                  F.col("nconst").alias("actor_nconst"),
              )
              .filter(F.col("actor_nconst").isNotNull()))

    # 4. Directed movies & Acted movies
    dir_movies = movies.join(directors, "tconst", "inner")
    act_movies = movies.join(actors, "tconst", "inner")

    # Baseline performance per director
    dir_stats = dir_movies.groupBy("director_nconst").agg(
        F.count("tconst").alias("dir_total_movies"),
        F.avg("averageRating").alias("dir_overall_avg_rating"),
        F.avg("numVotes").alias("dir_overall_avg_votes"),
    )

    # Baseline performance per actor
    act_stats = act_movies.groupBy("actor_nconst").agg(
        F.count("tconst").alias("act_total_movies"),
        F.avg("averageRating").alias("act_overall_avg_rating"),
        F.avg("numVotes").alias("act_overall_avg_votes"),
    )

    # 5. Joint collaborative works (inner join on tconst)
    collabs = (dir_movies
               .join(actors, "tconst", "inner")
               # Avoid self-directing / self-acting duos
               .filter(F.col("director_nconst") != F.col("actor_nconst")))

    # Aggregate per (director, actor) pair
    pair_stats = (collabs.groupBy("director_nconst", "actor_nconst").agg(
        F.count("tconst").alias("collab_count"),
        F.avg("averageRating").alias("duo_avg_rating"),
        F.avg("numVotes").alias("duo_avg_votes"),
        F.array_sort(F.collect_set("primaryTitle")).alias("sample_movies"),
    ).filter(F.col("collab_count") >= min_collabs))

    # 6. Join with solo baselines and names
    names = (load_silver(spark, "name_basics")
             .select("nconst", F.col("primaryName").alias("name")))

    joined = (pair_stats
              .join(dir_stats, "director_nconst", "inner")
              .join(act_stats, "actor_nconst", "inner")
              .join(names.withColumnRenamed("name", "director_name"),
                    pair_stats.director_nconst == names.nconst, "left")
              .drop("nconst")
              .join(names.withColumnRenamed("name", "actor_name"),
                    pair_stats.actor_nconst == names.nconst, "left")
              .drop("nconst"))

    # Expected baseline rating = (director_solo_avg + actor_solo_avg) / 2
    # Synergy delta = duo_avg_rating - expected_baseline
    result = (joined
              .withColumn("expected_baseline_rating",
                          (F.col("dir_overall_avg_rating") + F.col("act_overall_avg_rating")) / 2.0)
              .withColumn("expected_baseline_votes",
                          (F.col("dir_overall_avg_votes") + F.col("act_overall_avg_votes")) / 2.0)
              .withColumn("synergy_rating_delta",
                          F.round(F.col("duo_avg_rating") - F.col("expected_baseline_rating"), 2))
              .withColumn("vote_multiplier",
                          F.round(F.col("duo_avg_votes") / F.col("expected_baseline_votes"), 2))
              .withColumn("duo_avg_rating", F.round(F.col("duo_avg_rating"), 2))
              .withColumn("expected_baseline_rating", F.round(F.col("expected_baseline_rating"), 2))
              .withColumn("sample_titles", F.slice(F.col("sample_movies"), 1, 3))
              .withColumn("sample_titles_str", F.concat_ws("; ", F.col("sample_titles")))
              .select(
                  "director_name",
                  "actor_name",
                  "collab_count",
                  "duo_avg_rating",
                  "expected_baseline_rating",
                  "synergy_rating_delta",
                  "vote_multiplier",
                  "sample_titles_str",
              ))

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-collabs", type=int, default=3,
                        help="Minimum collaborative films together (default: 3)")
    parser.add_argument("--min-votes", type=int, default=250,
                        help="Minimum votes per film (default: 250)")
    parser.add_argument("--top-n", type=int, default=20,
                        help="Number of rows for report tables (default: 20)")
    args = parser.parse_args()

    spark = get_spark("imdb-open-question-synergy")
    duos = analyze_director_actor_synergy(spark, args.min_collabs, args.min_votes).cache()

    total_duos = duos.count()
    print(f"Analyzed {total_duos:,} recurring creative duos (>= {args.min_collabs} collaborations).")

    # 1. Macro Summary
    macro = duos.agg(
        F.count("*").alias("total_creative_duos"),
        F.sum(F.when(F.col("synergy_rating_delta") > 0, 1).otherwise(0)).alias("positive_synergy_duos"),
        F.sum(F.when(F.col("synergy_rating_delta") < 0, 1).otherwise(0)).alias("negative_synergy_duos"),
        F.round(100.0 * F.sum(F.when(F.col("synergy_rating_delta") > 0, 1).otherwise(0)) / F.count("*"), 2).alias("positive_synergy_pct"),
        F.round(F.avg("synergy_rating_delta"), 3).alias("average_synergy_delta"),
        F.round(F.avg("vote_multiplier"), 2).alias("average_vote_multiplier"),
    ).toPandas()

    save_table(macro, SECTION, "open_question_macro_summary")

    # 2. Top Positive Synergy Duos
    top_synergy = duos.orderBy(F.desc("synergy_rating_delta"), F.desc("collab_count")).limit(args.top_n).toPandas()
    save_table(top_synergy, SECTION, "open_question_top_synergy_duos")

    # 3. Top Negative Synergy Duos (Creative Fatigue)
    negative_synergy = duos.orderBy(F.asc("synergy_rating_delta"), F.desc("collab_count")).limit(args.top_n).toPandas()
    save_table(negative_synergy, SECTION, "open_question_negative_synergy_duos")

    spark.stop()


if __name__ == "__main__":
    main()
