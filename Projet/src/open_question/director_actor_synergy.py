"""Open Question: The Creative Duo Synergy in Cinema.

Investigates whether recurring collaborations between a director and an actor
produce a measurable lift in critical ratings (synergy delta) and audience
engagement compared to when each artist works independently (excluding their joint films).

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
    """Compute uncontaminated baseline vs collaborative metrics for recurring director-actor pairs."""
    # 1. Feature films with ratings
    movies = (
        feature_films(spark, with_ratings=True)
        .filter(F.col("averageRating").isNotNull() & (F.col("numVotes") >= min_votes))
        .select(
            "tconst",
            "primaryTitle",
            "startYear",
            "averageRating",
            "numVotes",
        )
    )

    # 2. Directors (exploded from title_crew)
    directors = (
        load_silver(spark, "title_crew")
        .filter(F.col("directors").isNotNull())
        .select("tconst", F.explode("directors").alias("director_nconst"))
        .filter(F.col("director_nconst").isNotNull())
    )

    # 3. Lead Actors/Actresses (from title_principals, lead roles with ordering <= 4)
    actors = (
        load_silver(spark, "title_principals")
        .filter(
            F.col("category").isin("actor", "actress")
            & (F.col("ordering") <= 4)
        )
        .select(
            "tconst",
            F.col("nconst").alias("actor_nconst"),
        )
        .filter(F.col("actor_nconst").isNotNull())
    )

    # 4. Directed movies & Acted movies
    dir_movies = movies.join(directors, "tconst", "inner")
    act_movies = movies.join(actors, "tconst", "inner")

    # Overall totals per director (sums and counts to derive clean non-joint baselines)
    dir_stats = dir_movies.groupBy("director_nconst").agg(
        F.count("tconst").alias("dir_total_movies"),
        F.sum("averageRating").alias("dir_sum_rating"),
        F.sum("numVotes").alias("dir_sum_votes"),
    )

    # Overall totals per actor
    act_stats = act_movies.groupBy("actor_nconst").agg(
        F.count("tconst").alias("act_total_movies"),
        F.sum("averageRating").alias("act_sum_rating"),
        F.sum("numVotes").alias("act_sum_votes"),
    )

    # 5. Joint collaborative works (inner join on tconst)
    collabs = (
        dir_movies
        .join(actors, "tconst", "inner")
        # Avoid self-directing / self-acting duos
        .filter(F.col("director_nconst") != F.col("actor_nconst"))
    )

    # Aggregate per (director, actor) pair
    pair_stats = (
        collabs.groupBy("director_nconst", "actor_nconst")
        .agg(
            F.count("tconst").alias("collab_count"),
            F.sum("averageRating").alias("duo_sum_rating"),
            F.sum("numVotes").alias("duo_sum_votes"),
            F.avg("averageRating").alias("duo_avg_rating"),
            F.avg("numVotes").alias("duo_avg_votes"),
            F.array_sort(F.collect_set("primaryTitle")).alias("sample_movies"),
        )
        .filter(F.col("collab_count") >= min_collabs)
    )

    # 6. Join duos with director totals and actor totals
    joined = (
        pair_stats
        .join(dir_stats, "director_nconst", "inner")
        .join(act_stats, "actor_nconst", "inner")
    )

    # 7. Compute UNCONTAMINATED baselines (subtracting joint films from each artist's total)
    # Both director and actor must have at least 1 film outside their partnership
    clean_baselines = (
        joined
        .withColumn("dir_solo_count", F.col("dir_total_movies") - F.col("collab_count"))
        .withColumn("act_solo_count", F.col("act_total_movies") - F.col("collab_count"))
        .filter((F.col("dir_solo_count") >= 1) & (F.col("act_solo_count") >= 1))
        .withColumn(
            "dir_solo_avg_rating",
            (F.col("dir_sum_rating") - F.col("duo_sum_rating")) / F.col("dir_solo_count"),
        )
        .withColumn(
            "dir_solo_avg_votes",
            (F.col("dir_sum_votes") - F.col("duo_sum_votes")) / F.col("dir_solo_count"),
        )
        .withColumn(
            "act_solo_avg_rating",
            (F.col("act_sum_rating") - F.col("duo_sum_rating")) / F.col("act_solo_count"),
        )
        .withColumn(
            "act_solo_avg_votes",
            (F.col("act_sum_votes") - F.col("duo_sum_votes")) / F.col("act_solo_count"),
        )
        .withColumn(
            "expected_baseline_rating",
            (F.col("dir_solo_avg_rating") + F.col("act_solo_avg_rating")) / 2.0,
        )
        .withColumn(
            "expected_baseline_votes",
            (F.col("dir_solo_avg_votes") + F.col("act_solo_avg_votes")) / 2.0,
        )
        .withColumn(
            "synergy_rating_delta",
            F.round(F.col("duo_avg_rating") - F.col("expected_baseline_rating"), 2),
        )
        .withColumn(
            "vote_multiplier",
            F.round(F.col("duo_avg_votes") / F.col("expected_baseline_votes"), 2),
        )
        .withColumn("duo_avg_rating", F.round(F.col("duo_avg_rating"), 2))
        .withColumn("expected_baseline_rating", F.round(F.col("expected_baseline_rating"), 2))
    )

    # 8. Join names with unambiguous aliases
    names_d = (
        load_silver(spark, "name_basics")
        .select(F.col("nconst"), F.col("primaryName").alias("director_name"))
        .alias("d")
    )
    names_a = (
        load_silver(spark, "name_basics")
        .select(F.col("nconst"), F.col("primaryName").alias("actor_name"))
        .alias("a")
    )

    result = (
        clean_baselines
        .join(names_d, F.col("director_nconst") == F.col("d.nconst"), "left")
        .drop("nconst")
        .join(names_a, F.col("actor_nconst") == F.col("a.nconst"), "left")
        .drop("nconst")
        .withColumn("director_name", F.coalesce(F.col("director_name"), F.col("director_nconst")))
        .withColumn("actor_name", F.coalesce(F.col("actor_name"), F.col("actor_nconst")))
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
        )
    )

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
