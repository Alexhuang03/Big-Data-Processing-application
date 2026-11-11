"""Q7: actors/actresses with an "early-peak" career arc.

Criteria:
  - Played in feature films for >= 20 years  (max_year - min_year >= 20)
  - Appeared in at least 15 rated feature films (numVotes >= min_votes)
  - Their highest-rated films are concentrated in the FIRST half of their
    career (early_half_avg_rating > late_half_avg_rating)

The "early-peak score" is defined as:
    early_peak_score = early_half_avg_rating - late_half_avg_rating
A positive score means the actor peaked early.

Outputs (in output/exploration/):
    q7_early_peak_actors.csv / .md       all matching actors
    q7_early_peak_actors_top.csv / .md   top 25 by early_peak_score

Usage:
    python -m exploration.q7_career_arc [--min-votes N] [--min-films M] [--min-years Y]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import feature_films, load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"
MIN_CAREER_YEARS = 20
MIN_FILMS = 15


def find_early_peak_actors(spark, min_votes=50, min_films=MIN_FILMS,
                           min_years=MIN_CAREER_YEARS):
    """Identify actors/actresses whose best-rated films cluster in the first
    half of their career.

    Steps
    -----
    1. Build a dataset of rated feature films.
    2. Join with title_principals to get actors/actresses per film.
    3. Per actor, compute career span (min/max startYear) and mid-point.
    4. Split each actor's filmography into first-half / second-half.
    5. Compute average rating in each half.
    6. Filter: career >= min_years, film count >= min_films,
       early_avg > late_avg (early-peak condition).
    7. Compute early_peak_score = early_avg - late_avg.
    """
    # 1. Rated feature films
    movies = (
        feature_films(spark, with_ratings=True)
        .filter(F.col("averageRating").isNotNull())
        .filter(F.col("numVotes") >= min_votes)
        .filter(F.col("startYear").isNotNull())
        .select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes")
    )

    # 2. Actors / Actresses from title_principals
    principals = (
        load_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst", "category")
    )

    # 3. Actor filmography: one row per (nconst, film)
    actor_films = principals.join(movies, "tconst", "inner")

    # 4. Career span per actor
    career = (
        actor_films.groupBy("nconst")
        .agg(
            F.min("startYear").alias("career_start"),
            F.max("startYear").alias("career_end"),
            F.count("*").alias("total_films"),
        )
        .withColumn("career_span", F.col("career_end") - F.col("career_start"))
        # mid-point: exclusive upper bound for the first half
        .withColumn(
            "career_mid",
            F.col("career_start") + F.floor((F.col("career_span")) / 2),
        )
    )

    # 5. Filter by career length and film count early (reduces downstream data)
    career_filtered = career.filter(
        (F.col("career_span") >= min_years) & (F.col("total_films") >= min_films)
    )

    # 6. Join filmography with filtered careers and tag first/second half
    actor_films_tagged = (
        actor_films
        .join(career_filtered.select("nconst", "career_mid"), "nconst", "inner")
        .withColumn(
            "half",
            F.when(F.col("startYear") <= F.col("career_mid"), "early").otherwise("late"),
        )
    )

    # 7. Average rating per actor per half
    half_ratings = (
        actor_films_tagged.groupBy("nconst", "half")
        .agg(F.avg("averageRating").alias("half_avg_rating"))
    )

    early_ratings = (
        half_ratings.filter(F.col("half") == "early")
        .select("nconst", F.col("half_avg_rating").alias("early_avg_rating"))
    )
    late_ratings = (
        half_ratings.filter(F.col("half") == "late")
        .select("nconst", F.col("half_avg_rating").alias("late_avg_rating"))
    )

    # 8. Names
    names = (
        load_silver(spark, "name_basics")
        .select("nconst", F.col("primaryName").alias("actor_name"))
    )

    # 9. Assemble result
    result = (
        career_filtered
        .join(early_ratings, "nconst", "inner")
        .join(late_ratings, "nconst", "inner")
        # Early-peak condition: first half rated higher
        .filter(F.col("early_avg_rating") > F.col("late_avg_rating"))
        .withColumn(
            "early_peak_score",
            F.round(F.col("early_avg_rating") - F.col("late_avg_rating"), 3),
        )
        .withColumn("early_avg_rating", F.round(F.col("early_avg_rating"), 3))
        .withColumn("late_avg_rating", F.round(F.col("late_avg_rating"), 3))
        .join(names, "nconst", "left")
        .withColumn("actor_name", F.coalesce(F.col("actor_name"), F.col("nconst")))
        .select(
            "nconst",
            "actor_name",
            "career_start",
            "career_end",
            "career_span",
            "total_films",
            "early_avg_rating",
            "late_avg_rating",
            "early_peak_score",
        )
        .orderBy(F.desc("early_peak_score"), F.desc("total_films"))
    )

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-votes", type=int, default=50,
        help="Minimum numVotes per film to qualify (default: 50)",
    )
    parser.add_argument(
        "--min-films", type=int, default=MIN_FILMS,
        help=f"Minimum number of qualifying films (default: {MIN_FILMS})",
    )
    parser.add_argument(
        "--min-years", type=int, default=MIN_CAREER_YEARS,
        help=f"Minimum career span in years (default: {MIN_CAREER_YEARS})",
    )
    parser.add_argument(
        "--top-n", type=int, default=25,
        help="Number of top actors in the summary table (default: 25)",
    )
    args = parser.parse_args()

    spark = get_spark("imdb-q7-career-arc")

    actors_df = find_early_peak_actors(
        spark,
        min_votes=args.min_votes,
        min_films=args.min_films,
        min_years=args.min_years,
    ).cache()

    total = actors_df.count()
    print(
        f"Found {total:,} actors/actresses with an early-peak career arc "
        f"(career >= {args.min_years} yrs, films >= {args.min_films}, "
        f"min_votes={args.min_votes})."
    )

    # Save full table
    full_path = output_dir(SECTION) / "q7_early_peak_actors.csv"
    actors_df.toPandas().to_csv(full_path, index=False)
    print(f"Saved full list to {full_path} ({total} rows)")

    # Save top-N markdown/csv report
    top_df = actors_df.limit(args.top_n).toPandas()
    save_table(top_df, SECTION, "q7_early_peak_actors_top")
    print(f"Saved top {args.top_n} report.")

    spark.stop()


if __name__ == "__main__":
    main()
