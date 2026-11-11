"""Q8: multi-talented film professionals (writer+director+actor combinations).

Identifies people who hold multiple roles (writer, director, actor/actress)
either across their whole career or on the SAME film, and compares their
performance metrics to single-role peers.

Role combinations tracked:
  - writer + director + actor/actress   (triple)
  - director + actor/actress            (double)
  - writer + actor/actress              (double)
  - writer + director                   (double)

Outputs (in output/exploration/):
    q8_multi_talented_profiles.csv / .md      career profile per multi-talent
    q8_same_film_performance.csv / .md        single-role vs multi-role on same film
    q8_multi_talented_top.csv / .md           top 25 by multi_role_film_count

Usage:
    python -m exploration.q8_multi_talented [--min-votes N]
"""
import argparse

from pyspark.sql import functions as F

from common.datasets import feature_films, load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rated_films(spark, min_votes):
    """Return rated feature films with min_votes filter."""
    return (
        feature_films(spark, with_ratings=True)
        .filter(F.col("averageRating").isNotNull())
        .filter(F.col("numVotes") >= min_votes)
        .select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes")
    )


def _actor_roles(spark):
    """Return (tconst, nconst) for actors/actresses from title_principals."""
    return (
        load_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst")
        .distinct()
    )


def _director_roles(spark):
    """Return (tconst, nconst) for directors from title_crew."""
    return (
        load_silver(spark, "title_crew")
        .filter(F.col("directors").isNotNull())
        .select("tconst", F.explode("directors").alias("nconst"))
        .filter(F.col("nconst").isNotNull())
        .distinct()
    )


def _writer_roles(spark):
    """Return (tconst, nconst) for writers from title_crew."""
    return (
        load_silver(spark, "title_crew")
        .filter(F.col("writers").isNotNull())
        .select("tconst", F.explode("writers").alias("nconst"))
        .filter(F.col("nconst").isNotNull())
        .distinct()
    )


def build_all_roles(spark):
    """Combine distinct (tconst, nconst) pairs across actor, director, writer roles."""
    actors = _actor_roles(spark).withColumn("is_actor", F.lit(1))
    directors = _director_roles(spark).withColumn("is_director", F.lit(1))
    writers = _writer_roles(spark).withColumn("is_writer", F.lit(1))

    return (
        actors
        .join(directors, ["tconst", "nconst"], "outer")
        .join(writers, ["tconst", "nconst"], "outer")
        .withColumn("is_actor",    F.coalesce(F.col("is_actor"),    F.lit(0)))
        .withColumn("is_director", F.coalesce(F.col("is_director"), F.lit(0)))
        .withColumn("is_writer",   F.coalesce(F.col("is_writer"),   F.lit(0)))
    )


# ---------------------------------------------------------------------------
# Main analytical functions
# ---------------------------------------------------------------------------

def build_multi_talented_profiles(spark, all_roles, min_votes=50):
    """Build career profiles for multi-talented professionals.

    For every person, determine which combination of roles they hold across
    rated feature films, then compute:
      - total_films: distinct films they appear in (any role)
      - multi_role_film_count: films where they wear >= 2 of the tracked roles
      - avg_rating, avg_votes: averaged over ALL their qualifying films
      - role_combination: human-readable label of their role set

    Returns a DataFrame ordered by multi_role_film_count desc.
    """
    movies = _rated_films(spark, min_votes)

    # Restrict to rated feature films only
    film_roles = all_roles.join(movies, "tconst", "inner")

    # Aggregate per person: career-level role flags and film counts
    person_stats = (
        film_roles.groupBy("nconst")
        .agg(
            F.max("is_actor").alias("ever_actor"),
            F.max("is_director").alias("ever_director"),
            F.max("is_writer").alias("ever_writer"),
            F.countDistinct("tconst").alias("total_films"),
            # multi-role films: films where they hold >= 2 roles simultaneously
            F.sum(
                F.when(
                    (F.col("is_actor") + F.col("is_director") + F.col("is_writer")) >= 2,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("multi_role_film_count"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.avg("numVotes"), 0).alias("avg_votes"),
        )
    )

    # Only keep people who have ever held >= 2 of the three roles
    multi = person_stats.filter(
        (F.col("ever_actor") + F.col("ever_director") + F.col("ever_writer")) >= 2
    )

    # Build a human-readable role_combination label
    multi = multi.withColumn(
        "role_combination",
        F.when(
            (F.col("ever_writer") == 1) & (F.col("ever_director") == 1) & (F.col("ever_actor") == 1),
            F.lit("writer+director+actor"),
        ).when(
            (F.col("ever_director") == 1) & (F.col("ever_actor") == 1),
            F.lit("director+actor"),
        ).when(
            (F.col("ever_writer") == 1) & (F.col("ever_actor") == 1),
            F.lit("writer+actor"),
        ).when(
            (F.col("ever_writer") == 1) & (F.col("ever_director") == 1),
            F.lit("writer+director"),
        ).otherwise(F.lit("other")),
    )

    # Join person names
    names = (
        load_silver(spark, "name_basics")
        .select("nconst", F.col("primaryName").alias("person_name"))
    )

    result = (
        multi
        .join(names, "nconst", "left")
        .withColumn("person_name", F.coalesce(F.col("person_name"), F.col("nconst")))
        .select(
            "nconst",
            "person_name",
            "role_combination",
            "total_films",
            "multi_role_film_count",
            "avg_rating",
            "avg_votes",
        )
        .orderBy(F.desc("multi_role_film_count"), F.desc("total_films"))
    )

    return result


def compare_single_vs_multi_role(spark, all_roles, min_votes=50):
    """Compare average rating/votes when a professional works solo vs multi-role
    on the SAME film.

    For each qualifying film in a multi-talented person's filmography, classify
    each (person, film) pair as:
      - 'multi_role'  if they held >= 2 of {actor, director, writer} on that film
      - 'single_role' otherwise (they held exactly 1 of the three roles)

    Then aggregate by role_type to get:
      avg_rating, avg_numVotes, film_count

    Returns a small summary DataFrame (one row per role_type).
    """
    movies = _rated_films(spark, min_votes)
    film_roles = all_roles.join(movies, "tconst", "inner")

    # Total roles per person per film
    film_roles = film_roles.withColumn(
        "role_count",
        F.col("is_actor") + F.col("is_director") + F.col("is_writer"),
    )

    # Keep only people who are multi-talented (ever hold >= 2 roles in their career)
    career_multi = (
        film_roles.groupBy("nconst")
        .agg(
            F.max("is_actor").alias("ever_actor"),
            F.max("is_director").alias("ever_director"),
            F.max("is_writer").alias("ever_writer"),
        )
        .filter(
            (F.col("ever_actor") + F.col("ever_director") + F.col("ever_writer")) >= 2
        )
        .select("nconst")
    )

    multi_films = film_roles.join(career_multi, "nconst", "inner")

    # Classify each (person, film) pair
    classified = multi_films.withColumn(
        "role_type",
        F.when(F.col("role_count") >= 2, F.lit("multi_role")).otherwise(F.lit("single_role")),
    )

    comparison = (
        classified.groupBy("role_type")
        .agg(
            F.count("*").alias("person_film_count"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.avg("numVotes"), 0).alias("avg_numVotes"),
        )
        .orderBy("role_type")
    )

    return comparison


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-votes", type=int, default=50,
        help="Minimum numVotes per film to qualify (default: 50)",
    )
    parser.add_argument(
        "--top-n", type=int, default=25,
        help="Number of top professionals for summary table (default: 25)",
    )
    args = parser.parse_args()

    spark = get_spark("imdb-q8-multi-talented")

    # Build all_roles once and cache it
    all_roles = build_all_roles(spark).cache()

    # --- Q8-A: career profiles ---
    profiles_df = build_multi_talented_profiles(spark, all_roles, args.min_votes).cache()
    total = profiles_df.count()
    print(f"Found {total:,} multi-talented professionals (min_votes={args.min_votes}).")

    profiles_full_path = output_dir(SECTION) / "q8_multi_talented_profiles.csv"
    profiles_df.toPandas().to_csv(profiles_full_path, index=False)
    print(f"Saved profiles to {profiles_full_path} ({total} rows)")

    # Top N markdown/csv report
    save_table(profiles_df.limit(args.top_n).toPandas(), SECTION, "q8_multi_talented_top")

    # --- Q8-B: single-role vs multi-role comparison on same film ---
    comparison_df = compare_single_vs_multi_role(spark, all_roles, args.min_votes)
    # save_table automatically writes both CSV and Markdown
    save_table(comparison_df.toPandas(), SECTION, "q8_same_film_performance")

    spark.stop()


if __name__ == "__main__":
    main()
