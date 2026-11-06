"""Q4: directors who directed exactly one movie with a rating >= 8.0,
but multiple other movies with an average rating <= 6.0.

Outputs (in output/exploration/):
    q4_one_hit_directors.csv / .md         all matching directors
    q4_one_hit_directors_top.csv / .md     top directors with the largest filmographies

Usage:
    python -m exploration.q4_one_hit_directors [--min-votes N] [--min-other M]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import feature_films, load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"
HIT_RATING_THRESHOLD = 8.0
OTHER_RATING_CEILING = 6.0


def find_one_hit_directors(spark, min_votes=50, min_other=2):
    """Identify directors meeting Q4 criteria."""
    # 1. Feature films with ratings
    movies = (feature_films(spark, with_ratings=True)
              .filter(F.col("averageRating").isNotNull())
              .filter(F.col("numVotes") >= min_votes)
              .select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes"))

    # 2. Explode directors from title_crew
    crew = (load_silver(spark, "title_crew")
            .filter(F.col("directors").isNotNull())
            .select("tconst", F.explode("directors").alias("nconst"))
            .filter(F.col("nconst").isNotNull()))

    # 3. Directed movies
    directed = crew.join(movies, "tconst", "inner")

    # 4. Aggregate per director
    is_hit = F.when(F.col("averageRating") >= HIT_RATING_THRESHOLD, 1).otherwise(0)
    is_other = F.when(F.col("averageRating") < HIT_RATING_THRESHOLD, 1).otherwise(0)

    stats = directed.groupBy("nconst").agg(
        F.count("*").alias("total_movies"),
        F.sum(is_hit).alias("hit_count"),
        F.sum(is_other).alias("other_movies_count"),
        F.avg(F.when(is_other == 1, F.col("averageRating"))).alias("other_avg_rating"),
        F.sum(F.when(is_other == 1, F.col("numVotes"))).alias("other_total_votes"),
    )

    # 5. Filter: exactly 1 hit, multiple other movies (>= min_other), other avg <= 6.0
    qualifying = stats.filter(
        (F.col("hit_count") == 1)
        & (F.col("other_movies_count") >= min_other)
        & (F.col("other_avg_rating") <= OTHER_RATING_CEILING)
    )

    # 6. Extract the single hit movie details per director
    hit_movies = (directed.filter(F.col("averageRating") >= HIT_RATING_THRESHOLD)
                  .join(qualifying.select("nconst"), "nconst", "inner")
                  .select(
                      F.col("nconst"),
                      F.col("tconst").alias("hit_tconst"),
                      F.col("primaryTitle").alias("hit_title"),
                      F.col("startYear").alias("hit_year"),
                      F.col("averageRating").alias("hit_rating"),
                      F.col("numVotes").alias("hit_votes"),
                  ))

    # 7. Join with director names from name_basics
    names = (load_silver(spark, "name_basics")
             .select("nconst", F.col("primaryName").alias("director_name")))

    result = (qualifying
              .join(hit_movies, "nconst", "inner")
              .join(names, "nconst", "left")
              .withColumn("director_name", F.coalesce(F.col("director_name"), F.col("nconst")))
              .withColumn("other_avg_rating", F.round(F.col("other_avg_rating"), 2))
              .withColumn("rating_gap", F.round(F.col("hit_rating") - F.col("other_avg_rating"), 2))
              .select(
                  "nconst",
                  "director_name",
                  "hit_title",
                  "hit_year",
                  "hit_rating",
                  "other_movies_count",
                  "other_avg_rating",
                  "rating_gap",
                  "total_movies",
                  "hit_tconst",
              )
              .orderBy(F.desc("rating_gap"), F.desc("other_movies_count")))

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-votes", type=int, default=50,
                        help="Minimum votes per movie to qualify (default: 50)")
    parser.add_argument("--min-other", type=int, default=2,
                        help="Minimum number of other movies directed (default: 2)")
    parser.add_argument("--top-n", type=int, default=20,
                        help="Number of top examples to output in summary table (default: 20)")
    args = parser.parse_args()

    spark = get_spark("imdb-q4-one-hit-directors")
    directors_df = find_one_hit_directors(spark, args.min_votes, args.min_other).cache()

    total_count = directors_df.count()
    print(f"Found {total_count:,} directors matching Q4 criteria "
          f"(min_votes={args.min_votes}, min_other={args.min_other}).")

    # Save full table
    full_pdf = directors_df.toPandas()
    full_path = output_dir(SECTION) / "q4_one_hit_directors.csv"
    full_pdf.to_csv(full_path, index=False)
    print(f"Saved full list to {full_path} ({len(full_pdf)} rows)")

    # Save top N table (for report markdown)
    top_df = directors_df.limit(args.top_n).toPandas()
    save_table(top_df, SECTION, "q4_one_hit_directors_top")

    spark.stop()


if __name__ == "__main__":
    main()
