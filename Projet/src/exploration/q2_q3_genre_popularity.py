"""Q2: top three most popular movie genres for each year from 1980 to 2025.
Q3: how the top three genres of a year compare with the other genres.

Popularity of a genre in a year = total number of IMDb votes received by the
feature films of that genre released that year (--metric votes, default).
--metric movies uses the number of films instead.

Outputs (in output/exploration/):
    q2_top3_genres_by_year.csv / .md     3 rows per year
    q2_top3_frequency.csv / .md          how often each genre is in the top 3
    q3_top3_vs_rest_by_year.csv / .md    per year: top 3 group vs other genres
    q3_top3_vs_rest_summary.csv / .md    averages over all years

Usage:
    python -m exploration.q2_q3_genre_popularity [--metric votes|movies]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import explode_genres, feature_films
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"
FIRST_YEAR, LAST_YEAR = 1980, 2025
TOP_N = 3


def genre_year_stats(spark):
    movies = (feature_films(spark)
              .filter(F.col("startYear").between(FIRST_YEAR, LAST_YEAR)))
    return explode_genres(movies).groupBy("startYear", "genre").agg(
        F.count("*").alias("movies"),
        F.count("numVotes").alias("rated_movies"),
        F.coalesce(F.sum("numVotes"), F.lit(0)).alias("votes"),
        F.avg("averageRating").alias("avg_rating"),
        # Vote-weighted rating: what the audience of the genre thought overall.
        (F.sum(F.col("averageRating") * F.col("numVotes"))
         / F.sum("numVotes")).alias("weighted_rating"),
    )


def rank_genres(stats, metric):
    order = [F.desc(metric), F.desc("votes" if metric == "movies" else "movies"),
             F.asc("genre")]
    window = Window.partitionBy("startYear").orderBy(*order)
    year_total = Window.partitionBy("startYear")
    return (stats
            .withColumn("rank", F.row_number().over(window))
            .withColumn("share_pct", 100 * F.col(metric) / F.sum(metric).over(year_total))
            .withColumn("genre_group", F.when(F.col("rank") <= TOP_N, "top3").otherwise("rest")))


def compare_groups(ranked):
    by_group = ranked.groupBy("startYear", "genre_group").agg(
        F.count("*").alias("genres"),
        F.sum("movies").alias("movies"),
        F.sum("votes").alias("votes"),
        F.sum("share_pct").alias("share_pct"),
        (F.sum("votes") / F.sum("rated_movies")).alias("votes_per_movie"),
        F.avg("avg_rating").alias("avg_rating"),
    )
    top = by_group.filter("genre_group = 'top3'").drop("genre_group")
    rest = by_group.filter("genre_group = 'rest'").drop("genre_group")
    renamed_rest = rest.select("startYear", *[F.col(c).alias(f"rest_{c}")
                                              for c in rest.columns if c != "startYear"])
    renamed_top = top.select("startYear", *[F.col(c).alias(f"top3_{c}")
                                            for c in top.columns if c != "startYear"])
    return (renamed_top.join(renamed_rest, "startYear")
            .withColumn("votes_per_movie_ratio",
                        F.col("top3_votes_per_movie") / F.col("rest_votes_per_movie"))
            .withColumn("rating_gap", F.col("top3_avg_rating") - F.col("rest_avg_rating"))
            .orderBy("startYear"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metric", choices=["votes", "movies"], default="votes")
    args = parser.parse_args()

    spark = get_spark("imdb-q2-q3")
    ranked = rank_genres(genre_year_stats(spark), args.metric).cache()

    top3 = (ranked.filter(F.col("rank") <= TOP_N)
            .select("startYear", "rank", "genre", "movies", "votes", "share_pct",
                    "avg_rating")
            .orderBy("startYear", "rank"))
    save_table(top3.toPandas(), SECTION, "q2_top3_genres_by_year")

    frequency = (ranked.filter(F.col("rank") <= TOP_N).groupBy("genre").agg(
        F.count("*").alias("years_in_top3"),
        F.sum(F.when(F.col("rank") == 1, 1).otherwise(0)).alias("years_ranked_first"),
        F.min("startYear").alias("first_year"),
        F.max("startYear").alias("last_year"),
    ).orderBy(F.desc("years_in_top3")))
    print(save_table(frequency.toPandas(), SECTION, "q2_top3_frequency"))

    comparison = compare_groups(ranked).cache()
    save_table(comparison.toPandas(), SECTION, "q3_top3_vs_rest_by_year")

    numeric = [c for c in comparison.columns if c != "startYear"]
    summary = comparison.agg(*[F.avg(c).alias(c) for c in numeric]).toPandas()
    summary = summary.T.reset_index()
    summary.columns = ["metric", f"average_{FIRST_YEAR}_{LAST_YEAR}"]
    print(save_table(summary, SECTION, "q3_top3_vs_rest_summary"))
    spark.stop()


if __name__ == "__main__":
    main()
