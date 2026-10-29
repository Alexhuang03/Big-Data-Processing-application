"""Q1: feature-length movies whose runtime is notably different from the
average runtime of their genre.

A movie with several genres is compared with each of its genres.
Two detection methods are available:
    zscore  |runtime - genre mean| / genre stddev >= threshold (default 2)
    iqr     runtime outside [Q1 - k * IQR, Q3 + k * IQR] (default k = 1.5)

Outputs (in output/exploration/):
    q1_genre_summary.csv / .md   per genre: stats and number of outliers
    q1_outliers.csv              every (movie, genre) outlier
    q1_outliers_examples.md      most extreme outliers per genre

Usage:
    python -m exploration.q1_runtime_outliers [--method zscore|iqr] [--threshold X]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import explode_genres, feature_films
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"
EXAMPLES_PER_GENRE = 3


def genre_stats(movies):
    return movies.groupBy("genre").agg(
        F.count("*").alias("movies"),
        F.avg("runtimeMinutes").alias("mean_runtime"),
        F.stddev("runtimeMinutes").alias("std_runtime"),
        F.percentile_approx("runtimeMinutes", 0.25).alias("q1_runtime"),
        F.percentile_approx("runtimeMinutes", 0.5).alias("median_runtime"),
        F.percentile_approx("runtimeMinutes", 0.75).alias("q3_runtime"),
    )


def find_outliers(movies, stats, method, threshold):
    df = movies.join(stats, "genre").withColumn(
        "z_score",
        (F.col("runtimeMinutes") - F.col("mean_runtime")) / F.col("std_runtime"))
    if method == "zscore":
        is_outlier = F.abs(F.col("z_score")) >= threshold
    else:
        iqr = F.col("q3_runtime") - F.col("q1_runtime")
        is_outlier = ((F.col("runtimeMinutes") < F.col("q1_runtime") - threshold * iqr)
                      | (F.col("runtimeMinutes") > F.col("q3_runtime") + threshold * iqr))
    return (df.filter(is_outlier)
            .withColumn("direction", F.when(F.col("z_score") > 0, "longer")
                        .otherwise("shorter")))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--method", choices=["zscore", "iqr"], default="zscore")
    parser.add_argument("--threshold", type=float, default=None,
                        help="2 for zscore, 1.5 for iqr by default")
    args = parser.parse_args()
    threshold = args.threshold or (2.0 if args.method == "zscore" else 1.5)

    spark = get_spark("imdb-q1")
    movies = explode_genres(feature_films(spark, with_ratings=False)).cache()
    stats = genre_stats(movies).cache()
    outliers = find_outliers(movies, stats, args.method, threshold).cache()

    counts = outliers.groupBy("genre").agg(
        F.count("*").alias("outliers"),
        F.sum(F.when(F.col("direction") == "longer", 1).otherwise(0)).alias("longer"),
        F.sum(F.when(F.col("direction") == "shorter", 1).otherwise(0)).alias("shorter"),
    )
    summary = (stats.join(counts, "genre", "left").fillna(0)
               .withColumn("outlier_pct", 100 * F.col("outliers") / F.col("movies"))
               .orderBy(F.desc("movies")))
    save_table(summary.toPandas(), SECTION, "q1_genre_summary")

    columns = ["genre", "tconst", "primaryTitle", "startYear", "runtimeMinutes",
               "mean_runtime", "z_score", "direction"]
    full = outliers.select(*columns).orderBy("genre", F.desc(F.abs("z_score")))
    full.toPandas().to_csv(output_dir(SECTION) / "q1_outliers.csv", index=False)

    rank = Window.partitionBy("genre", "direction").orderBy(F.desc(F.abs("z_score")))
    examples = (outliers.withColumn("rank", F.row_number().over(rank))
                .filter(F.col("rank") <= EXAMPLES_PER_GENRE)
                .select(*columns).orderBy("genre", "direction", F.desc(F.abs("z_score"))))
    save_table(examples.toPandas(), SECTION, "q1_outliers_examples")

    total = outliers.select("tconst").distinct().count()
    print(f"Method {args.method}, threshold {threshold}: {total:,} distinct movies "
          f"are runtime outliers in at least one of their genres")
    spark.stop()


if __name__ == "__main__":
    main()
