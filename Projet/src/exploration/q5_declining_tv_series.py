"""Q5: television series with at least 3 seasons of >= 10 episodes,
where the worst episode occurred in the last season and the best episode
occurred within the first two seasons.

Outputs (in output/exploration/):
    q5_declining_tv_series.csv / .md         all matching television series
    q5_declining_tv_series_top.csv / .md     top series with the steepest quality drop

Usage:
    python -m exploration.q5_declining_tv_series [--min-seasons N] [--min-episodes-per-season M]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.datasets import load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"


def find_declining_series(spark, min_seasons=3, min_eps_per_season=10, min_votes=10):
    """Identify TV series matching Q5 criteria."""
    # 1. Load TV series basics
    series_basics = (load_silver(spark, "title_basics")
                     .filter(F.col("titleType").isin("tvSeries", "tvMiniSeries"))
                     .select(
                         F.col("tconst").alias("series_id"),
                         F.col("primaryTitle").alias("series_title"),
                         F.col("startYear"),
                         F.col("endYear"),
                     ))

    # 2. Load episode metadata and join with episode titles and ratings
    episodes = (load_silver(spark, "title_episode")
                .filter(
                    F.col("seasonNumber").isNotNull()
                    & (F.col("seasonNumber") > 0)
                    & F.col("episodeNumber").isNotNull()
                    & (F.col("episodeNumber") > 0)
                )
                .select(
                    F.col("tconst").alias("ep_id"),
                    F.col("parentTconst").alias("series_id"),
                    F.col("seasonNumber"),
                    F.col("episodeNumber"),
                ))

    ep_ratings = (load_silver(spark, "title_ratings")
                  .filter(F.col("numVotes") >= min_votes)
                  .select(
                      F.col("tconst").alias("ep_id"),
                      F.col("averageRating").alias("ep_rating"),
                      F.col("numVotes").alias("ep_votes"),
                  ))

    ep_basics = (load_silver(spark, "title_basics")
                 .select(
                     F.col("tconst").alias("ep_id"),
                     F.col("primaryTitle").alias("ep_title"),
                 ))

    # Join episode data
    ep_full = (episodes
               .join(ep_ratings, "ep_id", "inner")
               .join(ep_basics, "ep_id", "left")
               .withColumn("ep_title", F.coalesce(F.col("ep_title"), F.col("ep_id"))))

    # 3. Check seasons criteria: at least min_seasons having >= min_eps_per_season
    season_counts = (ep_full.groupBy("series_id", "seasonNumber")
                     .agg(F.count("*").alias("ep_count")))

    qualifying_seasons = (season_counts
                          .filter(F.col("ep_count") >= min_eps_per_season)
                          .groupBy("series_id")
                          .agg(
                              F.count("*").alias("qualifying_seasons_count"),
                              F.max("seasonNumber").alias("max_qualifying_season"),
                          )
                          .filter(F.col("qualifying_seasons_count") >= min_seasons))

    # Series max season overall
    series_overall = (ep_full.groupBy("series_id")
                      .agg(
                          F.max("seasonNumber").alias("last_season"),
                          F.countDistinct("seasonNumber").alias("total_seasons"),
                          F.count("*").alias("total_episodes"),
                          F.min("ep_rating").alias("series_min_rating"),
                          F.max("ep_rating").alias("series_max_rating"),
                      ))

    candidates = (series_basics
                  .join(qualifying_seasons, "series_id", "inner")
                  .join(series_overall, "series_id", "inner")
                  .filter(F.col("series_max_rating") > F.col("series_min_rating")))

    # 4. Attach episodes with min and max ratings
    ep_with_series = ep_full.join(candidates, "series_id", "inner")

    # Find worst episode(s): rating == series_min_rating
    worst_window = Window.partitionBy("series_id").orderBy(
        F.asc("ep_rating"), F.desc("seasonNumber"), F.desc("episodeNumber")
    )
    worst_eps = (ep_with_series
                 .filter(F.col("ep_rating") == F.col("series_min_rating"))
                 .withColumn("rk", F.row_number().over(worst_window))
                 .filter(F.col("rk") == 1)
                 .select(
                     "series_id",
                     F.col("seasonNumber").alias("worst_season"),
                     F.col("episodeNumber").alias("worst_ep_num"),
                     F.col("ep_title").alias("worst_ep_title"),
                     F.col("ep_rating").alias("worst_rating"),
                 ))

    # Find best episode(s): rating == series_max_rating
    best_window = Window.partitionBy("series_id").orderBy(
        F.desc("ep_rating"), F.asc("seasonNumber"), F.asc("episodeNumber")
    )
    best_eps = (ep_with_series
                .filter(F.col("ep_rating") == F.col("series_max_rating"))
                .withColumn("rk", F.row_number().over(best_window))
                .filter(F.col("rk") == 1)
                .select(
                    "series_id",
                    F.col("seasonNumber").alias("best_season"),
                    F.col("episodeNumber").alias("best_ep_num"),
                    F.col("ep_title").alias("best_ep_title"),
                    F.col("ep_rating").alias("best_rating"),
                ))

    # 5. Filter: worst episode in last season, best episode in season 1 or 2
    matched = (candidates
               .join(worst_eps, "series_id", "inner")
               .join(best_eps, "series_id", "inner")
               .filter(
                   (F.col("worst_season") == F.col("last_season"))
                   & (F.col("best_season").isin(1, 2))
               )
               .withColumn("rating_drop", F.round(F.col("best_rating") - F.col("worst_rating"), 2))
               .select(
                   "series_id",
                   "series_title",
                   "startYear",
                   "endYear",
                   "total_seasons",
                   "qualifying_seasons_count",
                   "total_episodes",
                   "best_season",
                   "best_ep_num",
                   "best_ep_title",
                   "best_rating",
                   "worst_season",
                   "worst_ep_num",
                   "worst_ep_title",
                   "worst_rating",
                   "rating_drop",
               )
               .orderBy(F.desc("rating_drop"), F.desc("total_episodes")))

    return matched


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-seasons", type=int, default=3,
                        help="Minimum number of qualifying seasons (default: 3)")
    parser.add_argument("--min-episodes-per-season", type=int, default=10,
                        help="Minimum episodes per season to qualify (default: 10)")
    parser.add_argument("--min-votes", type=int, default=10,
                        help="Minimum votes per episode (default: 10)")
    parser.add_argument("--top-n", type=int, default=20,
                        help="Top N series in markdown report (default: 20)")
    args = parser.parse_args()

    spark = get_spark("imdb-q5-declining-tv-series")
    results_df = find_declining_series(
        spark,
        min_seasons=args.min_seasons,
        min_eps_per_season=args.min_episodes_per_season,
        min_votes=args.min_votes,
    ).cache()

    total_count = results_df.count()
    print(f"Found {total_count:,} television series matching Q5 criteria.")

    # Save full CSV
    full_pdf = results_df.toPandas()
    full_path = output_dir(SECTION) / "q5_declining_tv_series.csv"
    full_pdf.to_csv(full_path, index=False)
    print(f"Saved full results to {full_path} ({len(full_pdf)} rows)")

    # Save top N table
    top_pdf = results_df.limit(args.top_n).toPandas()
    save_table(top_pdf, SECTION, "q5_declining_tv_series_top")

    spark.stop()


if __name__ == "__main__":
    main()
