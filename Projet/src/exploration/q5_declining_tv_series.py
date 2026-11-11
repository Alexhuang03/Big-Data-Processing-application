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

from pyspark.sql import functions as F

from common.datasets import load_silver
from common.paths import output_dir
from common.report import save_table
from common.spark import get_spark

SECTION = "exploration"


def find_declining_series(spark, min_seasons=3, min_eps_per_season=10, min_votes=10):
    """Identify TV series matching Q5 criteria.

    Criteria:
      - Series has at least `min_seasons` (>= 3) with at least `min_eps_per_season` (>= 10)
        episodes each (counted on all episodes from title_episode, unconstrained by ratings).
      - The series' absolute worst rated episode occurs in the LAST season (and strictly nowhere else).
      - The series' absolute best rated episode occurs in Season 1 or 2 (and is not exceeded later).
    """
    # 1. Load TV series basics
    series_basics = (
        load_silver(spark, "title_basics")
        .filter(F.col("titleType").isin("tvSeries", "tvMiniSeries"))
        .select(
            F.col("tconst").alias("series_id"),
            F.col("primaryTitle").alias("series_title"),
            F.col("startYear"),
            F.col("endYear"),
        )
    )

    # 2. Season metadata directly from title_episode (all episodes)
    episodes_raw = (
        load_silver(spark, "title_episode")
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
        )
    )

    # 3. Check qualifying seasons on ALL episodes (unconstrained by ratings)
    season_ep_counts = (
        episodes_raw.groupBy("series_id", "seasonNumber")
        .agg(F.count("*").alias("ep_count"))
    )

    qualifying_seasons = (
        season_ep_counts.filter(F.col("ep_count") >= min_eps_per_season)
        .groupBy("series_id")
        .agg(
            F.count("*").alias("qualifying_seasons_count"),
        )
        .filter(F.col("qualifying_seasons_count") >= min_seasons)
    )

    # Last season and total episodes derived from title_episode
    series_structure = (
        episodes_raw.groupBy("series_id")
        .agg(
            F.max("seasonNumber").alias("last_season"),
            F.countDistinct("seasonNumber").alias("total_seasons"),
            F.count("*").alias("total_episodes"),
        )
    )

    series_candidates = (
        series_basics
        .join(qualifying_seasons, "series_id", "inner")
        .join(series_structure, "series_id", "inner")
    )

    # 4. Attach rated episodes
    ep_ratings = (
        load_silver(spark, "title_ratings")
        .filter(F.col("numVotes") >= min_votes)
        .select(
            F.col("tconst").alias("ep_id"),
            F.col("averageRating").alias("ep_rating"),
            F.col("numVotes").alias("ep_votes"),
        )
    )

    ep_basics = (
        load_silver(spark, "title_basics")
        .select(
            F.col("tconst").alias("ep_id"),
            F.col("primaryTitle").alias("ep_title"),
        )
    )

    ep_rated = (
        episodes_raw
        .join(ep_ratings, "ep_id", "inner")
        .join(ep_basics, "ep_id", "left")
        .withColumn("ep_title", F.coalesce(F.col("ep_title"), F.col("ep_id")))
        .join(series_candidates.select("series_id", "last_season"), "series_id", "inner")
    )

    # Global min and max ratings per series (among rated episodes)
    rating_extremes = (
        ep_rated.groupBy("series_id")
        .agg(
            F.min("ep_rating").alias("series_min_rating"),
            F.max("ep_rating").alias("series_max_rating"),
        )
        .filter(F.col("series_max_rating") > F.col("series_min_rating"))
    )

    ep_rated_extremes = ep_rated.join(rating_extremes, "series_id", "inner")

    # 5. Strict neutrality on ties:
    # - All episodes having the worst rating must be in the last season
    # - All episodes having the best rating must be in seasons 1 or 2
    worst_episodes = (
        ep_rated_extremes
        .filter(F.col("ep_rating") == F.col("series_min_rating"))
    )

    worst_seasons_check = (
        worst_episodes.groupBy("series_id", "last_season")
        .agg(
            F.min("seasonNumber").alias("min_season_of_worst"),
            F.max("seasonNumber").alias("max_season_of_worst"),
            F.first("episodeNumber").alias("worst_ep_num"),
            F.first("ep_title").alias("worst_ep_title"),
            F.first("ep_rating").alias("worst_rating"),
        )
        # Neutral condition: every episode tied for the worst rating must be in the last season
        .filter(
            (F.col("min_season_of_worst") == F.col("last_season"))
            & (F.col("max_season_of_worst") == F.col("last_season"))
        )
        .withColumn("worst_season", F.col("last_season"))
        .select("series_id", "worst_season", "worst_ep_num", "worst_ep_title", "worst_rating")
    )

    best_episodes = (
        ep_rated_extremes
        .filter(F.col("ep_rating") == F.col("series_max_rating"))
    )

    best_seasons_check = (
        best_episodes.groupBy("series_id")
        .agg(
            F.max("seasonNumber").alias("max_season_of_best"),
            F.first("seasonNumber").alias("best_season"),
            F.first("episodeNumber").alias("best_ep_num"),
            F.first("ep_title").alias("best_ep_title"),
            F.first("ep_rating").alias("best_rating"),
        )
        # Neutral condition: no episode with the top rating occurred after season 2
        .filter(F.col("max_season_of_best").isin(1, 2))
        .select("series_id", "best_season", "best_ep_num", "best_ep_title", "best_rating")
    )

    # 6. Final assembly
    matched = (
        series_candidates
        .join(worst_seasons_check, "series_id", "inner")
        .join(best_seasons_check, "series_id", "inner")
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
        .orderBy(F.desc("rating_drop"), F.desc("total_episodes"))
    )

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
