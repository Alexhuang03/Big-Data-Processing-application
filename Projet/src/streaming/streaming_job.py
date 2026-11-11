"""
IMDb Big Data Processing — Spark Structured Streaming Job
==========================================================
Consumes the ``wikipedia-edits`` Kafka topic produced by ``producer.py``
and matches edit events against the IMDb feature-film catalogue loaded from
the silver layer.

Architecture
------------
1.  Load a broadcast copy of IMDb feature-film titles from the silver layer.
2.  Read the Kafka stream and parse the JSON payload.
3.  Enrich each event with the event timestamp and apply a 10-minute watermark
    to handle late-arriving data.
4.  Filter: keep only events whose ``title`` matches an IMDb film title.
5.  Aggregate over a 5-minute sliding window (slide = 1 minute):
      - edit_count          — number of edits in the window
      - distinct_editors    — count of distinct Wikipedia users
      - last_comment        — most recent edit comment
6.  Write results in append mode to Parquet under ``OUTPUT_DIR``.
7.  Checkpoint state to ``CHECKPOINT_DIR`` for fault tolerance and exactly-once
    semantics.

Late-arriving data strategy
-----------------------------
*  ``withWatermark("event_time", "10 minutes")`` tells Spark to wait up to
   10 minutes past the window close before finalising the aggregate, so
   events delayed by up to 10 minutes are still included.
*  In append output mode Spark only emits a window row once it is considered
   complete (i.e. event_time watermark has advanced past window_end).
*  The checkpoint directory persists offsets and aggregation state across
   restarts, so a container restart does not lose partially-accumulated
   windows.

Usage (inside the imdb_spark container):
    python -m streaming.streaming_job

Environment variables (with defaults):
    KAFKA_BOOTSTRAP_SERVERS   kafka:19092
    KAFKA_TOPIC               wikipedia-edits
    SILVER_PATH               /home/jovyan/data/silver
    OUTPUT_DIR                /home/jovyan/output/streaming
    CHECKPOINT_DIR            /home/jovyan/data/streaming/checkpoints
    TRIGGER_SECONDS           60      (micro-batch interval, 0 = as-fast-as-possible)
"""

import logging
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.spark import get_spark

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC: str = os.environ.get("KAFKA_TOPIC", "wikipedia-edits")
SILVER_PATH: str = os.environ.get("SILVER_PATH", "/home/jovyan/data/silver")
OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "/home/jovyan/output/streaming")
CHECKPOINT_DIR: str = os.environ.get(
    "CHECKPOINT_DIR", "/home/jovyan/data/streaming/checkpoints"
)
TRIGGER_SECONDS: int = int(os.environ.get("TRIGGER_SECONDS", "60"))

# Window / watermark parameters
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION = "1 minute"
WATERMARK_DELAY = "10 minutes"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [streaming_job] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema for the JSON payload produced by producer.py
# ---------------------------------------------------------------------------
EDIT_SCHEMA = StructType(
    [
        StructField("title", StringType(), nullable=False),
        StructField("wiki", StringType(), nullable=True),
        StructField("timestamp", LongType(), nullable=True),   # Unix epoch seconds
        StructField("user", StringType(), nullable=True),
        StructField("comment", StringType(), nullable=True),
        StructField("server_name", StringType(), nullable=True),
        StructField("namespace", IntegerType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_spark() -> SparkSession:
    """
    Return a SparkSession configured for the Kafka Structured Streaming job.
    The spark-sql-kafka connector JAR must be on the class-path (provided by
    the pyspark-notebook image or via --packages at submit time).
    """
    spark = get_spark("imdb-streaming-job")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    return spark


def _load_imdb_titles(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """
    Load all feature-film primary titles from the silver layer and return a
    DataFrame with a single column ``imdb_title`` (upper-cased for
    case-insensitive matching).
    """
    title_basics = spark.read.parquet(f"{SILVER_PATH}/title_basics")
    feature_films = (
        title_basics.filter(F.col("titleType") == "movie")
        .select(
            F.upper(F.col("primaryTitle")).alias("imdb_title"),
            F.col("tconst"),
            F.col("startYear"),
        )
        .distinct()
    )
    count = feature_films.count()
    log.info("Loaded %d distinct IMDb feature-film titles for broadcast.", count)
    return feature_films


def _build_kafka_stream(spark: SparkSession):
    """
    Return a streaming DataFrame reading from the ``wikipedia-edits`` topic.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
    )


def _parse_stream(raw_df):
    """
    Cast the raw Kafka ``value`` bytes to a string, parse the JSON payload,
    and derive a proper event-time column from the Unix-epoch ``timestamp``
    field.
    """
    parsed = (
        raw_df.select(
            F.from_json(
                F.col("value").cast(StringType()), EDIT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp").cast(TimestampType())),
        )
    )
    return parsed


def _filter_imdb_matches(parsed_df, imdb_df):
    """
    Keep only edit events whose title matches an IMDb feature-film title
    (case-insensitive).  Uses a broadcast join so the full title list is
    shipped to every executor only once.
    """
    imdb_broadcast = F.broadcast(imdb_df)
    matched = parsed_df.join(
        imdb_broadcast,
        F.upper(F.col("title")) == F.col("imdb_title"),
        how="inner",
    )
    return matched


def _aggregate_windows(matched_df):
    """
    Sliding-window aggregation with watermark for late data tolerance.

    Window:    5-minute duration, sliding every 1 minute.
    Watermark: 10 minutes — events delayed up to 10 minutes are still counted.
    Mode:      append — a window row is emitted only once it is finalised.
    """
    windowed = (
        matched_df.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION).alias(
                "window"
            ),
            F.col("title").alias("imdb_title"),
            F.col("tconst"),
        )
        .agg(
            F.count("*").alias("edit_count"),
            F.countDistinct("user").alias("distinct_editors"),
            F.last("comment", ignorenulls=True).alias("last_comment"),
            F.first("startYear").alias("start_year"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("imdb_title"),
            F.col("tconst"),
            F.col("start_year"),
            F.col("edit_count"),
            F.col("distinct_editors"),
            F.col("last_comment"),
        )
    )
    return windowed


def _write_stream(agg_df, output_dir: str, checkpoint_dir: str):
    """
    Write the aggregated streaming results in append mode to Parquet.
    Partitioned by date derived from window_start for easy downstream reads.
    """
    trigger = (
        {"processingTime": f"{TRIGGER_SECONDS} seconds"}
        if TRIGGER_SECONDS > 0
        else {"once": True}
    )

    return (
        agg_df.withColumn(
            "date_partition", F.to_date(F.col("window_start"))
        )
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_dir)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("date_partition")
        .trigger(**trigger)
        .start()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info(
        "Starting streaming job | Kafka=%s | Topic=%s", BOOTSTRAP_SERVERS, TOPIC
    )
    spark = _get_spark()

    # 1. Load IMDb titles (static, from silver layer)
    imdb_df = _load_imdb_titles(spark)

    # 2. Read Kafka stream
    raw_df = _build_kafka_stream(spark)

    # 3. Parse JSON payload + derive event_time
    parsed_df = _parse_stream(raw_df)

    # 4. Filter: keep only edits matching an IMDb feature film
    matched_df = _filter_imdb_matches(parsed_df, imdb_df)

    # 5. Sliding-window aggregation with watermark
    agg_df = _aggregate_windows(matched_df)

    # 6. Write to Parquet (append mode) with checkpoint
    query = _write_stream(agg_df, OUTPUT_DIR, CHECKPOINT_DIR)

    log.info(
        "Streaming query started (id=%s). Writing to %s", query.id, OUTPUT_DIR
    )
    log.info(
        "Checkpoint at %s | Window=%s | Watermark=%s",
        CHECKPOINT_DIR, WINDOW_DURATION, WATERMARK_DELAY,
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received — stopping query gracefully.")
        query.stop()
    finally:
        spark.stop()
        log.info("Streaming job stopped.")


if __name__ == "__main__":
    main()
