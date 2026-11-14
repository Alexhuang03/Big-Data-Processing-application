"""Spark Structured Streaming: Wikipedia edits relevant to the IMDb dataset.

Pipeline
    Kafka topic (producer.py) -> parse JSON -> event time + watermark
    -> match the article title against a static IMDb lookup (broadcast join)
    -> drop duplicate events within the watermark -> sinks

Matching (an article title can point to a film or a person):
    "Heat (1995 film)"      -> film with that title and year   film_title_year
    "Heat (film)"           -> film with that title            film_title
    "Tom Hanks (actor)"     -> person with that name           person_disambiguated
    "Heat" / "Tom Hanks"    -> exact title or name             exact_title / exact_name
Matching is case-sensitive, like Wikipedia titles.
The lookup holds films with at least --min-votes IMDb votes and the actors,
directors and writers credited on them.

Streaming queries (Parquet sinks in /home/jovyan/data/streaming/):
    matched_events   every matched event                      append
    matched_windows  matches per tumbling window and type     append (watermark)
    all_windows      all events per window (match rate base)  append (watermark)

Late data: event time is the edit time written by Wikipedia. A watermark of
--watermark (default 2 minutes) bounds how late an event may arrive; older
events are dropped by the stateful operators and counted in the metrics.

After --duration seconds the queries are stopped and summary tables and
streaming metrics are written to output/streaming/.

Usage (inside the imdb_spark container, producer running):
    python -m streaming.streaming_job [--duration 1200] [--window "5 minutes"]
                                      [--watermark "2 minutes"] [--min-votes 1000]
"""
import argparse
import json
import os
import shutil
import time

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, LongType, StringType, StructField,
                               StructType)

from common.datasets import feature_films, load_silver
from common.paths import DATA_DIR
from common.report import save_table
from common.spark import get_spark, kafka_package

SECTION = "streaming"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-edits")
STREAMING_DIR = DATA_DIR / "streaming"
PERSON_CATEGORIES = ["actor", "actress", "director", "writer"]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("title", StringType()),
    StructField("wiki", StringType()),
    StructField("type", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("comment", StringType()),
    StructField("length_old", LongType()),
    StructField("length_new", LongType()),
])

# "Title (1995 film)", "Title (film)", "Title (1995 American film)"
FILM_TITLE = r"^(.+?) \((?:(\d{4}) )?(?:[A-Za-z.'\- ]+ )?film\)$"
# "Name (actor)", "Name (American actress)", "Name (director)"
PERSON_TITLE = (r"^(.+?) \((?:[A-Za-z.'\- ]+ )?"
                r"(?:actor|actress|director|filmmaker|screenwriter|writer|producer)\)$")


# ---------------------------------------------------------------------------
# Static lookup (batch, from the silver layer)
# ---------------------------------------------------------------------------

def build_lookup(spark, min_votes):
    films = (feature_films(spark)
             .filter(F.col("numVotes") >= min_votes)
             .select("tconst", "primaryTitle", "startYear", "numVotes"))
    film_keys = films.select(
        F.col("primaryTitle").alias("key"),
        F.col("startYear").alias("year"),
        F.lit("film").alias("entity_type"),
        F.col("tconst").alias("imdb_id"),
        F.col("primaryTitle").alias("imdb_name"),
        F.col("numVotes").alias("popularity"),
    )
    credits = (load_silver(spark, "title_principals")
               .filter(F.col("category").isin(PERSON_CATEGORIES))
               .join(films.select("tconst", "numVotes"), "tconst")
               .groupBy("nconst").agg(F.sum("numVotes").alias("popularity")))
    person_keys = (credits
                   .join(load_silver(spark, "name_basics").select("nconst", "primaryName"),
                         "nconst")
                   .select(F.col("primaryName").alias("key"),
                           F.lit(None).cast("int").alias("year"),
                           F.lit("person").alias("entity_type"),
                           F.col("nconst").alias("imdb_id"),
                           F.col("primaryName").alias("imdb_name"),
                           "popularity"))
    # Several films/people can share a name: keep the most popular one.
    best = Window.partitionBy("key", "entity_type", "year").orderBy(F.desc("popularity"))
    return (film_keys.unionByName(person_keys)
            .withColumn("rank", F.row_number().over(best))
            .filter("rank = 1").drop("rank"))


# ---------------------------------------------------------------------------
# Streaming transformations
# ---------------------------------------------------------------------------

def read_events(spark, watermark, starting_offsets):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", starting_offsets)
           .option("failOnDataLoss", "false")
           .load())
    events = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                         F.col("timestamp").alias("ingest_time"))
              .select("e.*", "ingest_time")
              .withColumn("event_time", F.col("timestamp").cast("timestamp"))
              .withColumn("delay_seconds",
                          F.col("ingest_time").cast("double") - F.col("timestamp"))
              .filter(F.col("title").isNotNull() & F.col("event_time").isNotNull()))
    return events.withWatermark("event_time", watermark)


def add_match_keys(events):
    title = F.col("title")
    is_film = title.rlike(FILM_TITLE)
    is_person = title.rlike(PERSON_TITLE)
    return (events
            .withColumn("title_suffix", F.when(is_film, "film")
                        .when(is_person, "person").otherwise("none"))
            # Case-sensitive: Wikipedia titles are ("BEG" is not the film "Beg").
            .withColumn("match_key",
                        F.when(is_film, F.regexp_extract(title, FILM_TITLE, 1))
                        .when(is_person, F.regexp_extract(title, PERSON_TITLE, 1))
                        .otherwise(title))
            .withColumn("title_year", F.when(is_film, F.regexp_extract(title, FILM_TITLE, 2))
                        .try_cast("int")))


def match_events(events, lookup):
    lk = F.broadcast(lookup.alias("lk"))
    suffix = F.col("title_suffix")
    condition = (F.col("match_key") == F.col("lk.key")) & (
        ((suffix == "film") & (F.col("lk.entity_type") == "film")
         & (F.col("title_year").isNull() | (F.col("title_year") == F.col("lk.year"))))
        | ((suffix == "person") & (F.col("lk.entity_type") == "person"))
        | (suffix == "none"))
    match_type = (F.when(suffix == "film",
                         F.when(F.col("title_year").isNotNull(), "film_title_year")
                         .otherwise("film_title"))
                  .when(suffix == "person", "person_disambiguated")
                  .when(F.col("lk.entity_type") == "film", "exact_title")
                  .otherwise("exact_name"))
    return (events.join(lk, condition, "inner")
            .withColumn("match_type", match_type)
            # One row per Wikipedia event, even if it matched several entities
            # or was delivered twice (dedup state is purged by the watermark).
            .dropDuplicatesWithinWatermark(["event_id"])
            .select("event_id", "event_time", "ingest_time", "delay_seconds", "title",
                    "type", "user", "bot", "comment", "match_type",
                    F.col("lk.entity_type").alias("entity_type"),
                    F.col("lk.imdb_id").alias("imdb_id"),
                    F.col("lk.imdb_name").alias("imdb_name"),
                    F.col("lk.year").alias("imdb_year")))


def window_counts(df, window, *group_cols):
    # countDistinct is not supported in streaming aggregations: approx_count_distinct is.
    return (df.groupBy(F.window("event_time", window).alias("w"), *group_cols)
            .agg(F.count("*").alias("events"),
                 F.approx_count_distinct("user").alias("distinct_users"),
                 F.approx_count_distinct("title").alias("distinct_pages"),
                 F.avg("delay_seconds").alias("avg_delay_seconds"),
                 F.max("delay_seconds").alias("max_delay_seconds"))
            .select(F.col("w.start").alias("window_start"),
                    F.col("w.end").alias("window_end"), *group_cols,
                    "events", "distinct_users", "distinct_pages",
                    "avg_delay_seconds", "max_delay_seconds"))


def start_sink(df, name, trigger):
    return (df.writeStream.queryName(name).outputMode("append").format("parquet")
            .option("path", str(STREAMING_DIR / name))
            .option("checkpointLocation", str(STREAMING_DIR / "checkpoints" / name))
            .trigger(processingTime=trigger)
            .start())


# ---------------------------------------------------------------------------
# Metrics and summaries (batch, after the queries are stopped)
# ---------------------------------------------------------------------------

def progress_dicts(query):
    return [p if isinstance(p, dict) else json.loads(p.json) for p in query.recentProgress]


def metrics_table(queries):
    rows = []
    for query in queries:
        progress = [p for p in progress_dicts(query) if p.get("numInputRows") is not None]
        dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                      for p in progress for op in p.get("stateOperators", []))
        non_empty = [p for p in progress if p["numInputRows"] > 0]
        rows.append({
            "query": query.name,
            "micro_batches": len(progress),
            "non_empty_batches": len(non_empty),
            "input_rows": sum(p["numInputRows"] for p in progress),
            "avg_input_rows_per_s": _mean(non_empty, "inputRowsPerSecond"),
            "avg_processed_rows_per_s": _mean(non_empty, "processedRowsPerSecond"),
            "avg_batch_ms": _mean([p["durationMs"] for p in non_empty], "triggerExecution"),
            "rows_dropped_by_watermark": dropped,
            "last_watermark": progress[-1].get("eventTime", {}).get("watermark") if progress else None,
        })
        path = STREAMING_DIR / f"progress_{query.name}.json"
        path.write_text(json.dumps(progress, indent=1, default=str), encoding="utf-8")
    return pd.DataFrame(rows)


def _mean(items, key):
    values = [item[key] for item in items if item.get(key) is not None]
    return sum(values) / len(values) if values else None


def write_summaries(spark):
    matched = spark.read.parquet(str(STREAMING_DIR / "matched_events"))
    by_type = (matched.groupBy("match_type", "entity_type")
               .agg(F.count("*").alias("events"),
                    F.countDistinct("imdb_id").alias("distinct_entities"))
               .orderBy(F.desc("events")))
    save_table(by_type.toPandas(), SECTION, "matches_by_type")

    top = (matched.groupBy("entity_type", "imdb_id", "imdb_name")
           .agg(F.count("*").alias("edits"), F.countDistinct("user").alias("editors"),
                F.max("title").alias("wikipedia_title"))
           .orderBy(F.desc("edits"), "imdb_name").limit(20))
    save_table(top.toPandas(), SECTION, "top_matched_entities")

    sample = (matched.orderBy(F.desc("event_time")).limit(15)
              .select("event_time", "title", "match_type", "imdb_id", "imdb_name",
                      "imdb_year", "user", "bot"))
    save_table(sample.toPandas(), SECTION, "matched_events_sample")

    delays = matched.agg(
        F.count("*").alias("matched_events"),
        F.min("event_time").alias("first_event"), F.max("event_time").alias("last_event"),
        F.expr("percentile(delay_seconds, 0.5)").alias("median_delay_s"),
        F.expr("percentile(delay_seconds, 0.99)").alias("p99_delay_s"),
        F.max("delay_seconds").alias("max_delay_s"))
    save_table(delays.toPandas(), SECTION, "matched_delay_summary")

    for name in ("matched_windows", "all_windows"):
        path = STREAMING_DIR / name
        if any(path.glob("*.parquet")):
            windows = spark.read.parquet(str(path)).orderBy(*(
                ["window_start", "match_type"] if name == "matched_windows" else ["window_start"]))
            save_table(windows.toPandas(), SECTION, name)
        else:
            print(f"{name}: no window closed yet (run longer than window + watermark)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duration", type=int, default=1200, help="seconds (default 1200)")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--watermark", default="2 minutes")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--min-votes", type=int, default=1000)
    parser.add_argument("--starting-offsets", choices=["latest", "earliest"], default="latest",
                        help="latest: only events produced during this run")
    args = parser.parse_args()

    shutil.rmtree(STREAMING_DIR, ignore_errors=True)  # each run starts from scratch
    STREAMING_DIR.mkdir(parents=True)
    spark = get_spark("imdb-streaming", packages=[kafka_package()], conf={
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.streaming.numRecentProgressUpdates": "1000",
    })

    lookup = build_lookup(spark, args.min_votes).cache()
    print(f"Lookup: {lookup.count():,} IMDb films and people")

    events = add_match_keys(read_events(spark, args.watermark, args.starting_offsets))
    matched = match_events(events, lookup)
    queries = [
        start_sink(matched, "matched_events", args.trigger),
        start_sink(window_counts(matched, args.window, "match_type"),
                   "matched_windows", args.trigger),
        start_sink(window_counts(events, args.window), "all_windows", args.trigger),
    ]

    deadline = time.time() + args.duration
    while time.time() < deadline:
        time.sleep(min(60, max(1, deadline - time.time())))
        for query in queries:
            if query.exception():
                raise RuntimeError(f"{query.name} failed: {query.exception()}")
        status = ", ".join(
            f"{q.name}={sum(p.get('numInputRows', 0) for p in progress_dicts(q))}"
            for q in queries)
        print(f"[{int(deadline - time.time())}s left] input rows: {status}", flush=True)

    for query in queries:
        query.stop()
    save_table(metrics_table(queries), SECTION, "streaming_metrics")
    write_summaries(spark)
    spark.stop()


if __name__ == "__main__":
    main()
