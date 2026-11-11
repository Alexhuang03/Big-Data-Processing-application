"""Data enrichment: Wikipedia synopses and biographies for the top films.

Scope (defaults): for every (genre, year) between 1980 and 2025, the 10 most
voted feature films; for each film, the first 3 principals (IMDb billing
order, archive footage excluded). Every limit is a parameter: the same code runs on the full dataset.

Stages (all run by default, each can be run alone with --stage):
    select   Spark: choose films and principals from the silver layer
    gather   Python: download raw records (enrichment.wiki), resumable
    build    Spark: parse, clean, enforce schema, join, write, summarise

Storage (Docker volume, /home/jovyan/data/enrichment/):
    selection/films, selection/principals      Parquet
    raw/*.jsonl                                raw downloaded records
    enriched_films, enriched_principals        final Parquet tables
Reports: output/enrichment/*.csv / *.md

Usage:
    python -m enrichment.pipeline [--stage select|gather|build|all]
                                  [--top-n 10] [--principals 3]
                                  [--first-year 1980] [--last-year 2025]
                                  [--limit-films N]
"""
import argparse
import logging

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common.datasets import explode_genres, feature_films, load_silver
from common.paths import DATA_DIR
from common.report import save_table
from common.spark import get_spark
from enrichment import wiki

SECTION = "enrichment"
ENRICHMENT_DIR = DATA_DIR / "enrichment"
RAW_DIR = ENRICHMENT_DIR / "raw"
SELECTED_FILMS = str(ENRICHMENT_DIR / "selection" / "films")
SELECTED_PRINCIPALS = str(ENRICHMENT_DIR / "selection" / "principals")
ENRICHED_FILMS = str(ENRICHMENT_DIR / "enriched_films")
ENRICHED_PRINCIPALS = str(ENRICHMENT_DIR / "enriched_principals")

RAW_FILES = {
    "wikidata_films": RAW_DIR / "wikidata_films.jsonl",
    "wikidata_people": RAW_DIR / "wikidata_people.jsonl",
    "wikipedia_films": RAW_DIR / "wikipedia_films.jsonl",
    "wikipedia_people": RAW_DIR / "wikipedia_people.jsonl",
}

# Section names used for the plot of a film on English Wikipedia.
PLOT_HEADINGS = "Plot|Plot summary|Synopsis|Summary|Premise|Story|Storyline"

HTML_ENTITIES = {"&nbsp;": " ", "&ndash;": "–", "&mdash;": "—",
                 "&amp;": "&", "&quot;": '"', "&minus;": "-", "&[a-z]+;": ""}

# Explicit schemas: raw files are never read with schema inference.
_STR = StringType()
WIKIDATA_SCHEMA = StructType([StructField(n, _STR) for n in (
    "imdb_id", "status", "wikidata_uri", "wikipedia_url", "birth_date",
    "death_date", "birth_place", "retrieved_at")])
WIKIPEDIA_FILM_SCHEMA = StructType(
    [StructField(n, _STR) for n in ("imdb_id", "page_title")]
    + [StructField("revision_id", LongType())]
    + [StructField(n, _STR) for n in ("source_url", "wikitext", "status",
                                      "retrieved_at")])
WIKIPEDIA_PERSON_SCHEMA = StructType(
    [StructField(n, _STR) for n in ("imdb_id", "page_title")]
    + [StructField("revision_id", LongType())]
    + [StructField(n, _STR) for n in ("source_url", "extract", "status",
                                      "retrieved_at")])

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 1: selection (Spark)
# ---------------------------------------------------------------------------

def select_films(spark, first_year, last_year, top_n, limit_films=None):
    """Top N most voted feature films per (genre, year)."""
    movies = (feature_films(spark)
              .filter(F.col("startYear").between(first_year, last_year))
              .filter(F.col("numVotes").isNotNull()))
    ranking = Window.partitionBy("genre", "startYear").orderBy(
        F.desc("numVotes"), F.desc("averageRating"), F.asc("tconst"))
    selected = (explode_genres(movies)
                .withColumn("rank_in_genre_year", F.row_number().over(ranking))
                .filter(F.col("rank_in_genre_year") <= top_n)
                .select("tconst", "primaryTitle", "startYear", "genre",
                        "rank_in_genre_year", "averageRating", "numVotes"))
    if limit_films:
        keep = (selected.select("tconst").distinct()
                .orderBy("tconst").limit(limit_films))
        selected = selected.join(keep, "tconst", "left_semi")
    return selected


# Archive footage / sound credits are not real participation in the film.
EXCLUDED_CATEGORIES = ["archive_footage", "archive_sound"]


def select_principals(spark, films, per_film):
    """First principals of each film in IMDb billing order."""
    film_ids = films.select("tconst").distinct()
    names = load_silver(spark, "name_basics").select("nconst", "primaryName")
    billing = Window.partitionBy("tconst").orderBy("ordering")
    return (load_silver(spark, "title_principals")
            .join(film_ids, "tconst", "left_semi")
            .filter(~F.col("category").isin(EXCLUDED_CATEGORIES))
            .withColumn("principal_rank", F.row_number().over(billing))
            .filter(F.col("principal_rank") <= per_film)
            .join(names, "nconst", "left")
            .select("tconst", "principal_rank", "nconst", "primaryName", "category"))


def run_select(spark, args):
    films = select_films(spark, args.first_year, args.last_year, args.top_n,
                         args.limit_films)
    films.write.mode("overwrite").parquet(SELECTED_FILMS)
    films = spark.read.parquet(SELECTED_FILMS)
    principals = select_principals(spark, films, args.principals)
    principals.write.mode("overwrite").parquet(SELECTED_PRINCIPALS)
    principals = spark.read.parquet(SELECTED_PRINCIPALS)
    print(f"Selected {films.select('tconst').distinct().count():,} films "
          f"({films.count():,} film-genre-year rows) and "
          f"{principals.select('nconst').distinct().count():,} people "
          f"({principals.count():,} film-person rows)")


# ---------------------------------------------------------------------------
# Stage 2: gathering (plain Python, outside Spark)
# ---------------------------------------------------------------------------

def _distinct_ids(spark, path, column):
    df = spark.read.parquet(path).select(column).distinct()
    return [row[column] for row in df.toLocalIterator()]


def run_gather(spark):
    client = wiki.PoliteClient()
    film_ids = _distinct_ids(spark, SELECTED_FILMS, "tconst")
    person_ids = _distinct_ids(spark, SELECTED_PRINCIPALS, "nconst")
    wiki.fetch_wikidata(client, film_ids, "films", RAW_FILES["wikidata_films"])
    wiki.fetch_wikidata(client, person_ids, "people", RAW_FILES["wikidata_people"])
    wiki.fetch_film_wikitext(client, RAW_FILES["wikidata_films"],
                             RAW_FILES["wikipedia_films"])
    wiki.fetch_person_extracts(client, RAW_FILES["wikidata_people"],
                               RAW_FILES["wikipedia_people"])
    print(f"Gathering done: {client.requests_sent:,} HTTP requests sent")


# ---------------------------------------------------------------------------
# Stage 3: cleaning and enrichment (Spark)
# ---------------------------------------------------------------------------

def clean_wikitext(column):
    """Turn MediaWiki markup into plain text with Spark regular expressions."""
    c = F.regexp_replace(column, r"(?s)<!--.*?-->", "")
    c = F.regexp_replace(c, r"(?is)<ref[^>]*/>", "")
    c = F.regexp_replace(c, r"(?is)<ref[^>]*>.*?</ref>", "")
    for _ in range(4):  # nested templates {{...{{...}}...}}
        c = F.regexp_replace(c, r"\{\{[^{}]*\}\}", "")
    c = F.regexp_replace(c, r"(?s)\{\|.*?\|\}", "")  # tables
    for _ in range(3):  # files may contain links in their caption
        c = F.regexp_replace(c, r"(?i)\[\[(?:File|Image):[^\[\]]*\]\]", "")
        c = F.regexp_replace(c, r"\[\[(?:[^\[\]|]*\|)?([^\[\]|]*)\]\]", "$1")
    c = F.regexp_replace(c, r"\[https?://[^\s\]]+\s*([^\]]*)\]", "$1")
    c = F.regexp_replace(c, r"'{2,}", "")
    c = F.regexp_replace(c, r"(?s)<[^>]+>", "")
    for entity, char in HTML_ENTITIES.items():
        c = F.regexp_replace(c, entity, char)
    c = F.regexp_replace(c, r"(?m)^=+[^=\n]*=+\s*$", "")   # sub-headings
    c = F.regexp_replace(c, r"(?m)^[*#:;]+\s*", "")         # list markers
    return clean_text(c)


def clean_text(column):
    c = F.regexp_replace(column, r"\(\s*[;,]?\s*\)", "")   # emptied parentheses
    c = F.trim(F.regexp_replace(c, r"\s+", " "))
    return F.when(F.length(c) > 0, c)


def build_films(spark, principals_per_film):
    films = spark.read.parquet(SELECTED_FILMS)
    principal_counts = (spark.read.parquet(SELECTED_PRINCIPALS)
                        .groupBy("tconst").agg(F.count("*").alias("principals_found")))
    wikidata = spark.read.schema(WIKIDATA_SCHEMA).json(str(RAW_FILES["wikidata_films"]))
    pages = spark.read.schema(WIKIPEDIA_FILM_SCHEMA).json(str(RAW_FILES["wikipedia_films"]))

    plot_regex = rf"(?msi)^==\s*(?:{PLOT_HEADINGS})\s*==[ \t]*$(.*?)(?=^==[^=]|\z)"
    lead_regex = r"(?ms)\A(.*?)(?=^==|\z)"
    pages = (pages.dropDuplicates(["imdb_id"])
             .withColumn("plot", clean_wikitext(F.regexp_extract("wikitext", plot_regex, 1)))
             .withColumn("lead", clean_wikitext(F.regexp_extract("wikitext", lead_regex, 1)))
             .select(F.col("imdb_id").alias("tconst"), "page_title", "revision_id",
                     "source_url", "plot", "lead",
                     F.col("status").alias("page_status"),
                     F.col("retrieved_at").alias("page_retrieved_at")))
    wikidata = (wikidata.dropDuplicates(["imdb_id"])
                .select(F.col("imdb_id").alias("tconst"),
                        F.col("status").alias("wikidata_status"),
                        "wikidata_uri", "wikipedia_url"))

    df = (films.join(wikidata, "tconst", "left").join(pages, "tconst", "left")
          .join(principal_counts, "tconst", "left")
          .fillna(0, ["principals_found"]))
    synopsis = F.coalesce("plot", "lead")
    note = (F.when(F.col("wikidata_status").isNull(), "not gathered yet")
            .when(F.col("wikidata_status") == "no_wikidata_item",
                  "no Wikidata item linked to this IMDb id")
            .when(F.col("wikipedia_url").isNull(), "no English Wikipedia article")
            .when(F.col("page_status") != "ok", "Wikipedia page could not be read")
            .when(F.col("plot").isNotNull(), None)
            .when(F.col("lead").isNotNull(), "no plot section, lead section used")
            .otherwise("article has no usable text"))
    return df.select(
        "tconst", "primaryTitle", "startYear", "genre", "rank_in_genre_year",
        "averageRating", "numVotes",
        synopsis.alias("synopsis"),
        F.when(F.col("plot").isNotNull(), "plot")
         .when(F.col("lead").isNotNull(), "lead").alias("synopsis_type"),
        "page_title", "revision_id", "wikidata_uri",
        # Lineage: exact Wikipedia revision, else the Wikidata item, else the query service.
        F.coalesce("source_url", "wikidata_uri", F.lit(wiki.SPARQL_URL)).alias("lineage"),
        F.col("page_retrieved_at").cast("timestamp").alias("retrieved_at"),
        note.alias("enrichment_note"),
        "principals_found",
        F.when(F.col("principals_found") < principals_per_film,
               F.concat(F.lit("only "), F.col("principals_found").cast("string"),
                        F.lit(" principal(s) listed on IMDb"))).alias("principals_note"),
    )


def _to_date(column):
    return F.substring(column, 1, 10).try_cast("date")


def build_principals(spark):
    principals = spark.read.parquet(SELECTED_PRINCIPALS)
    wikidata = spark.read.schema(WIKIDATA_SCHEMA).json(str(RAW_FILES["wikidata_people"]))
    pages = spark.read.schema(WIKIPEDIA_PERSON_SCHEMA).json(str(RAW_FILES["wikipedia_people"]))

    wikidata = (wikidata.dropDuplicates(["imdb_id"])
                .select(F.col("imdb_id").alias("nconst"),
                        F.col("status").alias("wikidata_status"),
                        "wikidata_uri", "wikipedia_url",
                        _to_date("birth_date").alias("birth_date"),
                        _to_date("death_date").alias("death_date"),
                        F.trim("birth_place").alias("birth_place")))
    pages = (pages.dropDuplicates(["imdb_id"])
             .select(F.col("imdb_id").alias("nconst"), "page_title", "revision_id",
                     "source_url", clean_text(F.col("extract")).alias("biography"),
                     F.col("status").alias("page_status"),
                     F.col("retrieved_at").cast("timestamp").alias("retrieved_at")))

    df = principals.join(wikidata, "nconst", "left").join(pages, "nconst", "left")
    note = (F.when(F.col("wikidata_status").isNull(), "not gathered yet")
            .when(F.col("wikidata_status") == "no_wikidata_item",
                  "no Wikidata item linked to this IMDb id")
            .when(F.col("wikipedia_url").isNull(),
                  "no English Wikipedia article (Wikidata facts only)")
            .when(F.col("page_status") != "ok", "Wikipedia page could not be read")
            .when(F.col("biography").isNull(), "article has no usable text"))
    return df.select(
        "tconst", "principal_rank", "nconst", "primaryName", "category",
        "biography", "birth_date", "death_date", "birth_place",
        "page_title", "revision_id", "wikidata_uri",
        F.coalesce("source_url", "wikidata_uri", F.lit(wiki.SPARQL_URL)).alias("lineage"),
        "retrieved_at",
        note.alias("enrichment_note"),
    )


def coverage(df, key):
    """Share of distinct films / people per enrichment result."""
    distinct = df.dropDuplicates([key])
    total = distinct.count()
    return (distinct
            .groupBy(F.coalesce("enrichment_note", F.lit("enriched")).alias("result"))
            .agg(F.count("*").alias("count"))
            .withColumn("pct", 100 * F.col("count") / total)
            .orderBy(F.desc("count")))


def run_build(spark, principals_per_film):
    films = build_films(spark, principals_per_film)
    films.write.mode("overwrite").parquet(ENRICHED_FILMS)
    films = spark.read.parquet(ENRICHED_FILMS)
    principals = build_principals(spark)
    principals.write.mode("overwrite").parquet(ENRICHED_PRINCIPALS)
    principals = spark.read.parquet(ENRICHED_PRINCIPALS)

    save_table(coverage(films, "tconst").toPandas(), SECTION, "film_coverage")
    save_table(coverage(principals, "nconst").toPandas(), SECTION, "principal_coverage")

    by_decade = (films.dropDuplicates(["tconst"])
                 .withColumn("decade", (F.floor(F.col("startYear") / 10) * 10).cast("int"))
                 .groupBy("decade").agg(
                     F.count("*").alias("films"),
                     (100 * F.avg(F.col("synopsis").isNotNull().cast("int"))).alias("with_synopsis_pct"),
                     (100 * F.avg(F.coalesce((F.col("synopsis_type") == "plot").cast("int"), F.lit(0)))).alias("with_plot_pct"))
                 .orderBy("decade"))
    save_table(by_decade.toPandas(), SECTION, "film_coverage_by_decade")

    film_sample = (films.filter(F.col("synopsis").isNotNull())
                   .dropDuplicates(["tconst"]).orderBy(F.desc("numVotes")).limit(10)
                   .select("tconst", "primaryTitle", "startYear", "synopsis_type",
                           F.concat(F.substring("synopsis", 1, 200), F.lit("...")).alias("synopsis"),
                           "lineage"))
    save_table(film_sample.toPandas(), SECTION, "film_sample")
    person_sample = (principals.filter(F.col("biography").isNotNull())
                     .dropDuplicates(["nconst"]).orderBy("nconst").limit(10)
                     .select("nconst", "primaryName", "category", "birth_date", "birth_place",
                             F.concat(F.substring("biography", 1, 160), F.lit("...")).alias("biography"),
                             "lineage"))
    save_table(person_sample.toPandas(), SECTION, "principal_sample")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage", choices=["select", "gather", "build", "all"], default="all")
    parser.add_argument("--first-year", type=int, default=1980)
    parser.add_argument("--last-year", type=int, default=2025)
    parser.add_argument("--top-n", type=int, default=10, help="films per genre and year")
    parser.add_argument("--principals", type=int, default=3, help="principals per film")
    parser.add_argument("--limit-films", type=int, default=None,
                        help="keep only N films (quick test run)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = get_spark("imdb-enrichment")
    if args.stage in ("select", "all"):
        run_select(spark, args)
    if args.stage in ("gather", "all"):
        run_gather(spark)
    if args.stage in ("build", "all"):
        run_build(spark, args.principals)
    spark.stop()


if __name__ == "__main__":
    main()
