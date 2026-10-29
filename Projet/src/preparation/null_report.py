"""Percentage of null / missing / empty values for every column of every table.

For each column we count, on the bronze layer (raw strings):
    null_count     no value at all (field missing from a malformed row)
    marker_count   the IMDb null marker "\\N"
    empty_count    empty or whitespace-only string
    invalid_count  value present but not castable to the expected type
                   (reported separately, not counted as missing)

Outputs (in output/preparation/):
    null_report.csv / .md          one row per (table, column)
    top5_null_columns.csv / .md    five columns with the highest missing %

Usage:
    python -m preparation.null_report
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.paths import bronze_path
from common.report import save_table
from common.schemas import CAST_TYPES, IMDB_NULL, TABLES
from common.spark import get_spark

SECTION = "preparation"


def column_metrics(table, name, spark_type):
    col = F.col(name)
    is_null = col.isNull()
    is_marker = col == F.lit(IMDB_NULL)
    is_empty = F.trim(col) == F.lit("")
    is_present = ~(is_null | is_marker | is_empty)

    if spark_type in CAST_TYPES:
        cast_failed = F.expr(f"try_cast(`{name}` AS {spark_type})").isNull()
        is_invalid = is_present & cast_failed
    else:
        is_invalid = F.lit(False)

    def count_if(condition, alias):
        return F.sum(F.when(condition, 1).otherwise(0)).alias(f"{name}__{alias}")

    return [
        count_if(is_null, "null"),
        count_if(is_marker, "marker"),
        count_if(is_empty, "empty"),
        count_if(is_invalid, "invalid"),
    ]


def table_report(spark, table):
    """One pass over the table computing the metrics of all its columns."""
    df = spark.read.parquet(bronze_path(table))
    columns = TABLES[table]["columns"]
    aggregations = [F.count(F.lit(1)).alias("__rows")]
    for name, spark_type in columns:
        aggregations += column_metrics(table, name, spark_type)
    stats = df.agg(*aggregations).first().asDict()

    rows = []
    total = stats["__rows"]
    for name, spark_type in columns:
        null = stats[f"{name}__null"]
        marker = stats[f"{name}__marker"]
        empty = stats[f"{name}__empty"]
        invalid = stats[f"{name}__invalid"]
        missing = null + marker + empty
        rows.append(
            (table, name, spark_type, total, null, marker, empty, missing,
             100.0 * missing / total if total else 0.0,
             invalid, 100.0 * invalid / total if total else 0.0)
        )
    return rows


def main():
    spark = get_spark("imdb-null-report")
    rows = []
    for table in TABLES:
        print(f"Analysing {table}...")
        rows += table_report(spark, table)

    columns = ["table", "column", "type", "rows", "null_count", "marker_count",
               "empty_count", "missing_count", "missing_pct", "invalid_count",
               "invalid_pct"]
    report = spark.createDataFrame(rows, columns).toPandas()
    save_table(report, SECTION, "null_report")

    top5 = (report.sort_values("missing_pct", ascending=False)
            .head(5)[["table", "column", "rows", "missing_count", "missing_pct"]])
    print(save_table(top5, SECTION, "top5_null_columns"))
    spark.stop()


if __name__ == "__main__":
    main()
