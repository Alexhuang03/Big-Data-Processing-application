"""Read a bronze table and apply the declared schema (no business rules yet).

    - "\\N", empty and whitespace-only values become null
    - strings are trimmed
    - int / double columns are cast with try_cast (bad values become null)
    - array columns are split into array<string>
    - the JSON "characters" column of title_principals becomes array<string>
"""
from pyspark.sql import functions as F

from common.paths import bronze_path
from common.schemas import IMDB_NULL, TABLES

# title.akas uses a control character (\x02) between array items, the other
# tables use a comma. Both are accepted so the code works for every table.
ARRAY_SEPARATOR = "[,\u0002]"


def _clean_string(name):
    trimmed = F.trim(F.col(name))
    return F.when((trimmed == F.lit(IMDB_NULL)) | (trimmed == F.lit("")), None).otherwise(trimmed)


def typed_column(name, spark_type):
    value = _clean_string(name)
    if spark_type in ("int", "double"):
        return value.try_cast(spark_type).alias(name)
    if spark_type == "array":
        return F.split(value, ARRAY_SEPARATOR).alias(name)
    if spark_type == "json_array":
        return F.from_json(value, "array<string>").alias(name)
    return value.alias(name)


def load_typed(spark, table, drop_malformed=False):
    df = spark.read.parquet(bronze_path(table))
    if drop_malformed:
        df = df.filter(F.col("_corrupt_record").isNull())
    return df.select(*[typed_column(n, t) for n, t in TABLES[table]["columns"]])


def load_all_typed(spark, drop_malformed=False):
    return {table: load_typed(spark, table, drop_malformed) for table in TABLES}
