"""Convert the raw IMDb TSV files into the bronze layer (Parquet, all strings).

Nothing is cleaned here: every value is kept exactly as written in the file
(including the "\\N" null marker) so that the null analysis can tell apart
the different kinds of missing values. Rows that do not have the expected
number of fields are kept and their raw text is stored in "_corrupt_record".

Usage:
    python -m preparation.ingest [table ...]
"""
import gzip
import math
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.paths import RAW_DIR, bronze_path
from common.schemas import TABLES, column_names
from common.spark import get_spark

CORRUPT_COLUMN = "_corrupt_record"
# gzip files cannot be split, so we repartition after reading:
# roughly one partition per 32 MB of compressed input.
COMPRESSED_BYTES_PER_PARTITION = 32 * 1024 * 1024


def check_header(path, expected):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        header = handle.readline().rstrip("\n").split("\t")
    if header != expected:
        raise ValueError(f"{path.name}: unexpected header {header}, expected {expected}")


def read_raw_tsv(spark, table):
    path = RAW_DIR / TABLES[table]["file"]
    columns = column_names(table)
    check_header(path, columns)

    schema = StructType(
        [StructField(name, StringType(), True) for name in columns]
        + [StructField(CORRUPT_COLUMN, StringType(), True)]
    )
    df = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        # IMDb does not quote fields: titles may contain " characters.
        .option("quote", "")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(schema)
        .load(str(path))
    )
    partitions = max(1, math.ceil(path.stat().st_size / COMPRESSED_BYTES_PER_PARTITION))
    return df.repartition(partitions)


def ingest(spark, table):
    df = read_raw_tsv(spark, table)
    df.write.mode("overwrite").parquet(bronze_path(table))
    stored = spark.read.parquet(bronze_path(table))
    stats = stored.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(CORRUPT_COLUMN).alias("malformed"),
    ).first()
    print(f"  {table}: {stats['rows']:,} rows ({stats['malformed']:,} malformed)")


def main():
    tables = sys.argv[1:] or list(TABLES)
    spark = get_spark("imdb-ingest")
    for table in tables:
        ingest(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
