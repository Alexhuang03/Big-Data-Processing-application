"""SparkSession factory used by every job of the project."""
import os

from pyspark.sql import SparkSession


def get_spark(app_name):
    """Create (or reuse) a local SparkSession configured from env variables."""
    spark = (
        SparkSession.builder.appName(app_name)
        # A fixed number of cores keeps enough memory per task (about
        # driver memory / cores); local[*] on a 24-core machine ran out of heap.
        .master(os.environ.get("SPARK_MASTER", "local[8]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
