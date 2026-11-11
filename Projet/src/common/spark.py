"""SparkSession factory used by every job of the project."""
import os

from pyspark.sql import SparkSession


def kafka_package():
    """Maven coordinates of the Kafka connector matching the Spark version."""
    import pyspark
    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def get_spark(app_name, packages=None, conf=None):
    """Create (or reuse) a local SparkSession configured from env variables.

    packages: extra Maven packages (e.g. [kafka_package()]), downloaded once.
    conf: extra Spark settings as a dict.
    """
    builder = (
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
    )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
