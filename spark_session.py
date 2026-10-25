"""One place that builds the SparkSession, so every layer configures Spark identically."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

# Use the user-local JRE if no system Java is on the PATH
_local_jdk = Path.home() / ".local" / "jdk"
if "JAVA_HOME" not in os.environ and _local_jdk.exists():
    os.environ["JAVA_HOME"] = str(_local_jdk)
    os.environ["PATH"] = f"{_local_jdk / 'bin'}:{os.environ['PATH']}"


def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name)
             .master("local[*]")                              # all local cores
             .config("spark.sql.session.timeZone", "Asia/Kolkata")
             .config("spark.sql.shuffle.partitions", "8")     # small data, few partitions
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
