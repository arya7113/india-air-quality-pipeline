"""
Layer 2: Load (bronze).  Read every raw gzipped CSV with PySpark and store it as Parquet.

Nothing is cleaned here. The only additions are bookkeeping columns:
  source_file  which archive file each row came from
  loaded_at    when this load ran
The folder names locationid=/year=/month= are Hive-style partitions, so Spark
turns them into columns automatically and reads all ~18k files in one call.
"""
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from config import BRONZE, RAW
from spark_session import get_spark

SCHEMA = StructType([                      # explicit: never let a pipeline guess types
    StructField("location_id", IntegerType()),
    StructField("sensors_id", IntegerType()),
    StructField("location", StringType()),
    StructField("datetime", StringType()),  # keep as text in bronze; parsed in silver
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("parameter", StringType()),
    StructField("units", StringType()),
    StructField("value", DoubleType()),
])


def main():
    t0 = time.time()
    spark = get_spark("airquality-load")

    raw_path = str(RAW / "records" / "csv.gz")
    df = (spark.read.csv(raw_path, header=True, schema=SCHEMA)   # reads every file under the path
          .withColumn("source_file", F.input_file_name())
          .withColumn("loaded_at", F.current_timestamp()))

    (df.write.mode("overwrite")
       .partitionBy("locationid", "month")       # partition columns came from the folder names
       .parquet(str(BRONZE / "readings")))

    out = spark.read.parquet(str(BRONZE / "readings"))
    n_rows = out.count()
    n_files = out.select("source_file").distinct().count()
    n_stations = out.select("locationid").distinct().count()
    print(f"[load] {n_rows:,} rows from {n_files:,} files, {n_stations} stations "
          f"-> {BRONZE / 'readings'} ({time.time() - t0:.0f}s)")
    spark.stop()


if __name__ == "__main__":
    main()
