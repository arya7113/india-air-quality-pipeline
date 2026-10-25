# Databricks notebook source
# MAGIC %md
# MAGIC # India Air Quality ELT pipeline on Databricks
# MAGIC Same pipeline as the GitHub repo (`extract -> load -> transform -> quality checks`),
# MAGIC run on Databricks Free Edition with Delta tables instead of Parquet + SQLite.
# MAGIC 10 stations / 10 cities so the download finishes in about a minute.
# MAGIC
# MAGIC Run cells top to bottom (Run all).

# COMMAND ----------

# MAGIC %md ## 0. Config

# COMMAND ----------

YEAR = 2025
WHO_PM25_DAILY_LIMIT = 15.0
MIN_HOURS_PER_DAY = 18
ARCHIVE_URL = "https://openaq-data-archive.s3.amazonaws.com"

CATALOG, SCHEMA, VOLUME = "workspace", "default", "airquality"
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw"          # files land here

STATIONS = [
    {
        "location_id": 17,
        "name": "R K Puram, Delhi - DPCC",
        "city": "Delhi"
    },
    {
        "location_id": 6927,
        "name": "Colaba, Mumbai - MPCB",
        "city": "Mumbai"
    },
    {
        "location_id": 5548,
        "name": "BTM Layout, Bengaluru - CPCB",
        "city": "Bengaluru"
    },
    {
        "location_id": 407,
        "name": "Zoo Park, Hyderabad - TSPCB",
        "city": "Hyderabad"
    },
    {
        "location_id": 11609,
        "name": "Mhada Colony, Pune - IITM",
        "city": "Pune"
    },
    {
        "location_id": 2586,
        "name": "Manali, Chennai - CPCB",
        "city": "Chennai"
    },
    {
        "location_id": 6950,
        "name": "Jadavpur, Kolkata - WBPCB",
        "city": "Kolkata"
    },
    {
        "location_id": 2456,
        "name": "Talkatora District Industries Center, Lucknow - CPCB",
        "city": "Lucknow"
    },
    {
        "location_id": 5612,
        "name": "Shastri Nagar, Jaipur - RSPCB",
        "city": "Jaipur"
    },
    {
        "location_id": 5652,
        "name": "IGSC Planetarium Complex, Patna - BSPCB",
        "city": "Patna"
    }
]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"{len(STATIONS)} stations, raw path: {RAW}")

# COMMAND ----------

# MAGIC %md ## 1. Extract: download daily gzipped CSVs from the public OpenAQ S3 archive

# COMMAND ----------

import os, time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
session = requests.Session()

def list_keys(location_id):
    prefix = f"records/csv.gz/locationid={location_id}/year={YEAR}/"
    keys, token = [], None
    while True:
        params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            params["continuation-token"] = token
        r = session.get(ARCHIVE_URL, params=params, timeout=60); r.raise_for_status()
        root = ET.fromstring(r.text)
        keys += [c.find("s3:Key", S3_NS).text for c in root.findall("s3:Contents", S3_NS)]
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            return keys
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)

def download(key):
    dest = f"{RAW}/{key}"
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return "skipped"
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    for attempt in range(3):
        try:
            r = session.get(f"{ARCHIVE_URL}/{key}", timeout=60)
            if r.status_code == 200:
                with open(dest, "wb") as f:
                    f.write(r.content)
                return "downloaded"
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    return "failed"

t0 = time.time()
keys = []
for s in STATIONS:
    k = list_keys(s["location_id"]); keys += k
    print(f"station {s['location_id']} ({s['city']}): {len(k)} files")
counts = {"skipped": 0, "downloaded": 0, "failed": 0}
with ThreadPoolExecutor(max_workers=16) as pool:
    for fut in as_completed(pool.submit(download, k) for k in keys):
        counts[fut.result()] += 1
print(counts, f"in {time.time() - t0:.0f}s")

# COMMAND ----------

# MAGIC %md ## 2. Load: every raw file in one Spark call -> bronze Delta table (untouched)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

SCHEMA_RAW = StructType([
    StructField("location_id", IntegerType()), StructField("sensors_id", IntegerType()),
    StructField("location", StringType()),     StructField("datetime", StringType()),
    StructField("lat", DoubleType()),          StructField("lon", DoubleType()),
    StructField("parameter", StringType()),    StructField("units", StringType()),
    StructField("value", DoubleType()),
])

bronze = (spark.read.csv(f"{RAW}/records/csv.gz", header=True, schema=SCHEMA_RAW)
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("loaded_at", F.current_timestamp()))
bronze.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_readings")
bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_readings")
print(f"bronze_readings: {bronze.count():,} rows from {bronze.select('source_file').distinct().count():,} files")

# COMMAND ----------

# MAGIC %md ## 3. Transform: bronze -> silver (clean) -> gold (metrics)

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
stations = spark.createDataFrame(STATIONS).select("location_id", F.col("name").alias("station_name"), "city")

hourly = (bronze
          .filter(F.col("parameter") == "pm25")
          .withColumn("ts", F.to_timestamp("datetime"))
          .withColumn("date", F.to_date("ts"))
          .filter(F.year("date") == YEAR)
          .filter(F.col("value").between(0, 1000))
          .dropDuplicates(["location_id", "sensors_id", "ts"])
          .join(stations, "location_id", "inner")
          .withColumn("hour", F.date_trunc("hour", "ts"))                 # 15-min readings -> hourly
          .groupBy("location_id", "station_name", "city", "date", "hour")
          .agg(F.round(F.avg("value"), 2).alias("value"), F.count("*").alias("n_readings")))
hourly.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_hourly_pm25")

daily_station = (spark.table(f"{CATALOG}.{SCHEMA}.silver_hourly_pm25")
                 .groupBy("location_id", "station_name", "city", "date")
                 .agg(F.round(F.avg("value"), 1).alias("pm25_avg"),
                      F.count("*").alias("hours_reported"),
                      F.round(F.stddev("value"), 2).alias("pm25_std"))
                 .withColumn("is_complete", (F.col("hours_reported") >= MIN_HOURS_PER_DAY) & (F.col("pm25_std") > 0)))
daily_station.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_daily_station")

daily_city = (daily_station.filter("is_complete")
              .groupBy("city", "date")
              .agg(F.round(F.avg("pm25_avg"), 1).alias("pm25_avg"), F.countDistinct("location_id").alias("n_stations"))
              .withColumn("above_who", F.col("pm25_avg") > WHO_PM25_DAILY_LIMIT))
daily_city.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_daily_city")

city_monthly = (daily_city.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
                .groupBy("city", "year", "month")
                .agg(F.count("*").alias("days_measured"),
                     F.sum(F.col("above_who").cast("int")).alias("days_above_who"),
                     F.round(F.avg("pm25_avg"), 1).alias("pm25_month_avg"),
                     F.max("pm25_avg").alias("pm25_month_max"))
                .withColumn("pct_days_above_who", F.round(100 * F.col("days_above_who") / F.col("days_measured"), 1)))
city_monthly.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_city_monthly")

coverage = (daily_station.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
            .groupBy("location_id", "station_name", "city", "year", "month")
            .agg(F.count("*").alias("days_with_data"), F.sum(F.col("is_complete").cast("int")).alias("complete_days")))
coverage.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_station_coverage")

for t in ["silver_hourly_pm25", "silver_daily_station", "silver_daily_city", "gold_city_monthly", "gold_station_coverage"]:
    print(f"{t}: {spark.table(f'{CATALOG}.{SCHEMA}.{t}').count():,} rows")

# COMMAND ----------

# MAGIC %md ## 4. Data-quality checks (pipeline should stop if any fails)

# COMMAND ----------

CHECKS = {
    "no complete station-day has < 18 hours":
        f"select count(*) from {CATALOG}.{SCHEMA}.silver_daily_station where is_complete and hours_reported < {MIN_HOURS_PER_DAY}",
    "no complete station-day has zero variance (stuck sensor)":
        f"select count(*) from {CATALOG}.{SCHEMA}.silver_daily_station where is_complete and (pm25_std is null or pm25_std = 0)",
    "daily city means within 0..1000":
        f"select count(*) from {CATALOG}.{SCHEMA}.silver_daily_city where pm25_avg < 0 or pm25_avg > 1000",
    "days_above_who <= days_measured":
        f"select count(*) from {CATALOG}.{SCHEMA}.gold_city_monthly where days_above_who > days_measured",
    "monthly days reconcile with daily rows":
        f"select abs((select sum(days_measured) from {CATALOG}.{SCHEMA}.gold_city_monthly) - (select count(*) from {CATALOG}.{SCHEMA}.silver_daily_city))",
    "no duplicate (city, year, month)":
        f"select count(*) - count(distinct concat(city, year, month)) from {CATALOG}.{SCHEMA}.gold_city_monthly",
}
failed = 0
for name, sql in CHECKS.items():
    bad = spark.sql(sql).collect()[0][0]
    failed += bad != 0
    print(("PASS " if bad == 0 else f"FAIL ({bad}) "), name)
assert failed == 0, f"{failed} data-quality checks failed"
print(f"{len(CHECKS)}/{len(CHECKS)} checks passed")

# COMMAND ----------

# MAGIC %md ## 5. Analytics in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which cities had the most days above the WHO PM2.5 limit in winter (Nov-Feb)?
# MAGIC select city,
# MAGIC        sum(days_measured)  as days_measured,
# MAGIC        sum(days_above_who) as days_above_who,
# MAGIC        round(100.0 * sum(days_above_who) / sum(days_measured), 1) as pct_days_above_who,
# MAGIC        round(avg(pm25_month_avg), 1) as avg_pm25
# MAGIC from workspace.default.gold_city_monthly
# MAGIC where month in (11, 12, 1, 2)
# MAGIC group by city
# MAGIC order by pct_days_above_who desc, avg_pm25 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleanest month per city (window function)
# MAGIC with ranked as (
# MAGIC   select city, month, pm25_month_avg, days_above_who,
# MAGIC          row_number() over (partition by city order by pm25_month_avg) as rn
# MAGIC   from workspace.default.gold_city_monthly where days_measured >= 15)
# MAGIC select city, month, pm25_month_avg, days_above_who from ranked where rn = 1 order by pm25_month_avg

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Station coverage gaps (data quality)
# MAGIC select city, station_name, month, days_with_data, complete_days
# MAGIC from workspace.default.gold_station_coverage
# MAGIC where complete_days < 20 order by complete_days limit 20
