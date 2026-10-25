# India Air Quality ELT Pipeline

PySpark + SQL pipeline that turns one year of public Indian air-quality readings
(50 CPCB stations, 25 cities, ~15k daily files) into clean, city-level PM2.5 metrics,
including **days per month above the WHO limit**.

**The problem.** OpenAQ publishes India's CPCB sensor data as one gzipped CSV per
station per day. Answering "which city had the most unhealthy days last winter"
means downloading thousands of files and reconciling 15-minute readings, time
zones, stuck sensors and missing weeks. After this pipeline it is one SQL query.

## Architecture

```
 OpenAQ API ──► discover_stations.py ──► data/stations.json   (50 stations, 25 cities)
                                                 │
 OpenAQ S3 archive ──► extract.py ──► data/raw/  │  14,806 gzipped CSVs, untouched, idempotent
   (public, no key)                      │       │
                                         ▼       ▼
                     load.py  ──►  bronze/readings.parquet     10.7M rows, all parameters, as received
                   (PySpark)             │
                                         ▼
                 transform.py ──►  silver/hourly_pm25           323k rows  15-min readings → hourly
                   (PySpark)       silver/daily_station         14.8k rows daily mean + completeness flag
                                   silver/daily_city            7.2k rows  mean of complete station-days
                                   gold/city_monthly            271 rows   days above WHO limit, avg, max
                                   gold/station_coverage        537 rows   data-quality: gaps per station
                                         │
                                         ▼
             quality_checks.py ──►  8 SQL checks, pipeline fails if any fails
                    sql/*.sql  ──►  analytics on data/airquality.db (SQLite)
```

Each layer is one file with one job. Raw and bronze are never edited; every
silver/gold table is rebuilt from bronze on each run. If a transform has a bug,
fix the code and re-run: no re-download.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # needs Java 17 on PATH (PySpark runs on the JVM)
cp .env.example .env                       # add your free key from explore.openaq.org

python discover_stations.py                # pick 50 stations (only step that uses the API)
python pipeline.py                         # extract → load → transform → quality checks
python run_sql.py                          # print the analytics queries
```

Full run on a laptop: ~6 min download, ~2 min load, ~30 s transform.
Running `pipeline.py` again downloads nothing and produces identical tables.

## Results (Feb 19 – Dec 31, 2025)

**Which cities had the most days above the WHO PM2.5 limit (15 µg/m³) in winter (Nov–Feb)?**
`sql/01_worst_winter_cities.sql`

```
     city  days_measured  days_above_who  pct_days_above_who  avg_pm25
    Delhi             70              70               100.0     191.6
Ghaziabad             68              68               100.0     155.3
 Gurugram             70              70               100.0     122.9
   Howrah             69              69               100.0     102.2
  Lucknow             69              69               100.0      94.3
  Kolkata             69              69               100.0      86.8
    Patna             68              68               100.0      81.2
  Asansol             69              69               100.0      78.1
     Agra             68              68               100.0      71.2
  Jodhpur             69              69               100.0      69.9
```

**The cities this job is in, plus the metros, full period:**

| City | Days measured | Days above WHO limit | % | Avg PM2.5 (µg/m³) |
|---|---|---|---|---|
| Delhi | 299 | 299 | 100.0 | 98.7 |
| Kolkata | 299 | 298 | 99.7 | 51.3 |
| Bengaluru | 280 | 273 | 97.5 | 24.5 |
| Hyderabad | 289 | 266 | 92.0 | 40.7 |
| Chennai | 297 | 258 | 86.9 | 27.6 |
| Pune | 299 | 249 | 83.3 | 38.8 |
| Mumbai | 257 | 199 | 77.4 | 46.1 |

**One finding.** Across all 25 cities, 91.2% of measured
city-days were above the WHO daily guideline. Bengaluru, usually considered a
clean city, still exceeded it on 97% of days; its PM2.5 is low by Indian
standards (avg 24 µg/m³) but not by WHO standards.

## Data-quality decisions (the interesting part)

| Problem found in the data | What the pipeline does |
|---|---|
| CPCB sensors report every **15 minutes**, not hourly (≈90 readings/day) | Roll up to hourly means first, so "hours reported" really counts hours |
| Archive files are cut by **UTC day**, readings are **IST** (+05:30) | Parse the offset, set Spark session timezone to Asia/Kolkata, bucket by IST date, filter to the target year |
| **Stuck sensors**: Colaba (Mumbai) reported a flat 985.0 for whole days | A day with zero variance across its hourly values is marked incomplete and excluded (342 station-days dropped) |
| **Missing weeks**: e.g. Sector-51 Gurugram had 1 complete day in April | 18-of-24-hour completeness rule; `station_coverage` table makes gaps visible instead of hiding them |
| Negative and absurd values (>1000 µg/m³) | Filtered in silver |
| Duplicate readings for the same sensor and instant | `dropDuplicates` on (station, sensor, timestamp) |
| API lists stations as active but the archive has **no 2024 CPCB files** | Discovery verifies in S3 that files exist for the target year before choosing a station |
| City averages dominated by one bad station | City value = mean of station means from complete days only |

Known limitations: February 2025 is partial (archive starts Feb 19); January is
missing for CPCB stations in the archive; a station that is stuck for part of a
day still passes the variance rule (see Mumbai 2025-12-16).

## Data model

- **bronze** (raw as received): `readings` partitioned by `locationid` and `month`.
- **silver** (normalised): `hourly_pm25` fact keyed by station + hour; `stations` dimension (id, name, city, lat, lon) joined on `location_id`.
- **gold** (denormalised for reads): `city_monthly` carries city, year, month and every pre-computed measure in one row, so queries need no joins or aggregation.

Why Parquet: columnar, compressed (97 MB raw gz → fast partitioned reads), keeps types.
Why SQLite on top: the gold tables are tiny; plain SQL with window functions is the easiest interface.

## Also runs on Databricks

`databricks/airquality_pipeline.py` is the same pipeline as a Databricks notebook
(Free Edition, serverless): files land in a Unity Catalog volume, each layer is a
Delta table (`bronze_readings`, `silver_*`, `gold_*`), and the analytics run as
`%sql` cells. Import it via Workspace > your folder > Import and Run all. Verified: 10 stations, 6/6 quality checks pass, ~3 min on serverless.

## Layout

```
config.py              settings: year, station count, WHO limit, completeness threshold, paths
discover_stations.py   OpenAQ API → data/stations.json          (needs OPENAQ_API_KEY)
extract.py             S3 archive → data/raw/ (list, download, skip existing, retry)
load.py                raw CSV.gz → bronze Parquet, one spark.read call
transform.py           bronze → silver → gold, all PySpark
quality_checks.py      8 SQL checks, non-zero exit on failure
pipeline.py            runs the four stages in order
run_sql.py             runs every sql/*.sql and prints the results
sql/                   01 worst winter cities · 02 cleanest month per city (ROW_NUMBER)
                       03 month-over-month trend (LAG) · 04 station gaps · 05 worst single days
spark_session.py       one SparkSession config for every layer
databricks/            the same pipeline as a Databricks notebook (Delta tables)
```

Data: [OpenAQ](https://openaq.org) (CC BY 4.0), readings originally from CPCB and state pollution control boards.
