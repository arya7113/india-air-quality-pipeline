"""Single place for every setting the pipeline uses."""
from pathlib import Path

YEAR = 2025                     # which year of readings to download (archive has no CPCB data for 2024)
N_STATIONS = 50                 # how many Indian PM2.5 stations to track
WHO_PM25_DAILY_LIMIT = 15.0     # WHO 2021 guideline for 24-hour PM2.5 (µg/m³)
MIN_HOURS_PER_DAY = 18          # a day needs at least this many hourly readings to count

ROOT = Path(__file__).parent
DATA = ROOT / "data"
RAW = DATA / "raw"              # gzipped CSVs exactly as downloaded, one per station per day
BRONZE = DATA / "bronze"        # same rows, as Parquet, untouched
SILVER = DATA / "silver"        # cleaned hourly + daily tables
GOLD = DATA / "gold"            # city-level monthly metrics
STATIONS_FILE = DATA / "stations.json"
SQLITE_DB = DATA / "airquality.db"

ARCHIVE_URL = "https://openaq-data-archive.s3.amazonaws.com"
API_URL = "https://api.openaq.org/v3"
