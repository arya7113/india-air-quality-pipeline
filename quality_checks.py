"""
Data quality checks, run after Transform. Each check prints PASS/FAIL; the script
exits non-zero if any check fails so pipeline.py stops before publishing bad data.
"""
import sqlite3
import sys

from config import MIN_HOURS_PER_DAY, SQLITE_DB

CHECKS = {
    # 1. every daily_city row must come from a city we track
    "every city in daily_city exists in stations":
        "select count(*) from daily_city d left join stations s using(city) where s.city is null",
    # 2. a daily mean is only trusted from a complete day
    f"no complete station-day has fewer than {MIN_HOURS_PER_DAY} hours":
        f"select count(*) from daily_station where is_complete = 1 and hours_reported < {MIN_HOURS_PER_DAY}",
    # 3. physical range: PM2.5 can't be negative, and the cap in transform is 1000
    "all daily PM2.5 means are between 0 and 1000":
        "select count(*) from daily_city where pm25_avg < 0 or pm25_avg > 1000",
    # 4. a month can't have more measured days than calendar days
    "days_measured never exceeds 31":
        "select count(*) from city_monthly where days_measured > 31",
    # 5. the metric cannot exceed its own denominator
    "days_above_who <= days_measured":
        "select count(*) from city_monthly where days_above_who > days_measured",
    # 6. city_monthly must reconcile with daily_city (no rows lost in aggregation)
    "sum of days_measured equals row count of daily_city":
        "select abs((select sum(days_measured) from city_monthly) - (select count(*) from daily_city))",
    # 7. flatline (stuck sensor) days must never be marked complete
    "no complete station-day has zero variance (stuck sensor)":
        "select count(*) from daily_station where is_complete = 1 and (pm25_std is null or pm25_std = 0)",
    # 8. one row per city per month, no duplicates in the gold table
    "city_monthly has no duplicate (city, year, month)":
        "select count(*) - count(distinct city || year || month) from city_monthly",
}


def main():
    conn = sqlite3.connect(SQLITE_DB)
    failed = 0
    for name, sql in CHECKS.items():
        bad = conn.execute(sql).fetchone()[0]
        status = "PASS" if bad == 0 else f"FAIL ({bad} rows)"
        failed += bad != 0
        print(f"[dq] {status:14} {name}")
    conn.close()
    print(f"[dq] {len(CHECKS) - failed}/{len(CHECKS)} checks passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
