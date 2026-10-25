"""
Step 0: find Indian stations that measure PM2.5 and were active during YEAR.

Uses the OpenAQ v3 API (needs a free key in .env). Writes data/stations.json:
a list of {location_id, name, city, lat, lon}. This is the only step that
touches the API; everything after reads from the public S3 archive.
"""
import json
import os
import time
from collections import defaultdict

import requests
from dotenv import load_dotenv

from config import API_URL, ARCHIVE_URL, DATA, N_STATIONS, STATIONS_FILE, YEAR

load_dotenv()
HEADERS = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}


def fetch_all_india_locations():
    """Page through /locations for India (countries_id=9) until no more pages."""
    out, page = [], 1
    while True:
        for attempt in range(4):              # the API occasionally returns 5xx; back off and retry
            r = requests.get(f"{API_URL}/locations",
                             params={"countries_id": 9, "limit": 1000, "page": page},
                             headers=HEADERS, timeout=60)
            if r.status_code < 500:
                break
            time.sleep(2 ** attempt)
        r.raise_for_status()
        results = r.json()["results"]
        out.extend(results)
        if len(results) < 1000:
            return out
        page += 1


def is_good(loc):
    """Keep stations that have a pm25 sensor and were reporting across YEAR."""
    has_pm25 = any(s["parameter"]["name"] == "pm25" for s in loc.get("sensors", []))
    first = (loc.get("datetimeFirst") or {}).get("utc") or "9999"
    last = (loc.get("datetimeLast") or {}).get("utc") or "0000"
    return has_pm25 and first <= f"{YEAR}-03-01" and last >= f"{YEAR}-12-01"


CITY_ALIASES = {"New Delhi": "Delhi", "Bangalore": "Bengaluru", "Gurgaon": "Gurugram"}
PRIORITY_CITIES = ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Pune", "Chennai", "Kolkata"]
MAX_PER_CITY = 2


def in_archive(location_id):
    """The API says a station is active, but the S3 archive can still have gaps.
    Ask the archive for a single key under this station/year to be sure."""
    r = requests.get(ARCHIVE_URL, params={"list-type": "2", "max-keys": "1",
                     "prefix": f"records/csv.gz/locationid={location_id}/year={YEAR}/"}, timeout=60)
    return "<Key>" in r.text


def city_from_name(name):
    """CPCB names look like 'Anand Vihar, New Delhi - DPCC' -> 'Delhi'."""
    site_city = name.split(" - ")[0]          # drop the agency suffix
    city = site_city.split(",")[-1].strip()   # text after the last comma
    return CITY_ALIASES.get(city, city)


def main():
    DATA.mkdir(exist_ok=True)
    locs = fetch_all_india_locations()
    good = [l for l in locs if is_good(l) and l.get("provider", {}).get("name") == "CPCB"]
    print(f"{len(locs)} Indian locations, {len(good)} CPCB stations with pm25 active through {YEAR}")

    by_city = defaultdict(list)
    for l in good:
        by_city[city_from_name(l["name"])].append(l)

    # priority cities first, then the cities with the most stations, max 2 stations each
    order = PRIORITY_CITIES + sorted((c for c in by_city if c not in PRIORITY_CITIES),
                                     key=lambda c: -len(by_city[c]))
    chosen, skipped = [], 0
    for city in order:
        picked = 0
        for l in by_city.get(city, []):
            if picked == MAX_PER_CITY or len(chosen) >= N_STATIONS:
                break
            if in_archive(l["id"]):
                chosen.append(l)
                picked += 1
            else:
                skipped += 1
        if len(chosen) >= N_STATIONS:
            break
    print(f"skipped {skipped} stations the API lists as active but the archive has no {YEAR} files for")

    stations = [{"location_id": l["id"], "name": l["name"], "city": city_from_name(l["name"]),
                 "lat": l["coordinates"]["latitude"], "lon": l["coordinates"]["longitude"]}
                for l in chosen]
    STATIONS_FILE.write_text(json.dumps(stations, indent=2))
    cities = sorted({s["city"] for s in stations})
    print(f"wrote {len(stations)} stations across {len(cities)} cities -> {STATIONS_FILE}")
    print("cities:", ", ".join(cities))


if __name__ == "__main__":
    main()
