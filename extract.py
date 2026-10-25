"""
Layer 1: Extract.  Download raw daily files from the public OpenAQ S3 archive.

The archive stores one gzipped CSV per station per day:
  records/csv.gz/locationid=<id>/year=<yyyy>/month=<mm>/location-<id>-<yyyymmdd>.csv.gz

For every station in data/stations.json we list that station's keys for YEAR
and download each file into data/raw/ with the same folder layout. Files that
already exist are skipped, so re-running only fetches what is missing.
Nothing is parsed or cleaned here; the bytes land on disk exactly as served.
"""
import json
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from config import ARCHIVE_URL, RAW, STATIONS_FILE, YEAR

S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
session = requests.Session()


def list_keys(location_id):
    """Return every archive key for one station in YEAR, following S3 pagination."""
    prefix = f"records/csv.gz/locationid={location_id}/year={YEAR}/"
    keys, token = [], None
    while True:
        params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            params["continuation-token"] = token
        r = session.get(ARCHIVE_URL, params=params, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        keys += [c.find("s3:Key", S3_NS).text for c in root.findall("s3:Contents", S3_NS)]
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            return keys
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)


def download(key):
    """Download one key to RAW/<key>. Returns 'skipped' | 'downloaded' | 'failed'."""
    dest = RAW / key
    if dest.exists() and dest.stat().st_size > 0:
        return "skipped"
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(3):
        try:
            r = session.get(f"{ARCHIVE_URL}/{key}", timeout=60)
            if r.status_code == 200:
                dest.write_bytes(r.content)
                return "downloaded"
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    return "failed"


def main(station_ids=None):
    t0 = time.time()
    if station_ids is None:
        station_ids = [s["location_id"] for s in json.loads(STATIONS_FILE.read_text())]

    keys = []
    for sid in station_ids:
        k = list_keys(sid)
        keys += k
        print(f"[extract] station {sid}: {len(k)} daily files in {YEAR}")
    print(f"[extract] {len(keys)} files to check across {len(station_ids)} stations")

    counts = {"skipped": 0, "downloaded": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for fut in as_completed(pool.submit(download, k) for k in keys):
            counts[fut.result()] += 1
    print(f"[extract] {counts} in {time.time() - t0:.0f}s")
    if counts["downloaded"] + counts["skipped"] == 0:
        sys.exit("extract produced no files")


if __name__ == "__main__":
    main()
