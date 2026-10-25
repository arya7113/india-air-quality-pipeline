"""Run every sql/*.sql file against the SQLite warehouse and print the results as tables."""
import sqlite3
from pathlib import Path

import pandas as pd

from config import ROOT, SQLITE_DB


def main():
    conn = sqlite3.connect(SQLITE_DB)
    for path in sorted((ROOT / "sql").glob("*.sql")):
        sql = path.read_text()
        title = sql.splitlines()[0].lstrip("- ").strip()
        print(f"\n== {path.name}: {title}")
        print(pd.read_sql_query(sql, conn).to_string(index=False))
    conn.close()


if __name__ == "__main__":
    main()
