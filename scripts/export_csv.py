import sqlite3
import csv
from pathlib import Path

from config import DB_PATH, CSV_ROOT


def export_table(cur, table_name: str, out_path: Path):
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    print(f"✅ Exported: {out_path}")


def main():
    CSV_ROOT.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    for table in ["projects", "files", "keywords", "licenses", "person_role"]:
        export_table(cur, table, CSV_ROOT / f"{table}.csv")

    con.close()
    print("✅ CSV export finished")


if __name__ == "__main__":
    main()