import csv
import sqlite3
from pathlib import Path

DB_PATH = Path("data/metadata.db")
OUT_DIR = Path("data/csv")

def export_table(con, table, out_path):
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

def main():
    con = sqlite3.connect(DB_PATH)
    export_table(con, "datasets", OUT_DIR / "datasets.csv")
    export_table(con, "files", OUT_DIR / "files.csv")
    con.close()
    print("✅ Exported CSV to data/csv/")

if __name__ == "__main__":
    main()