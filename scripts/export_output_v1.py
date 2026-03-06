import csv
import sqlite3
from pathlib import Path

print("✅ export_output_v1.py started")

DB_PATH = Path("data/metadata.db")
OUT_PATH = Path("data/csv/output_v1.csv")

QDA_EXTS = {".qdpx", ".nvpx", ".atlproj", ".mx", ".mx24", ".mx20"}

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        SELECT
            f.file_url,
            f.downloaded_at,
            d.local_folder,
            f.file_name,
            d.source,
            d.license
        FROM files f
        JOIN datasets d ON d.id = f.dataset_id
    """)
    rows = cur.fetchall()
    con.close()

    out_rows = []
    for file_url, ts, local_folder, file_name, source, license_str in rows:
        if Path(str(file_name).lower()).suffix not in QDA_EXTS:
            continue

        out_rows.append({
            "qda_file_url": file_url or "",
            "last_download_timestamp": ts or "",
            "local_directory": (local_folder or "").replace("\\", "/"),
            "local_qda_filename": Path(str(file_name)).name,
            "repository": source or "",
            "license": license_str or "",
            "uploader_name": "",
            "uploader_email": "",
        })

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "qda_file_url",
            "last_download_timestamp",
            "local_directory",
            "local_qda_filename",
            "repository",
            "license",
            "uploader_name",
            "uploader_email",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"✅ Wrote: {OUT_PATH}")
    print(f"✅ QDA rows: {len(out_rows)}")

if __name__ == "__main__":
    main()