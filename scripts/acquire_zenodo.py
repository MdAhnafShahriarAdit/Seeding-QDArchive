import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import requests
from tqdm import tqdm

DB_PATH = Path("data/metadata.db")
ARCHIVE_DIR = Path("archive/zenodo")

QDA_EXTS = {".qdpx", ".nvpx", ".atlproj", ".mx", ".mx24", ".mx20"}

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def safe_name(s: str, max_len: int = 80) -> str:
    s = "".join(ch if ch.isalnum() or ch in "._- " else "" for ch in (s or ""))
    s = "_".join(s.split())
    return (s[:max_len] or "untitled").strip("_")

def has_license(rec: dict) -> bool:
    lic = rec.get("metadata", {}).get("license")
    if not lic:
        return False
    if isinstance(lic, dict):
        return bool(lic.get("id") or lic.get("title"))
    return True

def license_str(rec: dict) -> str:
    lic = rec.get("metadata", {}).get("license")
    if isinstance(lic, dict):
        return lic.get("id") or lic.get("title") or ""
    return str(lic)

def record_has_qda_file(rec: dict) -> bool:
    for f in rec.get("files", []) or []:
        key = f.get("key") or ""
        if Path(key.lower()).suffix in QDA_EXTS:
            return True
    return False

def download_file(url: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(out_path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, leave=False) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

def main():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    query = '(qdpx OR nvpx OR "MAXQDA" OR "ATLAS.ti" OR atlproj)'
    url = "https://zenodo.org/api/records"
    params = {"q": query, "size": 10, "page": 1}

    target_downloads = 5
    downloaded = 0

    while downloaded < target_downloads:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for rec in hits:
            if downloaded >= target_downloads:
                break

            if not has_license(rec):
                continue

            if not record_has_qda_file(rec):
                continue

            rec_id = str(rec.get("id"))
            title = rec.get("metadata", {}).get("title", "")
            pub = rec.get("metadata", {}).get("publication_date", "")
            lic = license_str(rec)
            source_url = rec.get("links", {}).get("html", "")
            t = now_utc()

            folder = ARCHIVE_DIR / f"{rec_id}_{safe_name(title)}"
            folder.mkdir(parents=True, exist_ok=True)

            cur.execute("""
                INSERT OR IGNORE INTO datasets
                (source, source_record_id, source_url, title, license, published, downloaded_at, local_folder, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ("zenodo", rec_id, source_url, title, lic, pub, t, str(folder), ""))

            cur.execute("SELECT id FROM datasets WHERE source=? AND source_record_id=?", ("zenodo", rec_id))
            dataset_db_id = cur.fetchone()[0]

            for f in rec.get("files", []) or []:
                key = f.get("key") or ""
                dl = (f.get("links", {}) or {}).get("download")
                size = f.get("size")
                if not key or not dl:
                    continue

                out_path = folder / key
                try:
                    download_file(dl, out_path)
                except Exception:
                    continue

                cur.execute("""
                    INSERT INTO files
                    (dataset_id, file_name, file_url, size, local_path, downloaded_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (dataset_db_id, key, dl, size, str(out_path), t))

            con.commit()
            downloaded += 1
            print(f"✅ Downloaded dataset {downloaded}/{target_downloads}: {rec_id} - {title}")

        params["page"] += 1

    con.close()
    print("✅ Done.")
    print(f"Archive: {ARCHIVE_DIR}")
    print(f"DB: {DB_PATH}")

if __name__ == "__main__":
    main()