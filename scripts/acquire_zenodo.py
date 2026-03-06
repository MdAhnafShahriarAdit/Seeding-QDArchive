import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import requests
from tqdm import tqdm

DB_PATH = Path("data/metadata.db")
ARCHIVE_DIR = Path("archive/zenodo")

# QDA file extensions we care about
QDA_EXTS = {".qdpx", ".nvpx", ".atlproj", ".mx", ".mx24", ".mx20"}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(text: str, max_len: int = 80) -> str:
    """Make a Windows-safe folder name."""
    text = text or ""
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in "._- ":
            cleaned.append(ch)
    s = "".join(cleaned).strip()
    s = "_".join(s.split())
    return (s[:max_len] or "untitled").strip("_")


def has_license(rec: dict) -> bool:
    """No license => skip (as required)."""
    lic = rec.get("metadata", {}).get("license")
    if not lic:
        return False
    if isinstance(lic, dict):
        return bool(lic.get("id") or lic.get("title"))
    return True


def get_license_str(rec: dict) -> str:
    lic = rec.get("metadata", {}).get("license")
    if isinstance(lic, dict):
        return lic.get("id") or lic.get("title") or ""
    return str(lic)


def record_has_qda_file(rec: dict) -> bool:
    """Only keep records that actually contain a QDA file."""
    for f in rec.get("files", []) or []:
        name = f.get("key") or f.get("filename") or ""
        if Path(name.lower()).suffix in QDA_EXTS:
            return True
    return False


def download_file(url: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # If already downloaded, skip (resume-friendly)
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

    # Search query (simple but effective)
    query = '(qdpx OR nvpx OR "MAXQDA" OR "ATLAS.ti" OR atlproj)'
    api_url = "https://zenodo.org/api/records"

    # We want 5 datasets for now
    target_downloads = 5
    downloaded = 0

    page = 1
    size = 10  # fetch 10 per page, filter down to QDA+license

    while downloaded < target_downloads:
        params = {"q": query, "size": size, "page": page}
        resp = requests.get(api_url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            print("No more records found.")
            break

        for rec in hits:
            if downloaded >= target_downloads:
                break

            # 1) Must have license
            if not has_license(rec):
                continue

            # 2) Must contain QDA file
            if not record_has_qda_file(rec):
                continue

            rec_id = str(rec.get("id"))
            title = rec.get("metadata", {}).get("title", "")
            published = rec.get("metadata", {}).get("publication_date", "")
            license_str = get_license_str(rec)
            source_url = rec.get("links", {}).get("html") or ""
            t = now_utc()

            folder = ARCHIVE_DIR / f"{rec_id}_{safe_name(title)}"
            folder.mkdir(parents=True, exist_ok=True)

            # Insert dataset row
            cur.execute(
                """
                INSERT OR IGNORE INTO datasets
                (source, source_record_id, source_url, title, license, published, downloaded_at, local_folder, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("zenodo", rec_id, source_url, title, license_str, published, t, str(folder), "")
            )

            cur.execute(
                "SELECT id FROM datasets WHERE source=? AND source_record_id=?",
                ("zenodo", rec_id)
            )
            dataset_db_id = cur.fetchone()[0]

            files_list = rec.get("files", []) or []
            print(f"✅ Dataset {downloaded+1}/{target_downloads}: {rec_id} - {title}")
            print(f"   -> Zenodo lists {len(files_list)} file(s)")

            # Download + insert file rows
            files_added = 0
            for f in files_list:
                key = f.get("key") or f.get("filename") or ""
                links = f.get("links", {}) or {}

                # IMPORTANT FIX: sometimes Zenodo uses links.self instead of links.download
                dl = links.get("download") or links.get("self")

                size_bytes = f.get("size")

                if not key or not dl:
                    continue

                out_path = folder / key

                try:
                    download_file(dl, out_path)
                except Exception as e:
                    print(f"   ! Failed to download {key}: {e}")
                    continue

                cur.execute(
                    """
                    INSERT INTO files
                    (dataset_id, file_name, file_url, size, local_path, downloaded_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (dataset_db_id, key, dl, size_bytes, str(out_path), t)
                )
                files_added += 1

            con.commit()
            print(f"   -> Files saved in DB for this dataset: {files_added}")
            print("")

            downloaded += 1

        page += 1

    con.close()
    print("✅ Done.")
    print(f"Archive: {ARCHIVE_DIR}")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()