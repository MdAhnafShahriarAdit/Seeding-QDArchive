import sqlite3
from pathlib import Path

from config import DB_PATH, QDA_EXTENSIONS


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM projects")
    projects_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM files")
    files_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM keywords")
    keywords_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM licenses")
    licenses_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM person_role")
    people_count = cur.fetchone()[0]

    cur.execute("SELECT file_name, file_type, status, status_note FROM files")
    rows = cur.fetchall()

    qda_count = 0
    zip_count = 0
    zip_inner_qda_count = 0
    downloaded_count = 0
    pending_count = 0
    failed_count = 0
    restricted_count = 0
    metadata_only_count = 0

    for file_name, file_type, status, status_note in rows:
        file_name = file_name or ""
        ext = Path(file_name.lower()).suffix
        status = (status or "").upper()
        status_note = status_note or ""

        if ext in QDA_EXTENSIONS:
            qda_count += 1

        if ext == ".zip":
            zip_count += 1

        if "inside_zip:" in status_note and ext in QDA_EXTENSIONS:
            zip_inner_qda_count += 1

        if status == "DOWNLOADED":
            downloaded_count += 1
        elif status == "PENDING":
            pending_count += 1
        elif status == "FAILED":
            failed_count += 1
        elif status == "RESTRICTED":
            restricted_count += 1
        elif status == "METADATA_ONLY":
            metadata_only_count += 1

    print("Summary")
    print("-------")
    print(f"Projects in DB:             {projects_count}")
    print(f"Files in DB:                {files_count}")
    print(f"Keywords in DB:             {keywords_count}")
    print(f"Licenses in DB:             {licenses_count}")
    print(f"People/roles in DB:         {people_count}")
    print()
    print(f"QDA files:                  {qda_count}")
    print(f"ZIP files:                  {zip_count}")
    print(f"QDA files found inside ZIP: {zip_inner_qda_count}")
    print()
    print(f"Downloaded files:           {downloaded_count}")
    print(f"Pending files:              {pending_count}")
    print(f"Restricted files:           {restricted_count}")
    print(f"Metadata-only files:        {metadata_only_count}")
    print(f"Failed files:               {failed_count}")

    con.close()


if __name__ == "__main__":
    main()