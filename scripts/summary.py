import sqlite3
from pathlib import Path

DB_PATH = Path("data/metadata.db")
QDA_EXTS = {".qdpx", ".nvpx", ".atlproj", ".mx", ".mx24", ".mx20"}

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM datasets")
    datasets_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM files")
    files_count = cur.fetchone()[0]

    cur.execute("SELECT file_name FROM files")
    qda_count = 0
    for (name,) in cur.fetchall():
        if Path(str(name).lower()).suffix in QDA_EXTS:
            qda_count += 1

    print("Summary")
    print("-------")
    print(f"Datasets in DB: {datasets_count}")
    print(f"Files in DB:    {files_count}")
    print(f"QDA files:      {qda_count}")

    con.close()

if __name__ == "__main__":
    main()