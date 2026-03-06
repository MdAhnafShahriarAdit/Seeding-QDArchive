import sqlite3
from pathlib import Path

DB_PATH = Path("data/metadata.db")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS datasets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        source_record_id TEXT NOT NULL,
        source_url TEXT,
        title TEXT,
        license TEXT,
        published TEXT,
        downloaded_at TEXT,
        local_folder TEXT,
        notes TEXT,
        UNIQUE(source, source_record_id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id INTEGER NOT NULL,
        file_name TEXT,
        file_url TEXT,
        size INTEGER,
        local_path TEXT,
        downloaded_at TEXT,
        FOREIGN KEY(dataset_id) REFERENCES datasets(id)
    );
    """)

    con.commit()
    con.close()

    print(f"✅ Database created at: {DB_PATH}")

if __name__ == "__main__":
    main()