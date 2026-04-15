import sqlite3
from pathlib import Path

DB_PATH = Path("data/metadata.db")


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query_string TEXT,
        repository_id INTEGER NOT NULL,
        repository_url TEXT NOT NULL,
        project_url TEXT NOT NULL,
        version TEXT,
        title TEXT NOT NULL,
        description TEXT,
        language TEXT,
        doi TEXT,
        upload_date TEXT,
        download_date TEXT NOT NULL,
        download_repository_folder TEXT NOT NULL,
        download_project_folder TEXT NOT NULL,
        download_version_folder TEXT,
        download_method TEXT NOT NULL DEFAULT 'API-CALL'
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        file_type TEXT,
        file_url TEXT,
        size INTEGER,
        local_path TEXT,
        status TEXT NOT NULL,
        status_note TEXT,
        downloaded_at TEXT,
        FOREIGN KEY(project_id) REFERENCES projects(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        FOREIGN KEY(project_id) REFERENCES projects(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        license TEXT NOT NULL,
        FOREIGN KEY(project_id) REFERENCES projects(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS person_role (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'UNKNOWN',
        FOREIGN KEY(project_id) REFERENCES projects(id)
    );
    """)

    con.commit()
    con.close()

    print(f"✅ Database created at: {DB_PATH}")


if __name__ == "__main__":
    main()