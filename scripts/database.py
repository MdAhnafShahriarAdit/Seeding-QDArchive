import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from config import DB_PATH


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    con = connect_db()
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
        download_method TEXT NOT NULL DEFAULT 'OAI-PMH',
        UNIQUE(repository_id, project_url)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        file_type TEXT,
        file_url TEXT,
        size TEXT,
        local_path TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        status_note TEXT,
        downloaded_at TEXT,
        FOREIGN KEY(project_id) REFERENCES projects(id),
        UNIQUE(project_id, file_name, local_path)
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


def project_exists(cur, repository_id: int, project_url: str):
    cur.execute("""
        SELECT id FROM projects
        WHERE repository_id=? AND project_url=?
    """, (repository_id, project_url))
    row = cur.fetchone()
    return row[0] if row else None


def insert_project(cur, con, payload: dict):
    existing = project_exists(cur, payload["repository_id"], payload["project_url"])
    if existing:
        return existing, False

    cur.execute("""
        INSERT INTO projects
        (query_string, repository_id, repository_url, project_url, version, title, description,
         language, doi, upload_date, download_date, download_repository_folder,
         download_project_folder, download_version_folder, download_method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload["query_string"],
        payload["repository_id"],
        payload["repository_url"],
        payload["project_url"],
        payload.get("version", ""),
        payload["title"],
        payload.get("description", ""),
        payload.get("language", ""),
        payload.get("doi", ""),
        payload.get("upload_date", ""),
        payload.get("download_date", now_utc()),
        payload["download_repository_folder"],
        payload["download_project_folder"],
        payload.get("download_version_folder", payload["download_project_folder"]),
        payload.get("download_method", "OAI-PMH"),
    ))
    con.commit()
    return cur.lastrowid, True


def insert_keyword(cur, con, project_id: int, keyword: str):
    if not keyword:
        return
    cur.execute("INSERT INTO keywords (project_id, keyword) VALUES (?, ?)", (project_id, keyword))
    con.commit()


def insert_license(cur, con, project_id: int, license_text: str):
    if not license_text:
        return
    cur.execute("INSERT INTO licenses (project_id, license) VALUES (?, ?)", (project_id, license_text))
    con.commit()


def insert_person(cur, con, project_id: int, name: str, role: str):
    if not name:
        return
    cur.execute(
        "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
        (project_id, name, role or "UNKNOWN"),
    )
    con.commit()


def file_exists(cur, project_id: int, file_name: str, local_path: str = ""):
    local_path = local_path or ""
    cur.execute("""
        SELECT id FROM files
        WHERE project_id=? AND file_name=? AND local_path=?
    """, (project_id, file_name, local_path))
    row = cur.fetchone()
    return row[0] if row else None


def insert_file(cur, con, payload: dict):
    local_path = payload.get("local_path", "") or ""

    existing = file_exists(
        cur,
        payload["project_id"],
        payload["file_name"],
        local_path
    )
    if existing:
        return existing, False

    cur.execute("""
        INSERT INTO files
        (project_id, file_name, file_type, file_url, size, local_path, status, status_note, downloaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload["project_id"],
        payload["file_name"],
        payload.get("file_type", ""),
        payload.get("file_url", ""),
        payload.get("size", ""),
        local_path,
        payload.get("status", "PENDING"),
        payload.get("status_note", ""),
        payload.get("downloaded_at", now_utc()),
    ))
    con.commit()
    return cur.lastrowid, True


def update_file_status(cur, con, project_id: int, file_name: str, local_path: str, status: str, status_note: str):
    local_path = local_path or ""
    cur.execute("""
        UPDATE files
        SET status=?, status_note=?, downloaded_at=?
        WHERE project_id=? AND file_name=? AND local_path=?
    """, (
        status,
        status_note[:500],
        now_utc(),
        project_id,
        file_name,
        local_path
    ))
    con.commit()