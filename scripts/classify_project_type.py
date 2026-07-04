import sqlite3
from pathlib import Path
from collections import Counter
 
from config import DB_PATH
from file_policy import QDA_EXTENSIONS, get_file_extension
 
# Narrower than file_policy.DOCUMENT_EXTENSIONS: only true "primary data"
# transcript/article formats per the project description (p.13).
PRIMARY_DATA_EXTENSIONS = {".txt", ".pdf", ".rtf", ".doc", ".docx"}
 
# Anything else we can still positively identify as "valid data" for
# OTHER_PROJECT, i.e. not garbage/unknown.
OTHER_VALID_EXTENSIONS = {
    ".csv", ".xls", ".xlsx", ".ods", ".tsv",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".mp3", ".wav", ".m4a", ".flac", ".aac",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".mpeg", ".mpg",
    ".zip", ".sav", ".dta", ".sas7bdat", ".sas", ".html",
}
 
 
def ensure_type_column(cur, con):
    cur.execute("PRAGMA table_info(projects)")
    cols = [row[1] for row in cur.fetchall()]
    if "type" not in cols:
        cur.execute("ALTER TABLE projects ADD COLUMN type TEXT")
        con.commit()
        print("Added 'type' column to projects table.")
 
 
def classify_project(extensions: list[str]) -> str:
    ext_set = set(extensions)
    if ext_set & QDA_EXTENSIONS:
        return "QDA_PROJECT"
    if ext_set & PRIMARY_DATA_EXTENSIONS:
        return "QD_PROJECT"
    if ext_set & OTHER_VALID_EXTENSIONS:
        return "OTHER_PROJECT"
    return "NOT_A_PROJECT"
 
 
def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
 
    ensure_type_column(cur, con)
 
    cur.execute("SELECT id FROM projects")
    project_ids = [row[0] for row in cur.fetchall()]
 
    counts = Counter()
 
    for pid in project_ids:
        cur.execute("SELECT file_name, file_type FROM files WHERE project_id=?", (pid,))
        rows = cur.fetchall()
 
        extensions = []
        for file_name, file_type in rows:
            # prefer stored file_type, fall back to deriving from file_name
            ext = (file_type or get_file_extension(file_name or "")).lower()
            if ext and not ext.startswith("."):
                ext = "." + ext
            if ext:
                extensions.append(ext)
 
        ptype = classify_project(extensions)
        counts[ptype] += 1
 
        cur.execute("UPDATE projects SET type=? WHERE id=?", (ptype, pid))
 
    con.commit()
    con.close()
 
    print("\nProject type classification complete.")
    print(f"Total projects: {len(project_ids)}")
    for ptype in ["QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"]:
        print(f"  {ptype:15s}: {counts[ptype]}")
 
 
if __name__ == "__main__":
    main()
