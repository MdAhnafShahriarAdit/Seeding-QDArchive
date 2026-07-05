import sqlite3
from pathlib import Path

from isic_data import DIVISIONS

DB_PATH = Path("data/23206422-sq26.db")
QDA_EXTENSIONS = {".qdpx", ".qdc", ".nvp", ".nvpx", ".atlproj", ".mx", ".mx20", ".mx24"}


def count_projects(cur, repo_id):
    cur.execute("SELECT COUNT(*) FROM projects WHERE repository_id=?", (repo_id,))
    return cur.fetchone()[0]


def count_keywords(cur, repo_id):
    cur.execute("""
        SELECT COUNT(*)
        FROM keywords k
        JOIN projects p ON k.project_id = p.id
        WHERE p.repository_id=?
    """, (repo_id,))
    return cur.fetchone()[0]


def count_licenses(cur, repo_id):
    cur.execute("""
        SELECT COUNT(*)
        FROM licenses l
        JOIN projects p ON l.project_id = p.id
        WHERE p.repository_id=?
    """, (repo_id,))
    return cur.fetchone()[0]


def count_people(cur, repo_id):
    cur.execute("""
        SELECT COUNT(*)
        FROM person_role pr
        JOIN projects p ON pr.project_id = p.id
        WHERE p.repository_id=?
    """, (repo_id,))
    return cur.fetchone()[0]


def get_files_for_repo(cur, repo_id):
    cur.execute("""
        SELECT f.file_name, f.file_type, f.status, f.status_note
        FROM files f
        JOIN projects p ON f.project_id = p.id
        WHERE p.repository_id=?
    """, (repo_id,))
    return cur.fetchall()


def summarize_files(rows):
    total_files = len(rows)
    downloaded = 0
    pending = 0
    restricted = 0
    metadata_only = 0
    failed = 0
    qda_count = 0
    zip_count = 0
    qda_inside_zip_count = 0

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
            qda_inside_zip_count += 1

        if status == "DOWNLOADED":
            downloaded += 1
        elif status == "PENDING":
            pending += 1
        elif status == "RESTRICTED":
            restricted += 1
        elif status == "METADATA_ONLY":
            metadata_only += 1
        elif status == "FAILED":
            failed += 1

    return {
        "total_files": total_files,
        "downloaded": downloaded,
        "pending": pending,
        "restricted": restricted,
        "metadata_only": metadata_only,
        "failed": failed,
        "qda_count": qda_count,
        "zip_count": zip_count,
        "qda_inside_zip_count": qda_inside_zip_count,
    }


def class_label(code):
    if not code:
        return "UNCLASSIFIED (no confident match)"
    info = DIVISIONS.get(code)
    return f"{code} - {info['number']} - {info['title']}" if info else code


def print_classification_summary(cur, repo_id):
    """Prints stats using the EXACT field names from the Part 2 Google Form,
    so this section can be copied straight into the form for this repository."""

    cur.execute("SELECT COUNT(*) FROM projects WHERE repository_id=?", (repo_id,))
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT type, COUNT(*) FROM projects
        WHERE repository_id=? GROUP BY type
    """, (repo_id,))
    type_counts = dict(cur.fetchall())

    cur.execute("""
        SELECT primary_class, COUNT(*) c FROM projects
        WHERE repository_id=? GROUP BY primary_class ORDER BY c DESC LIMIT 1
    """, (repo_id,))
    row = cur.fetchone()
    dominant = class_label(row[0]) if row else "n/a"

    print("--- Google Form fields for this repository ---")
    print(f"Total No (number of) projects found:  {total}")
    print(f"No QDA_PROJECT found:                 {type_counts.get('QDA_PROJECT', 0)}")
    print(f"No QD_PROJECT found:                  {type_counts.get('QD_PROJECT', 0)}")
    print(f"No OTHER_PROJECT found:                {type_counts.get('OTHER_PROJECT', 0)}")
    print(f"No NOT_A_PROJECT found:                {type_counts.get('NOT_A_PROJECT', 0)}")
    print(f"Comment on projects found (optional):  (your own notes, if any)")
    print(f"Most common class (across all projects): {dominant}")
    print()


def print_repo_summary(cur, repo_id, repo_name):
    projects = count_projects(cur, repo_id)
    keywords = count_keywords(cur, repo_id)
    licenses = count_licenses(cur, repo_id)
    people = count_people(cur, repo_id)
    file_rows = get_files_for_repo(cur, repo_id)
    files = summarize_files(file_rows)

    print(f"{repo_name}")
    print("-" * len(repo_name))
    print(f"Projects collected:           {projects}")
    print(f"Files discovered:            {files['total_files']}")
    print(f"Successfully downloaded:     {files['downloaded']}")
    print(f"Failed downloads:            {files['failed']}")
    print()
    print(f"Keywords extracted:          {keywords}")
    print(f"Licenses recorded:           {licenses}")
    print(f"People/roles identified:     {people}")
    print()
    print(f"Restricted files:            {files['restricted']}")
    print(f"Metadata-only files:         {files['metadata_only']}")
    print(f"Pending files:               {files['pending']}")
    print()
    print(f"QDA files:                   {files['qda_count']}")
    print(f"ZIP files:                   {files['zip_count']}")
    print(f"QDA files found inside ZIP:  {files['qda_inside_zip_count']}")
    print()
    print_classification_summary(cur, repo_id)


def print_overall_summary(cur):
    cur.execute("SELECT COUNT(*) FROM projects")
    projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM files")
    files = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM keywords")
    keywords = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM licenses")
    licenses = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM person_role")
    people = cur.fetchone()[0]

    print("Overall Database Summary")
    print("------------------------")
    print(f"Total projects:             {projects}")
    print(f"Total files:                {files}")
    print(f"Total keywords:             {keywords}")
    print(f"Total licenses:             {licenses}")
    print(f"Total people/roles:         {people}")
    print()


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print_overall_summary(cur)

    print_repo_summary(cur, 16, "Repository 16 (uni-halle)")
    print_repo_summary(cur, 7, "Repository 7 (ADA Dataverse)")

    con.close()


if __name__ == "__main__":
    main()