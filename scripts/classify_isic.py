"""
Part 2, Step 2 & 3: Classify projects (and their primary data files) into
ISIC Rev. 5 divisions.

What it does:
1. Adds primary_class / secondary_class columns to `projects`.
2. Creates a `file_classifications` table for per-file classification.
3. Scores each project's text (title + description + non-ddc keywords)
   against the ISIC division lexicon (isic_data.py), with a strong bonus
   for any ddc: keyword that maps to a division (ddc_mapping.py).
4. For QDA_PROJECT / QD_PROJECT projects, also classifies each primary
   data file individually:
   - if the file exists locally (using `local_path`), extracts text from
     it (pdf/txt/docx/rtf) and classifies that text
   - if the file isn't available on this machine, falls back to the
     project-level classification (noted as such)

Run from the repo root:
    python3 scripts/classify_isic.py

Safe to re-run -- it recomputes and overwrites classifications each time.

Optional dependencies for file-level text extraction (only needed if you
want to classify file content rather than fall back to project-level):
    pip install pypdf python-docx striprtf
"""

import re
import sqlite3
from collections import Counter
from pathlib import Path

from config import DB_PATH
from isic_data import DIVISIONS
from ddc_mapping import ddc_to_isic
from file_policy import QDA_EXTENSIONS, get_file_extension
from classify_project_type import PRIMARY_DATA_EXTENSIONS

DDC_BONUS_WEIGHT = 5  # how much more a ddc-code match counts vs a plain keyword hit
MIN_SCORE_FOR_CLASS = 1  # below this, a class is not confident enough to report


# ---------------------------------------------------------------------------
# Text scoring
# ---------------------------------------------------------------------------

def _word_hits(text: str, keyword: str) -> int:
    """Count occurrences of keyword (word-boundary, case-insensitive) in text."""
    pattern = r"\b" + re.escape(keyword) + r"\b"
    return len(re.findall(pattern, text, flags=re.IGNORECASE))


def score_text_against_divisions(text: str, ddc_codes: list[str]) -> Counter:
    text = text or ""
    scores = Counter()

    for code, info in DIVISIONS.items():
        for kw in info["keywords"]:
            hits = _word_hits(text, kw)
            if hits:
                scores[code] += hits

    for ddc in ddc_codes:
        mapped = ddc_to_isic(ddc)
        if mapped:
            scores[mapped] += DDC_BONUS_WEIGHT

    return scores


def top_two_classes(scores: Counter) -> tuple[str | None, str | None]:
    ranked = [code for code, s in scores.most_common() if s >= MIN_SCORE_FOR_CLASS]
    primary = ranked[0] if len(ranked) > 0 else None
    secondary = ranked[1] if len(ranked) > 1 else None
    return primary, secondary


# ---------------------------------------------------------------------------
# File content extraction (best-effort, local machine only)
# ---------------------------------------------------------------------------

def extract_text_from_file(local_path: str, max_chars: int = 8000) -> tuple[str | None, str]:
    """Returns (text_or_None, reason). reason is one of:
    'ok', 'no_path', 'file_not_found', 'unsupported_ext',
    'lib_missing:<pkg>', 'empty_text', 'read_error:<msg>'
    """
    if not local_path:
        return None, "no_path"

    candidates = [Path(local_path), Path(str(local_path).replace("\\", "/"))]
    path = None
    for candidate in candidates:
        try:
            if candidate.exists():
                path = candidate
                break
        except OSError:
            continue  # e.g. "file name too long" when backslashes aren't separators on this OS
    if path is None:
        return None, "file_not_found"

    ext = path.suffix.lower()
    try:
        if ext == ".txt":
            text = path.read_text(errors="ignore")[:max_chars]
            return (text, "ok") if text.strip() else (None, "empty_text")

        if ext == ".pdf":
            try:
                from pypdf import PdfReader
            except ImportError:
                return None, "lib_missing:pypdf"
            reader = PdfReader(str(path))
            text = ""
            for page in reader.pages[:15]:  # cap pages for speed
                text += page.extract_text() or ""
                if len(text) >= max_chars:
                    break
            text = text[:max_chars]
            return (text, "ok") if text.strip() else (None, "empty_text")

        if ext == ".docx":
            try:
                import docx
            except ImportError:
                return None, "lib_missing:python-docx"
            d = docx.Document(str(path))
            text = "\n".join(p.text for p in d.paragraphs)[:max_chars]
            return (text, "ok") if text.strip() else (None, "empty_text")

        if ext == ".rtf":
            try:
                from striprtf.striprtf import rtf_to_text
            except ImportError:
                return None, "lib_missing:striprtf"
            raw = path.read_text(errors="ignore")
            text = rtf_to_text(raw)[:max_chars]
            return (text, "ok") if text.strip() else (None, "empty_text")

        return None, "unsupported_ext"

    except Exception as e:
        return None, f"read_error:{type(e).__name__}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ensure_schema(cur, con):
    cur.execute("PRAGMA table_info(projects)")
    cols = [row[1] for row in cur.fetchall()]
    if "primary_class" not in cols:
        cur.execute("ALTER TABLE projects ADD COLUMN primary_class TEXT")
    if "secondary_class" not in cols:
        cur.execute("ALTER TABLE projects ADD COLUMN secondary_class TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS file_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            primary_class TEXT,
            secondary_class TEXT,
            classification_source TEXT NOT NULL DEFAULT 'project_fallback',
            FOREIGN KEY(file_id) REFERENCES files(id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        );
    """)
    con.commit()


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    ensure_schema(cur, con)

    cur.execute("SELECT id, title, description, type FROM projects")
    projects = cur.fetchall()

    project_class = {}  # project_id -> (primary, secondary)
    class_counts = Counter()

    for pid, title, description, ptype in projects:
        cur.execute("SELECT keyword FROM keywords WHERE project_id=?", (pid,))
        all_keywords = [k[0] for k in cur.fetchall()]
        ddc_codes = [k for k in all_keywords if k.lower().startswith("ddc:")]
        plain_keywords = [k for k in all_keywords if not k.lower().startswith("ddc:")]

        text = " ".join(filter(None, [title, description, " ".join(plain_keywords)]))
        scores = score_text_against_divisions(text, ddc_codes)
        primary, secondary = top_two_classes(scores)

        project_class[pid] = (primary, secondary)
        class_counts[primary or "UNCLASSIFIED"] += 1

        cur.execute(
            "UPDATE projects SET primary_class=?, secondary_class=? WHERE id=?",
            (primary, secondary, pid),
        )

    con.commit()

    # --- file-level classification for QDA_PROJECT / QD_PROJECT ---
    cur.execute("DELETE FROM file_classifications")  # re-runnable
    file_level_count = 0
    fallback_reasons = Counter()

    cur.execute("""
        SELECT f.id, f.project_id, f.file_name, f.file_type, f.local_path, p.type
        FROM files f JOIN projects p ON p.id = f.project_id
        WHERE p.type IN ('QDA_PROJECT', 'QD_PROJECT')
    """)
    all_files = cur.fetchall()

    primary_or_qda = QDA_EXTENSIONS | PRIMARY_DATA_EXTENSIONS
    target_files = []
    skipped_not_primary = 0
    for file_id, pid, file_name, file_type, local_path, ptype in all_files:
        ext = (file_type or get_file_extension(file_name or "")).lower()
        if ext and not ext.startswith("."):
            ext = "." + ext
        if ext in primary_or_qda:
            target_files.append((file_id, pid, local_path, ptype))
        else:
            skipped_not_primary += 1

    for file_id, pid, local_path, ptype in target_files:
        text, reason = extract_text_from_file(local_path)
        proj_primary, proj_secondary = project_class.get(pid, (None, None))

        if text:
            cur.execute("SELECT keyword FROM keywords WHERE project_id=?", (pid,))
            ddc_codes = [k[0] for k in cur.fetchall() if k[0].lower().startswith("ddc:")]
            scores = score_text_against_divisions(text, ddc_codes)
            primary, secondary = top_two_classes(scores)
            if primary is None:
                primary, secondary = proj_primary, proj_secondary
                source = "project_fallback"
                fallback_reasons["extracted_but_no_keyword_match"] += 1
            else:
                source = "file_content"
                file_level_count += 1
        else:
            primary, secondary = proj_primary, proj_secondary
            source = "project_fallback"
            fallback_reasons[reason] += 1

        cur.execute("""
            INSERT INTO file_classifications
            (file_id, project_id, primary_class, secondary_class, classification_source)
            VALUES (?, ?, ?, ?, ?)
        """, (file_id, pid, primary, secondary, source))

    con.commit()
    con.close()

    print("Project-level classification complete.")
    print(f"Total projects: {len(projects)}")
    for code, count in class_counts.most_common(15):
        title = DIVISIONS.get(code, {}).get("title", "")
        print(f"  {code:5s} {count:4d}  {title}")

    fallback_total = sum(fallback_reasons.values())
    print(f"\nFile-level classification (primary data / QDA files only): {len(target_files)} files")
    print(f"  (skipped {skipped_not_primary} non-primary secondary files in these projects)")
    print(f"  classified from file content: {file_level_count}")
    print(f"  fell back to project class:   {fallback_total}")
    if fallback_total:
        print("  fallback breakdown:")
        for reason, count in fallback_reasons.most_common():
            print(f"    {reason:35s} {count}")


if __name__ == "__main__":
    main()
