import re
import sqlite3
import time
import random
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================================================
# CONFIG
# =========================================================
DB_PATH = Path("data/23206422-sq26.db")
SEED_FILE = Path("data/ada_seed_urls.txt")

REPOSITORY_ID = 7
REPOSITORY_URL = "https://dataverse.ada.edu.au/"
DOWNLOAD_REPOSITORY_FOLDER = "files/ada"

SLEEP_BETWEEN_URLS_MIN = 2.0
SLEEP_BETWEEN_URLS_MAX = 4.0
PAGE_WAIT_SECONDS = 4


# =========================================================
# HELPERS
# =========================================================
def now_utc():
    return datetime.now(timezone.utc).isoformat()


def normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def extract_persistent_id(url: str) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    return q.get("persistentId", [""])[0].strip()


def file_ext(name: str) -> str:
    return Path(name).suffix.lower()


def blocked_response(text: str) -> bool:
    text_low = text.lower()
    return (
        "request rejected" in text_low
        or "network security team" in text_low
        or "access denied" in text_low
        or "forbidden" in text_low
    )


def sleep_jitter():
    time.sleep(random.uniform(SLEEP_BETWEEN_URLS_MIN, SLEEP_BETWEEN_URLS_MAX))


# =========================================================
# DB HELPERS
# =========================================================
def project_exists(cur, project_url: str):
    cur.execute("""
        SELECT id FROM projects
        WHERE repository_id=? AND project_url=?
    """, (REPOSITORY_ID, project_url))
    row = cur.fetchone()
    return row[0] if row else None


def insert_project(cur, con, meta):
    cur.execute("""
        INSERT INTO projects (
            query_string,
            repository_id,
            repository_url,
            project_url,
            version,
            title,
            description,
            language,
            doi,
            upload_date,
            download_date,
            download_repository_folder,
            download_project_folder,
            download_version_folder,
            download_method
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "ada_seed_selenium",
        REPOSITORY_ID,
        REPOSITORY_URL,
        meta["project_url"],
        "",
        meta["title"],
        meta["description"],
        meta["language"],
        meta["doi"],
        meta["upload_date"],
        now_utc(),
        DOWNLOAD_REPOSITORY_FOLDER,
        DOWNLOAD_REPOSITORY_FOLDER,
        DOWNLOAD_REPOSITORY_FOLDER,
        "selenium-seed"
    ))
    con.commit()
    return cur.lastrowid


def update_project_metadata(cur, con, project_id, meta):
    cur.execute("""
        UPDATE projects
        SET title=?,
            description=?,
            language=?,
            doi=?,
            upload_date=?,
            download_date=?
        WHERE id=?
    """, (
        meta["title"],
        meta["description"],
        meta["language"],
        meta["doi"],
        meta["upload_date"],
        now_utc(),
        project_id
    ))
    con.commit()


def keyword_exists(cur, project_id: int, keyword: str):
    cur.execute("""
        SELECT id FROM keywords
        WHERE project_id=? AND keyword=?
    """, (project_id, keyword))
    return cur.fetchone() is not None


def insert_keyword(cur, con, project_id: int, keyword: str):
    keyword = normalize_text(keyword)
    if not keyword:
        return
    if keyword_exists(cur, project_id, keyword):
        return
    cur.execute("INSERT INTO keywords (project_id, keyword) VALUES (?, ?)", (project_id, keyword))
    con.commit()


def license_exists(cur, project_id: int, license_text: str):
    cur.execute("""
        SELECT id FROM licenses
        WHERE project_id=? AND license=?
    """, (project_id, license_text))
    return cur.fetchone() is not None


def insert_license(cur, con, project_id: int, license_text: str):
    license_text = normalize_text(license_text)
    if not license_text:
        return
    if license_exists(cur, project_id, license_text):
        return
    cur.execute("INSERT INTO licenses (project_id, license) VALUES (?, ?)", (project_id, license_text))
    con.commit()


def person_exists(cur, project_id: int, name: str, role: str):
    cur.execute("""
        SELECT id FROM person_role
        WHERE project_id=? AND name=? AND role=?
    """, (project_id, name, role))
    return cur.fetchone() is not None


def insert_person(cur, con, project_id: int, name: str, role: str):
    name = normalize_text(name)
    role = normalize_text(role) or "AUTHOR"
    if not name:
        return
    if person_exists(cur, project_id, name, role):
        return
    cur.execute("""
        INSERT INTO person_role (project_id, name, role)
        VALUES (?, ?, ?)
    """, (project_id, name, role))
    con.commit()


def file_exists(cur, project_id: int, file_name: str, local_path: str = ""):
    cur.execute("""
        SELECT id FROM files
        WHERE project_id=? AND file_name=? AND local_path=?
    """, (project_id, file_name, local_path or ""))
    row = cur.fetchone()
    return row[0] if row else None


def insert_file(cur, con, payload: dict):
    local_path = payload.get("local_path", "") or ""

    if file_exists(cur, payload["project_id"], payload["file_name"], local_path):
        return False

    cur.execute("""
        INSERT INTO files (
            project_id,
            file_name,
            file_type,
            file_url,
            size,
            local_path,
            status,
            status_note,
            downloaded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload["project_id"],
        payload["file_name"],
        payload.get("file_type", ""),
        payload.get("file_url", ""),
        payload.get("size", ""),
        local_path,
        payload.get("status", "METADATA_ONLY"),
        payload.get("status_note", ""),
        payload.get("downloaded_at", now_utc())
    ))
    con.commit()
    return True


def insert_blocked_placeholder(cur, con, project_id: int, reason: str):
    file_name = "__ADA_BLOCKED__"
    local_path = ""

    if file_exists(cur, project_id, file_name, local_path):
        return False

    cur.execute("""
        INSERT INTO files (
            project_id,
            file_name,
            file_type,
            file_url,
            size,
            local_path,
            status,
            status_note,
            downloaded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        project_id,
        file_name,
        "",
        "",
        "",
        local_path,
        "FAILED",
        reason,
        now_utc()
    ))
    con.commit()
    return True


# =========================================================
# SELENIUM
# =========================================================
def setup_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--no-sandbox")
    # options.add_argument("--headless=new")  # leave off for ADA

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def fetch_page_with_selenium(driver, url: str):
    try:
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(PAGE_WAIT_SECONDS)

        html = driver.page_source
        if blocked_response(html):
            return None, "blocked_by_server"

        return BeautifulSoup(html, "lxml"), None

    except Exception as e:
        return None, str(e)[:200]


# =========================================================
# EXTRACTION
# =========================================================
def extract_section_text(page_text: str, start_label: str, stop_labels):
    start_idx = page_text.lower().find(start_label.lower())
    if start_idx == -1:
        return ""

    start_idx += len(start_label)
    section = page_text[start_idx:]

    end_idx = len(section)
    for label in stop_labels:
        idx = section.lower().find(label.lower())
        if idx != -1 and idx < end_idx:
            end_idx = idx

    return normalize_text(section[:end_idx])


def extract_project_metadata(soup, url):
    page_text = soup.get_text("\n", strip=True)

    title = ""
    doi = ""
    description = ""
    language = ""
    upload_date = ""
    keywords = []
    people = []
    license_text = ""

    # title
    h1 = soup.find("h1")
    if h1:
        title = normalize_text(h1.get_text(" ", strip=True))

    if not title:
        m = re.search(r'\d{4},\s*"([^"]+)"', page_text)
        if m:
            title = normalize_text(m.group(1))

    if not title:
        title = extract_persistent_id(url) or url

    # doi
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "doi.org" in href:
            doi = href
            break

    if not doi:
        m = re.search(r"(https?://doi\.org/[A-Za-z0-9./_-]+)", page_text)
        if m:
            doi = m.group(1)

    # description
    description = extract_section_text(
        page_text,
        "Description",
        ["Subject", "Keyword", "Notes", "License/Data Use Agreement", "Files", "Metadata", "Terms", "Versions"]
    )

    # language
    m = re.search(r"Language\s+([A-Za-z]+)", page_text)
    if m:
        language = normalize_text(m.group(1))

    # upload date
    m = re.search(r"Publication Date\s+(\d{4}-\d{2}-\d{2})", page_text)
    if m:
        upload_date = m.group(1)

    # keywords
    raw_keywords = extract_section_text(
        page_text,
        "Keyword",
        ["Notes", "License/Data Use Agreement", "Files", "Metadata", "Terms", "Versions", "Topic Classification"]
    )
    keywords = [normalize_text(k) for k in re.split(r"[,\n;]+", raw_keywords) if normalize_text(k)]

    # license
    license_text = extract_section_text(
        page_text,
        "License/Data Use Agreement",
        ["Files", "Metadata", "Terms", "Versions", "Restricted Files + Terms of Access", "Guestbook"]
    )[:2000]

    # people
    m = re.search(r'^(.*?)\s*,\s*\d{4}\s*,\s*"', page_text, flags=re.DOTALL)
    if m:
        raw_people = normalize_text(m.group(1))
        if ";" in raw_people:
            people = [normalize_text(x) for x in raw_people.split(";") if normalize_text(x)]
        else:
            people = [raw_people]

    return {
        "project_url": url,
        "title": title,
        "doi": doi,
        "description": description,
        "language": language,
        "upload_date": upload_date,
        "keywords": keywords,
        "license": license_text,
        "people": people,
    }


def extract_file_rows(soup):
    results = []
    seen = set()

    rows = soup.select("table tbody tr")
    for row in rows:
        row_text = normalize_text(row.get_text(" ", strip=True))
        if not row_text:
            continue

        links = row.find_all("a", href=True)

        file_name = ""
        file_url = ""
        size = ""
        restricted = False

        if "restricted" in row_text.lower() or "request access" in row_text.lower():
            restricted = True

        for a in links:
            txt = normalize_text(a.get_text(" ", strip=True))
            href = a.get("href", "").strip()

            if txt and "." in txt and len(txt) < 250:
                file_name = txt

            if "/api/access/datafile/" in href:
                file_url = href if href.startswith("http") else "https://dataverse.ada.edu.au" + href

        m_size = re.search(r"(\d+(?:\.\d+)?)\s*(KB|MB|GB|B)", row_text, flags=re.IGNORECASE)
        if m_size:
            size = f"{m_size.group(1)} {m_size.group(2).upper()}"

        if file_name:
            key = (file_name, file_url, restricted)
            if key not in seen:
                results.append({
                    "file_name": file_name,
                    "file_url": file_url,
                    "size": size,
                    "status": "RESTRICTED" if restricted else "METADATA_ONLY",
                    "status_note": "visible_restricted_file_row" if restricted else "visible_file_row_extracted"
                })
                seen.add(key)

    return results


# =========================================================
# MAIN
# =========================================================
def main():
    print("ADA SELENIUM SCRIPT RUNNING")
    print(f"Using DB: {DB_PATH}")
    print(f"Using seed file: {SEED_FILE}")

    if not SEED_FILE.exists():
        print("❌ Seed file not found")
        return

    urls = [line.strip() for line in SEED_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    print(f"Total seed URLs: {len(urls)}")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    driver = setup_driver()

    inserted_projects = 0
    updated_projects = 0
    failed_projects = 0
    inserted_files = 0
    blocked_placeholders = 0

    try:
        for i, url in enumerate(urls, 1):
            print(f"\n=== [{i}/{len(urls)}] ===")
            print(url)

            existing_id = project_exists(cur, url)

            soup, err = fetch_page_with_selenium(driver, url)
            if err:
                print(f"❌ Fetch failed: {err}")

                if existing_id:
                    inserted = insert_blocked_placeholder(
                        cur,
                        con,
                        existing_id,
                        "blocked_by_server_before_file_extraction"
                    )
                    if inserted:
                        blocked_placeholders += 1

                failed_projects += 1
                sleep_jitter()
                continue

            meta = extract_project_metadata(soup, url)

            if existing_id:
                try:
                    update_project_metadata(cur, con, existing_id, meta)
                    project_id = existing_id
                    updated_projects += 1
                    print("🔄 Project metadata updated")
                except Exception as e:
                    print(f"❌ Update error: {e}")
                    failed_projects += 1
                    sleep_jitter()
                    continue
            else:
                try:
                    project_id = insert_project(cur, con, meta)
                    inserted_projects += 1
                    print("✅ Project inserted")
                except Exception as e:
                    print(f"❌ Insert error: {e}")
                    failed_projects += 1
                    sleep_jitter()
                    continue

            # metadata extras
            for kw in meta["keywords"]:
                insert_keyword(cur, con, project_id, kw)

            if meta["license"]:
                insert_license(cur, con, project_id, meta["license"])

            for person in meta["people"]:
                insert_person(cur, con, project_id, person, "AUTHOR")

            # visible file rows
            file_rows = extract_file_rows(soup)
            print(f"📄 Visible file rows found: {len(file_rows)}")

            for f in file_rows:
                inserted = insert_file(cur, con, {
                    "project_id": project_id,
                    "file_name": f["file_name"],
                    "file_type": file_ext(f["file_name"]),
                    "file_url": f["file_url"],
                    "size": f["size"],
                    "local_path": "",
                    "status": f["status"],
                    "status_note": f["status_note"],
                    "downloaded_at": now_utc(),
                })
                if inserted:
                    inserted_files += 1

            sleep_jitter()

    finally:
        driver.quit()
        con.close()

    print("\n=== DONE ===")
    print(f"Projects inserted: {inserted_projects}")
    print(f"Projects updated: {updated_projects}")
    print(f"Project fetch failures: {failed_projects}")
    print(f"File rows inserted: {inserted_files}")
    print(f"Blocked placeholder rows inserted: {blocked_placeholders}")


if __name__ == "__main__":
    main()