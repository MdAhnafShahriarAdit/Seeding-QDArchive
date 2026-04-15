import os
import random
import re
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin, unquote
import requests
import xml.etree.ElementTree as ET

from config import (
    OAI_ENDPOINT_UNI_HALLE,
    REPOSITORY_ID_UNI_HALLE,
    REPOSITORY_URL_UNI_HALLE,
    USER_AGENT_OAI,
    USER_AGENT_WEB,
    REQUEST_TIMEOUT,
    DELAY_MIN,
    DELAY_MAX,
    FILES_ROOT,
    QDA_KEYWORDS,
    QDA_EXTENSIONS,
)
from database import (
    now_utc,
    insert_project,
    insert_keyword,
    insert_license,
    insert_person,
    insert_file,
    update_file_status,
)


NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


BITSTREAM_RE = re.compile(r'href="(/bitstream/[^"?]+)"')


def sleep_jitter():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def safe_folder_name(text: str) -> str:
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in "._- ":
            cleaned.append(ch)
    s = "".join(cleaned).strip()
    s = "_".join(s.split())
    return s.lower()[:120] if s else "project"


def file_ext(name: str) -> str:
    return Path(name).suffix.lower()


def is_qda_file(name: str) -> bool:
    return file_ext(name) in QDA_EXTENSIONS


def normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def safe_request(session: requests.Session, url: str, **kwargs):
    resp = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp


def is_blocked_html(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            content = f.read(2048).lower()
        return (
            b"<html" in content or
            b"access denied" in content or
            b"request rejected" in content or
            b"forbidden" in content
        )
    except Exception:
        return False


def is_valid_pdf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except Exception:
        return False


def is_valid_zip(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False


class UniHalleHarvester:
    def __init__(self):
        self.session_oai = requests.Session()
        self.session_oai.headers.update({"User-Agent": USER_AGENT_OAI})

        self.session_web = requests.Session()
        self.session_web.headers.update({"User-Agent": USER_AGENT_WEB})

        self.repository_folder = FILES_ROOT / "uni-halle"
        self.repository_folder.mkdir(parents=True, exist_ok=True)

    def is_qda_candidate(self, meta: dict) -> bool:
        hay = " ".join([
            meta.get("title", ""),
            meta.get("description", ""),
            " ".join(meta.get("subjects", [])),
            " ".join(meta.get("identifiers", [])),
        ]).lower()

        return any(k.lower() in hay for k in QDA_KEYWORDS)

    def _extract_project_url(self, identifiers):
        for ident in identifiers:
            if "opendata.uni-halle.de/handle/" in ident:
                return ident
        for ident in identifiers:
            if ident.startswith("http"):
                return ident
        return ""

    def _extract_doi(self, identifiers):
        for ident in identifiers:
            if "doi.org/" in ident.lower():
                return ident.split("doi.org/")[-1]
            if ident.lower().startswith("doi:"):
                return ident[4:]
        return ""

    def _parse_record(self, record_el):
        metadata_el = record_el.find("oai:metadata", NS)
        if metadata_el is None:
            return None

        title = ""
        descriptions = []
        creators = []
        subjects = []
        identifiers = []
        rights = []
        dates = []
        languages = []

        for dc_el in metadata_el.iter():
            tag = dc_el.tag.split("}")[-1]
            text = normalize_text(dc_el.text)

            if not text:
                continue

            if tag == "title":
                title = text
            elif tag == "description":
                descriptions.append(text)
            elif tag == "creator":
                creators.append(text)
            elif tag == "subject":
                subjects.append(text)
            elif tag == "identifier":
                identifiers.append(text)
            elif tag == "rights":
                rights.append(text)
            elif tag == "date":
                dates.append(text)
            elif tag == "language":
                languages.append(text)

        project_url = self._extract_project_url(identifiers)
        doi = self._extract_doi(identifiers)

        if not project_url:
            return None

        return {
            "title": title or project_url,
            "description": " | ".join(descriptions),
            "creators": creators,
            "subjects": subjects,
            "identifiers": identifiers,
            "rights": rights,
            "dates": dates,
            "language": languages[0] if languages else "",
            "project_url": project_url,
            "doi": doi,
        }

    def harvest_records(self):
        print("🔄 Starting OAI-PMH harvest for uni-halle...")
        records = []

        params = {
            "verb": "ListRecords",
            "metadataPrefix": "oai_dc",
        }

        page_no = 1
        token = None

        while True:
            if token:
                params = {"verb": "ListRecords", "resumptionToken": token}

            print(f"OAI page {page_no}...")
            resp = safe_request(self.session_oai, OAI_ENDPOINT_UNI_HALLE, params=params)
            root = ET.fromstring(resp.text)

            page_records = root.findall(".//oai:record", NS)
            print(f"→ {len(page_records)} records")

            for rec in page_records:
                parsed = self._parse_record(rec)
                if parsed:
                    records.append(parsed)

            token_el = root.find(".//oai:resumptionToken", NS)
            token = token_el.text.strip() if token_el is not None and token_el.text else None

            if not token:
                break

            page_no += 1
            sleep_jitter()

        print(f"✅ Total parsed OAI records with project URLs: {len(records)}")
        return records

    def scrape_bitstreams(self, project_url: str):
        sleep_jitter()
        resp = safe_request(self.session_web, project_url)
        html = resp.text

        paths = list(dict.fromkeys(BITSTREAM_RE.findall(html)))

        files = []
        for path in paths:
            dl_url = urljoin(REPOSITORY_URL_UNI_HALLE, path)
            name = unquote(Path(path).name)
            files.append({
                "file_name": name,
                "download_url": dl_url,
                "file_url": dl_url,
            })

        return files

    def safe_download(self, url: str, target_path: Path):
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix(target_path.suffix + ".part")

        try:
            r = self.session_web.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            r.raise_for_status()

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)

            if is_blocked_html(tmp_path):
                if tmp_path.exists():
                    tmp_path.unlink()
                return False, "blocked_html_response"

            ext = target_path.suffix.lower()

            if ext == ".pdf" and not is_valid_pdf(tmp_path):
                if tmp_path.exists():
                    tmp_path.unlink()
                return False, "invalid_pdf"

            if ext == ".zip" and not is_valid_zip(tmp_path):
                if tmp_path.exists():
                    tmp_path.unlink()
                return False, "invalid_zip"

            if target_path.exists():
                target_path.unlink()

            os.replace(tmp_path, target_path)
            return True, "downloaded"

        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink()
            return False, str(e)[:300]

    def inspect_zip_for_qda(self, cur, con, project_id: int, zip_path: Path):
        found = 0

        try:
            if not zipfile.is_zipfile(zip_path):
                return 0

            with zipfile.ZipFile(zip_path, "r") as zf:
                for info in zf.infolist():
                    inner_name = info.filename
                    if is_qda_file(inner_name):
                        local_path = f"{zip_path}::{inner_name}"
                        _, inserted = insert_file(cur, con, {
                            "project_id": project_id,
                            "file_name": inner_name,
                            "file_type": file_ext(inner_name),
                            "file_url": "",
                            "size": str(info.file_size),
                            "local_path": local_path,
                            "status": "DOWNLOADED",
                            "status_note": f"inside_zip:{zip_path.name}",
                            "downloaded_at": now_utc(),
                        })
                        if inserted:
                            found += 1
        except Exception as e:
            print(f"   ⚠️ zip inspection failed for {zip_path.name}: {e}")

        return found

    def process(self, cur, con):
        records = self.harvest_records()
        candidates = [r for r in records if self.is_qda_candidate(r)]

        print(f"🎯 QDA-like candidate projects: {len(candidates)}")

        for idx, meta in enumerate(candidates, start=1):
            print(f"\n===== PROJECT {idx}/{len(candidates)} =====")
            print(meta["project_url"])

            project_folder = self.repository_folder / safe_folder_name(meta["title"])
            project_folder.mkdir(parents=True, exist_ok=True)

            project_payload = {
                "query_string": "oai_full_harvest+qda_filter",
                "repository_id": REPOSITORY_ID_UNI_HALLE,
                "repository_url": REPOSITORY_URL_UNI_HALLE,
                "project_url": meta["project_url"],
                "version": "",
                "title": meta["title"],
                "description": meta["description"],
                "language": meta["language"],
                "doi": meta["doi"],
                "upload_date": meta["dates"][0] if meta["dates"] else "",
                "download_date": now_utc(),
                "download_repository_folder": str(self.repository_folder),
                "download_project_folder": str(project_folder),
                "download_version_folder": str(project_folder),
                "download_method": "OAI-PMH+HTML",
            }

            project_id, inserted = insert_project(cur, con, project_payload)

            if inserted:
                for s in meta["subjects"]:
                    insert_keyword(cur, con, project_id, s)
                for lic in meta["rights"]:
                    insert_license(cur, con, project_id, lic)
                for c in meta["creators"]:
                    insert_person(cur, con, project_id, c, "AUTHOR")
                print("   ✅ project metadata inserted")
            else:
                print("   ℹ️ project already exists")

            try:
                files = self.scrape_bitstreams(meta["project_url"])
                print(f"   visible bitstream files: {len(files)}")
            except Exception as e:
                print(f"   ❌ bitstream scrape failed: {e}")
                continue

            downloaded = 0
            failed = 0
            qda_inside_zip = 0

            for f in files:
                file_name = f["file_name"]
                local_path = str(project_folder / file_name)

                _, inserted = insert_file(cur, con, {
                    "project_id": project_id,
                    "file_name": file_name,
                    "file_type": file_ext(file_name),
                    "file_url": f["file_url"],
                    "size": "",
                    "local_path": local_path,
                    "status": "PENDING",
                    "status_note": "metadata_inserted_before_download",
                    "downloaded_at": now_utc(),
                })

                ok, note = self.safe_download(f["download_url"], Path(local_path))

                if ok:
                    update_file_status(cur, con, project_id, file_name, local_path, "DOWNLOADED", note)
                    downloaded += 1

                    if file_ext(file_name) == ".zip":
                        qda_inside_zip += self.inspect_zip_for_qda(cur, con, project_id, Path(local_path))
                else:
                    update_file_status(cur, con, project_id, file_name, local_path, "FAILED", note)
                    failed += 1

            print(f"   ✅ downloaded files: {downloaded}")
            print(f"   ❌ failed files: {failed}")
            print(f"   🎯 QDA files found inside ZIP: {qda_inside_zip}")