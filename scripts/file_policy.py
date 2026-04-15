from pathlib import Path


QDA_EXTENSIONS = {
    ".qdpx", ".qdc", ".nvp", ".nvpx", ".atlproj", ".mx", ".mx20", ".mx24"
}

DOCUMENT_EXTENSIONS = {
    ".pdf", ".txt", ".csv", ".xls", ".xlsx", ".doc", ".docx", ".rtf", ".tsv", ".ods"
}

MEDIA_EXTENSIONS = {
    ".mp3", ".wav", ".m4a", ".flac", ".aac",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".mpeg", ".mpg"
}


def get_file_extension(file_name: str) -> str:
    return Path(file_name).suffix.lower()


def guess_mime_group(file_name: str) -> str:
    ext = get_file_extension(file_name)

    if ext in QDA_EXTENSIONS:
        return "qda"
    if ext in DOCUMENT_EXTENSIONS:
        return "document"
    if ext in MEDIA_EXTENSIONS:
        return "media"
    return "other"


def decide_file_policy(file_name: str, restricted: bool = False):
    """
    Returns a dict with:
    - file_extension
    - mime_type (group, not strict MIME)
    - access_status
    - download_status
    - download_policy
    - failure_reason
    """

    ext = get_file_extension(file_name)
    mime_group = guess_mime_group(file_name)

    if restricted:
        return {
            "file_extension": ext,
            "mime_type": mime_group,
            "access_status": "restricted",
            "download_status": "pending",
            "download_policy": "restricted",
            "failure_reason": "request_access_required"
        }

    if ext in QDA_EXTENSIONS:
        return {
            "file_extension": ext,
            "mime_type": mime_group,
            "access_status": "downloadable",
            "download_status": "pending",
            "download_policy": "download",
            "failure_reason": ""
        }

    if ext in DOCUMENT_EXTENSIONS:
        return {
            "file_extension": ext,
            "mime_type": mime_group,
            "access_status": "downloadable",
            "download_status": "metadata_only",
            "download_policy": "metadata_only",
            "failure_reason": "not_attempted_by_policy"
        }

    if ext in MEDIA_EXTENSIONS:
        return {
            "file_extension": ext,
            "mime_type": mime_group,
            "access_status": "downloadable",
            "download_status": "skipped",
            "download_policy": "never_download",
            "failure_reason": "skipped_large_media_type"
        }

    return {
        "file_extension": ext,
        "mime_type": mime_group,
        "access_status": "unknown",
        "download_status": "metadata_only",
        "download_policy": "metadata_only",
        "failure_reason": "unknown_file_type"
    }