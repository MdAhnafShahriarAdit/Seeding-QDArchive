from pathlib import Path

DB_PATH = Path("data/metadata.db")
FILES_ROOT = Path("files")
CSV_ROOT = Path("data/csv")

# Repo 16
REPOSITORY_ID_UNI_HALLE = 16
REPOSITORY_URL_UNI_HALLE = "https://opendata.uni-halle.de/"
OAI_ENDPOINT_UNI_HALLE = "https://opendata.uni-halle.de/oai/request"

USER_AGENT_OAI = "QDA-OAI-Harvester/2.0"
USER_AGENT_WEB = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 60
DELAY_MIN = 1.5
DELAY_MAX = 4.0

QDA_KEYWORDS = [
    "qdpx",
    "nvivo",
    "nvpx",
    "atlas.ti",
    "atlasi",
    "atlas ti",
    "atlproj",
    "maxqda",
    "caqdas",
    "qualitative data",
    "qualitative research",
    "qualitative interview",
    "interview transcript",
    "focus group",
    "coding",
    "coded transcript",
    "transcript",
]

QDA_EXTENSIONS = {
    ".qdpx", ".qdc", ".nvp", ".nvpx", ".atlproj", ".mx", ".mx20", ".mx24"
}