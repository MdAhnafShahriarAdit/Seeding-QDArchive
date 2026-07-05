# QDArchive Data Acquisition Pipeline

This project implements a complete data acquisition pipeline for collecting qualitative research datasets from public repositories. The pipeline focuses on large-scale metadata harvesting, structured storage, and file download automation.

The system is designed to support reproducible data collection workflows for qualitative data archives.

---

## Overview

The pipeline performs the following steps:

1. Harvests metadata from repository endpoints (OAI-PMH)
2. Filters projects based on qualitative research indicators
3. Extracts file links from project pages
4. Downloads files and stores them locally
5. Organizes metadata into a structured SQLite database
6. Exports collected data into CSV format for analysis

---

## Repository Source

This implementation focuses on:

- **Repository 16 (uni-halle)**
- Accessed via **OAI-PMH interface**
- Project pages are parsed to extract downloadable file links
- Repository 7 (ADA Dataverse)
- Accessed via seed URLs and browser-based extraction (Selenium)
- Dataset pages are parsed to extract metadata and visible file listings

---
## Project Structure

```text
qdarchive-project/
│
├── scripts/
│   ├── config.py
│   ├── database.py
│   ├── harvesters.py
│   ├── pipeline.py
│   ├── acquire_ada_seed.py
│   ├── summary.py
│   └── export_csv.py
│
├── data/
│   ├── 23206422-sq26.db
│   └── csv/
│       ├── projects.csv
│       ├── files.csv
│       ├── keywords.csv
│       ├── licenses.csv
│       └── person_role.csv
│
├── files/
│   ├── uni-halle/
│   └── ada/
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Database Schema

The pipeline stores all information in a normalized SQLite database:

- **projects** – project-level metadata
- **files** – downloaded and discovered files
- **keywords** – subject tags
- **licenses** – licensing information
- **person_role** – authors and contributors

---

## Pipeline Workflow

### Step 1 – Metadata Harvesting
- Uses OAI-PMH to retrieve all repository records
- Processes records page-by-page

### Step 2 – Filtering
- Identifies relevant projects using keyword-based filtering

### Step 3 – Project Page Processing
- Visits project pages
- Extracts file download links using HTML parsing

### Step 4 – File Download
- Downloads files with validation checks
- Tracks success and failure states

### Step 5 – ZIP Inspection
- Inspects downloaded archives for nested files

### Step 6 – Data Storage
- Stores metadata and file information in SQLite database
  
### Step 7 – ADA Processing (Repository 7)
- Uses Selenium to render dataset pages
- Extracts metadata and visible file rows
- Identifies restricted files and stores them in database



---

## Running the Pipeline

### 1. Run Repository 16 pipeline
python scripts/pipeline.py

### 2. Run ADA (Repository 7) acquisition
python scripts/acquire_ada_seed.py

### 3. Generate summary
python scripts/summary.py

### 4. Export CSV files
python scripts/export_csv.py

---

## Output

The pipeline produces:

- Structured SQLite database (23206422-sq26.db)
- CSV exports for analysis
- Downloaded dataset files (Repository 16)
- Metadata-only file records for restricted repositories (Repository 7)
  
---

## Key Features

- Modular pipeline design
- Scalable metadata harvesting using OAI-PMH
- Browser-based extraction using Selenium
- Automated file discovery and download
- Robust file validation
- Structured data storage
- CSV export for downstream processing

---

## Notes

- The pipeline performs a full metadata harvest due to OAI-PMH limitations (no server-side keyword filtering)
- Processing time depends on repository size
- Downloaded files are validated to avoid corrupted or blocked responses
- Repository 16 supports full metadata and file access
- Repository 7 (ADA) enforces strict access control on files
- ADA uses a strong server-side system that distinguishes between human users and automated requests
- Despite multiple approaches (requests and Selenium), file downloads from ADA were not possible
- Attempts were made to contact ADA administrators for access permission, but no response was received during the project timeline

---

## Results Summary

The pipeline was executed on two repositories.

### Overall Database Summary

Final records stored in the database:

- Total projects: 908
- Total files: 1527
- Total keywords: 3565
- Total licenses: 904
- Total people/roles: 3550

These values represent the complete dataset collected across both repositories.

### Repository 16 (uni-halle) Harvesting Results

- Total projects collected: **855**
- Total files discovered: **950**
- Successfully downloaded files: **948**
- Failed downloads: **2**

### Metadata Collected

- Keywords extracted: **2849**
- Licenses recorded: **851**
- People/roles identified: **3424**

### Repository 7 (ADA Dataverse) Acquisition Results

- Total projects collected: **53**
- Total files discovered: **577**
- Successfully downloaded files: **0**
- Failed downloads: **577**

### Metadata Collected

- Keywords extracted: **716**
- Licenses recorded: **53**
- People/roles identified: **126**
- Total ADA projects processed: **53**
- Restricted files: **577**
- Metadata-only files: **360**
- Zip Files: **186**
- Dataset pages successfully accessed using Selenium
- File rows extracted and recorded in database

### File  Status
Files detected but marked as:
- status = RESTRICTED
- status_note = visible_restricted_file_row

No files downloaded due to repository restrictions

### Metadata Collected
- Keywords, licenses, and authors successfully extracted
- File-level metadata recorded without download

---

## Interpretation

Data acquisition is not only a technical problem, but also depends on repository access policies and permissions.

---

### Conclusion

This demonstrates:

- A complete, scalable data acquisition pipeline
- Full metadata and file download capabilities for open repositories
- Adaptation to restricted repositories using browser-based techniques
- Proper handling and storage of restricted data scenarios

However, Repository 7 illustrates a realistic constraint:

- Strong repository security mechanisms can prevent automated file downloads
- Access to such datasets requires explicit permission from repository administrators

---

### key Takeway


---

## Part 2: Classification

Building on the acquisition pipeline from Part 1, Part 2 classifies every collected project into a project type and an ISIC Rev. 5 industry division, using both project metadata and, where available, the actual content of primary data files.

### Overview

Part 2 performs the following steps:

1. Classifies each project into one of four types based on its file extensions
2. Classifies each project into an ISIC Rev. 5 division (two levels deep: section + division), using a keyword-based classifier enriched with DDC (Dewey Decimal) subject code mapping
3. Classifies each primary data file individually (not just the project as a whole)
4. Exports the results as an XLSX table and a PDF report with histograms and ranked class tables, per repository

---

### Project Type Classification

Each project is classified into exactly one of:

- **QDA_PROJECT** – contains a file with a QDA/REFI file extension (e.g. `.qdpx`)
- **QD_PROJECT** – no QDA file, but contains a primary data file (`.txt`, `.pdf`, `.rtf`, `.doc`, `.docx`)
- **OTHER_PROJECT** – no primary data file, but contains some other recognizable file type (e.g. `.csv`, `.xlsx`, image/media files)
- **NOT_A_PROJECT** – no file type could be identified at all

### ISIC Rev. 5 Classification

Each project (and each of its primary data files) is scored against all 81 ISIC Rev. 5 divisions using:

- Keyword matching against project title, description, and keywords
- A DDC (Dewey Decimal Classification) → ISIC mapping, giving extra weight to `ddc:` keywords present on repository 16 (uni-halle) projects, since these are curated subject codes rather than free text
- For `QD_PROJECT`/`QDA_PROJECT` types, the actual text content of each primary data file (PDF, DOCX, TXT, RTF) is extracted and classified independently; if a file's content can't be read or doesn't yield a confident match, its classification falls back to the project-level result

The top two matching divisions are recorded as `primary_class` and `secondary_class` (if any).

---

### Project Structure (additions)

```text
scripts/
├── classify_project_type.py         # Step 1: QDA/QD/OTHER/NOT_A_PROJECT classification
├── isic_data.py                     # ISIC Rev. 5 division data + keyword lexicon
├── ddc_mapping.py                   # DDC subject code -> ISIC division mapping
├── classify_isic.py                 # Step 2/3: ISIC classification (project + file level)
├── export_classification_xlsx.py    # Step 4c: XLSX table export
├── generate_classification_report.py # Step 4d: PDF report with histograms and tables
└── summary.py                       # extended with classification stats per repository

data/
├── 23206422-sq26-classification.db  # final classification database
├── 23206422-classification-table.xlsx
└── 23206422-classification-report.pdf
```

---

### Running the Classification Pipeline

Run all of the following from the **repository root** (not from inside `scripts/`):

Automated data pipelines can efficiently extract metadata across repositories.

However, actual data access depends on repository-level permissions, which cannot always be bypassed through technical methods alone.

