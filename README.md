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
│   ├── summary.py
│   └── export_csv.py
│
├── data/
│   ├── metadata.db
│   └── csv/
│       ├── projects.csv
│       ├── files.csv
│       ├── keywords.csv
│       ├── licenses.csv
│       └── person_role.csv
│
├── files/
│   └── uni-halle/
│       └── [downloaded project folders]
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

---

## Running the Pipeline

### 1. Initialize and run pipeline
python scripts/pipeline.py

### 2. Generate summary

### 3. Export CSV files
python scripts/export_csv.py

---

Output

The pipeline produces:

- Structured SQLite database (metadata.db)
- CSV exports for analysis
- Downloaded dataset files organized by project
  
---

Key Features
Modular pipeline design
Scalable metadata harvesting using OAI-PMH
Automated file discovery and download
Robust file validation
Structured data storage
CSV export for downstream processing

---

Notes
The pipeline performs a full metadata harvest due to OAI-PMH limitations (no server-side keyword filtering)
Processing time depends on repository size
Downloaded files are validated to avoid corrupted or blocked responses

---

## Results Summary

The pipeline was executed on Repository 16 (uni-halle) using the OAI-PMH harvesting approach.

### Harvesting Results

- Total projects collected: **855**
- Total files discovered: **950**
- Successfully downloaded files: **948**
- Failed downloads: **2**

### Metadata Collected

- Keywords extracted: **2849**
- Licenses recorded: **851**
- People/roles identified: **3424**

### Data Size

- Total downloaded data: approximately **6 GB**

---

## Interpretation

The pipeline successfully performed large-scale metadata harvesting and file acquisition from the repository.

This demonstrates:

- Efficient OAI-PMH harvesting across thousands of records
- Reliable extraction of file links from project pages
- Stable large-scale downloading with minimal failure rate
- Structured storage of metadata for further analysis

The modular design allows this pipeline to be extended to additional repositories with minimal changes.

