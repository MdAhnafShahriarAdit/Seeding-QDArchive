# Seeding QDArchive — Part 1 (Data Acquisition)

This project downloads open-licensed qualitative research project files from Zenodo and stores metadata in a SQLite database (exportable to CSV).

## Run (Windows / PowerShell)
1) Create DB
python scripts/init_db.py

2) Download datasets
python scripts/acquire_zenodo.py

3) Export CSV
python scripts/export_csv.py

4) Quick summary
python scripts/summary.py

5) Export Output Format v1 (one row per QDA file)
python scripts/export_output_v1.py

## Outputs
- Downloaded files: archive/zenodo/<one-folder-per-project>/
- SQLite DB (local): data/metadata.db
- CSV exports (tracked): data/csv/
  - datasets.csv
  - files.csv
  - output_v1.csv