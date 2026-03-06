# Technical challenges observed (Part 1)

- Zenodo records do not always expose a download link the same way, so the pipeline uses a fallback from `links.download` to `links.self`.
- Search results can match QDA keywords but still not contain real QDA files, so we filter by file extensions (e.g., .qdpx, .nvpx, .mx).
- Metadata is inconsistent across records (some fields missing or formatted differently), so the schema must be flexible.
- Individual file downloads can fail, so the pipeline continues instead of crashing.
- A “project folder” is represented as one repository record with multiple attached files, not a real directory structure.