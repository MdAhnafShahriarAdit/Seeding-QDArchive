ADA (Repository 7) — Data Access Limitation and Permission Constraints

For Repository 7 (ADA Dataverse), a browser-based acquisition strategy using Selenium was implemented to overcome initial request blocking encountered with standard HTTP requests. This approach successfully allowed dataset pages to be fully rendered and enabled the extraction of project-level metadata, including titles, descriptions, keywords, and file listings.

Unlike the earlier attempt using direct requests, which resulted in server-side blocking, the Selenium-based approach was able to access dataset pages consistently. This allowed the pipeline to identify file entries and extract file-level metadata such as file names and sizes.

However, despite successfully accessing the dataset pages, all files within ADA were found to be marked as restricted. These files require explicit permission from the repository administrators or dataset owners before access can be granted. As a result, no files could be downloaded through the automated pipeline.

Each detected file entry was therefore recorded in the database with:

- status = RESTRICTED
- status_note = visible_restricted_file_row

This reflects that the file exists and is visible, but cannot be accessed without authorization.

To address this limitation, an attempt was made to obtain legitimate access by contacting the ADA Dataverse administrators and requesting permission to download the datasets. However, no response was received during the project timeline. Without approved credentials or access rights, it was not possible to proceed with file downloads.

This highlights an important practical challenge when working with real-world research data repositories. While metadata can often be accessed and extracted using automated techniques, actual data acquisition may be restricted due to privacy concerns, licensing conditions, or institutional access policies.
