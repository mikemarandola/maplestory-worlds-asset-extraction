# Metadata Downloader

Downloads resource metadata (RUIDs, category, date, format, tags) from MapleStory Worlds.

**Paths:** All paths are relative to this folder (the folder containing `metadata-extractor.ps1`). Store the project anywhere; scripts resolve paths from this directory.

## Requirements

- **PowerShell 7** or newer
- **Python 3.7+** on your PATH

## First run

1. Open PowerShell at the **repo root** (the folder that contains `Metadata Downloader` and `asset-extraction`). Then go into this folder and run the launcher:
   ```powershell
   cd "Metadata Downloader"
   ```
2. Run the launcher:
   ```powershell
   .\metadata-extractor.ps1
   ```
3. When prompted, choose **Yes** to install dependencies from `requirements.txt` (optional: browser_cookie3 for reading your session from the browser).
4. You will be asked for your **_ifwt** token. Get it from the site: log in at https://maplestoryworlds.nexon.com, then F12 → Application → Cookies → copy the `_ifwt` value.

## Options

- **Test mode:** Catalog is written to **RootDesk/MyDesk/resources_test.csv**; step 0 data goes to `test/`. From repo root: `.\run-metadata.ps1` and choose Y when prompted. Or from this folder:
  ```powershell
  .\metadata-extractor.ps1 -Test
  ```
- **Skip step 0** (use existing `last_pages.csv`):
  ```powershell
  .\metadata-extractor.ps1 -FromStep 1
  ```
- **Enrich only** (use existing `resources.csv`):
  ```powershell
  .\metadata-extractor.ps1 -FromStep 2
  ```

## Outputs

- `last_pages.csv` – last page number per category/subcategory (from step 0)
- `resources.csv` – RUID, Category, Subcategory, Date, Format, Tags (from steps 1 and 2)

The final enriched CSV is always written to **RootDesk/MyDesk**: **resources.csv** (non-test) or **resources_test.csv** (test). The extraction pipeline (`run-extraction.ps1`) and the MSW Builder project (AssetRuidList) read from that folder.
