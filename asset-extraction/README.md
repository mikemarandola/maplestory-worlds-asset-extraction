# MSW Asset Extraction Pipeline

Takes MSW cached assets and a **resources catalog CSV** and produces extracted images/audio, animation frame data, a **SQLite metadata DB**, and thumbnails. Used by the skill-editor asset browser.

**Paths:** All paths are relative to this folder (the one containing `run-asset-extraction.ps1`). Store the project anywhere; scripts resolve paths from this directory.

**Runner:** **run-asset-extraction.ps1** (in this folder). It runs **7 steps** under **scripts/Asset Extraction Pipeline/** (PowerShell step scripts, **helper-scripts/** for Node.js, **sql/** for DuckDB).

**Requirements:** PowerShell 7+, Node.js, **DuckDB CLI**, **sqlite3** CLI, and npm dependencies (`npm install`). MSW resource cache must be populated (open Builder → Resource Storage and browse).  
When you run the script **without -NonInteractive**, it will prompt to install any missing dependency (via **winget** on Windows) or run **npm install** if node_modules are missing; confirm with Y to install.

**Run the full pipeline:**

From the **asset-extraction** folder (e.g. from the repo root: `cd asset-extraction`, or use your path to this folder):

```powershell
cd asset-extraction   # or: cd "path\to\where\you\stored\asset-extraction"
.\run-asset-extraction.ps1
```

**Catalog:** The pipeline uses **RootDesk/MyDesk/resources.csv** (created by the metadata extraction step — `run-metadata.ps1`). It must have columns: **RUID**, **Category**, **Subcategory**, **Format**, **Tags** (optional: Date, Name). Step 1 writes **output/staging/catalog.csv**; later steps read from **output/staging/** and write images, audio, and **output/metadata.db** there.

**Test run (no overwrite):** All step outputs go to **output-test/** and **temp-test/**; main **output/** and **temp/** are unchanged.

```powershell
.\run-asset-extraction.ps1 -Test -NonInteractive
```

---

## Table of contents

1. [Flags](#flags)
2. [Folders and paths](#folders-and-paths)
3. [Step 1 — Build catalog](#step-1--build-catalog)
4. [Step 2 — Enrich with cache](#step-2--enrich-with-cache)
5. [Step 3 — Extract sprites + audio](#step-3--extract-sprites--audio)
6. [Step 4 — Build enc map](#step-4--build-enc-map)
7. [Step 5 — Extract clip frames](#step-5--extract-clip-frames)
8. [Step 6 — Build final DB](#step-6--build-final-db)
9. [Step 7 — Build thumbnails](#step-7--build-thumbnails)

---

## Flags

| Flag | Meaning |
|------|--------|
| **-Test** | Test mode: same catalog (RootDesk/MyDesk/resources.csv), but **all step outputs** go to **output-test/** and **temp-test/** (never overwrites output/ or temp/). Some steps apply limits (e.g. row limits) when -Test. |
| **-SkipExisting** | Skip a step if its main output already exists (steps 1, 2, 3, 4, 5, 7; step 6 always runs Phase A/B/C). |
| **-Workers N** | N = 0: use half of logical cores (default). N &gt; 0: that many workers/threads. Passed to steps as Workers, ThrottleLimit, or Concurrency. |
| **-StartAtStep N** | Run steps N through 7 (1–7). |
| **-OnlyStep N** | Run only step N (1–7). |
| **-AssetExtractionRoot "path"** | Use this folder as the asset-extraction root (default: directory containing the script). Lets you run from another CWD or after moving the folder. |
| **-NonInteractive** | No prompts; use RootDesk/MyDesk/resources.csv. Output format defaults to SQLite. |
| **-OutputFormat sqlite \| csv \| both** | Final output: **sqlite** = output/metadata.db only (default). **csv** = only the 5 final_*.csv in output/staging (no metadata.db); step 7 skipped. **both** = metadata.db and the 5 final_*.csv. |

When running without -NonInteractive, you are prompted: **Final output format: 1 = SQLite DB, 2 = CSV per table only, 3 = Both.** Choose 1, 2, or 3; default is 1.

Examples:

```powershell
.\run-asset-extraction.ps1 -Test -NonInteractive
.\run-asset-extraction.ps1 -SkipExisting -Workers 4
.\run-asset-extraction.ps1 -StartAtStep 3
.\run-asset-extraction.ps1 -OnlyStep 6
.\run-asset-extraction.ps1 -OutputFormat csv -NonInteractive
```

---

## Folders and paths

All paths are relative to the **asset-extraction root**—the folder that contains **run-asset-extraction.ps1** (or the path you pass as **-AssetExtractionRoot**). Store this folder anywhere; the runner and every step resolve paths from that root (script location or `ASSET_EXTRACTION_ROOT` env). No absolute paths are used.

| Path | Purpose |
|------|--------|
| **output/** | After pipeline: **output/images/** (Category/Subcategory/ruid.png), **output/audio/** (Category/Subcategory/ruid.ogg), **output/thumbs/** (ruid.png), **output/metadata.db**. |
| **output/staging/** | Intermediate CSVs: catalog.csv, catalog_enriched.csv, cache_index.csv, extract_list.csv, offsets.csv, sprite_list.csv, enc_keys.csv, clip_list.csv, enc_ruid_map.csv, frame_index.csv, existing_paths.csv, final_*.csv. Used by steps 1–6. |
| **temp/** | Offsets staging (e.g. offsets-staging.jsonl) during step 3. |
| **output-test/**, **temp-test/** | Used only when **-Test**; same structure as output/ and temp/, so the main pipeline is never overwritten. |
| **logs/** | Pipeline log: **asset-extraction-YYYYMMDD-HHmmss.log**; per-step logs: **stepNN-YYYYMMDD-HHmmss.log**. |
| **Cache** | MSW cache: `%LOCALAPPDATA%\..\LocalLow\nexon\MapleStory Worlds\resource_cache`. Resolved by **scripts/Asset Extraction Pipeline/helper-scripts/_get-msw-cache-dir.ps1**; override with **-CacheDir** when calling step scripts directly. |

**Catalog:** The pipeline reads **RootDesk/MyDesk/resources.csv** (created by the metadata extraction step — `run-metadata.ps1` in the workspace root). Required columns: **RUID**, **Category**, **Subcategory**, **Format**, **Tags**. Pipeline step 1 normalizes it to **output/staging/catalog.csv**; all later steps use staging CSVs and write back to staging or output. The **temp/** folder is cleaned automatically after a successful run.

---

## Step 1 — Build catalog

**Script:** `scripts/Asset Extraction Pipeline/01-build-catalog.ps1`  
**Input:** **RootDesk/MyDesk/resources.csv** (from repo root; or **-CsvPath**). Columns: RUID, Category, Subcategory, Format, Tags (optional Date, Name).  
**Output:** **output/staging/catalog.csv** — normalized catalog (ruid, category, subcategory, output_subdir, name, asset_type, tags_normalized). One row per valid 32‑hex RUID.  
**How:** DuckDB reads the CSV, normalizes columns and tags, dedupes by RUID, writes catalog. **-Test** can apply a row limit.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\01-build-catalog.ps1` or `-CsvPath "path\to\file.csv"` `-Workers N` `-SkipExisting` `-Test`

---

## Step 2 — Enrich with cache

**Script:** `scripts/Asset Extraction Pipeline/02-enrich-catalog.ps1`  
**Input:** **output/staging/catalog.csv**, MSW resource cache (default from helper).  
**Output:** **output/staging/cache_index.csv** (Phase A), **output/staging/catalog_enriched.csv** (Phase B) — catalog joined with cache (relative_path, suffix, asset_type, etc.).  
**How:** Phase A: Node **walk-cache-to-csv.js** walks the cache → cache_index.csv. Phase B: DuckDB joins catalog + cache_index → catalog_enriched.csv.  
**Test mode:** With **-Test**, the walk uses **--sample-all-categories**: full cache scan, up to 10 entries per asset_type (sprite, audioclip, animationclip, avataritem, etc.) so the pipeline exercises every category. The catalog is augmented with any cache_index RUIDs not in the input catalog so the join produces enriched rows for audio, clips, and avatar items.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\02-enrich-catalog.ps1` `-CacheDir "path"` `-SkipExisting` `-Test` `-Workers N`

---

## Step 3 — Extract sprites + audio

**Script:** `scripts/Asset Extraction Pipeline/03-extract-sprites-audio.ps1`  
**Input:** **output/staging/catalog_enriched.csv**, cache.  
**Output:** **output/staging/extract_list.csv** (Phase A), **output/images/** (Category/Subcategory/ruid.png), **output/audio/** (Category/Subcategory/ruid.ogg), **output/staging/offsets.csv** (ruid, output_subdir, offset_x, offset_y). Offsets are written via **temp/offsets-staging.jsonl** then merged to staging.  
**How:** Phase A: DuckDB exports extract_list from catalog_enriched. Phase B: **helper-scripts/extract-sprites-to-output.ps1** (Node + batch scripts) extracts sprites and audio, writes offsets to temp. Phase C: offsets JSONL → **output/staging/offsets.csv**.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\03-extract-sprites-audio.ps1` `-CacheDir` `-OutDir` `-AudioOutDir` `-StagingDir` `-SkipExisting` `-Test` `-Workers N` `-ThrottleLimit N`

---

## Step 4 — Build enc map

**Script:** `scripts/Asset Extraction Pipeline/04-build-enc-map.ps1`  
**Input:** **output/staging/catalog_enriched.csv**, cache.  
**Output:** **output/staging/sprite_list.csv** (Phase A), **output/staging/enc_keys.csv** (Phase B) — enc_hex → ruid for sprites (.win/.dxt .mod bytes 3–18).  
**How:** Phase A: DuckDB exports sprite_list (sprites only). Phase B: Node **build-enc-map-db.js** reads .mod files, extracts 16‑byte enc, writes enc_keys.csv. Used by step 5 to resolve clip frame RUIDs.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\04-build-enc-map.ps1` `-CacheDir` `-StagingDir` `-SkipExisting` `-Test` `-Workers N` `-Concurrency N`

---

## Step 5 — Extract clip frames

**Script:** `scripts/Asset Extraction Pipeline/05-extract-clip-frames.ps1`  
**Input:** **output/staging/catalog_enriched.csv**, **output/staging/enc_keys.csv**, cache.  
**Output:** **output/staging/clip_list.csv**, **output/staging/enc_ruid_map.csv** (Phase A), **output/staging/frame_index.csv** (Phase B) — clip_ruid, frame_index, frame_ruid, frame_duration_ms.  
**How:** Phase A: DuckDB exports clip_list and enc_ruid_map. Phase B: Node **extract-clip-frames-db.js** parses animation clip .mod files, resolves frame RUIDs via enc map, writes frame_index.csv.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\05-extract-clip-frames.ps1` `-CacheDir` `-StagingDir` `-SkipExisting` `-Test` `-Workers N` `-Concurrency N`

---

## Step 6 — Build final DB

**Script:** `scripts/Asset Extraction Pipeline/06-build-final-db.ps1`  
**Input:** All staging CSVs (catalog, catalog_enriched, offsets, enc_keys, frame_index); **output/images** and **output/audio** (for existence checks).  
**Output:** **output/staging/existing_paths.csv** (Phase A), **output/staging/final_tag_names.csv**, **final_tags.csv**, **final_assets.csv**, **final_animation_frames.csv**, **final_cache_locations.csv** (Phase B), **output/metadata.db** (Phase C).  
**How:** Phase A: Node **walk-output-to-csv.js** lists existing image/audio paths → existing_paths.csv. Phase B: DuckDB loads all staging CSVs, builds tag_names, tags, enriched view, animation_frames, assets, cache_locations, exports 5 final CSVs. Phase C (when output format is **sqlite** or **both**): sqlite3 creates a fresh **metadata.db** and imports the final CSVs (schema in **sql/06-create-sqlite.sql**). When output format is **csv**, Phase C is skipped and only the 5 final_*.csv files are produced. **both** produces the CSVs (Phase B) and metadata.db (Phase C).  
**Run alone:** `.\scripts\Asset Extraction Pipeline\06-build-final-db.ps1` `-OutDb` `-StagingDir` `-OutputDir` `-OutputFormat sqlite|csv|both` `-Workers N` `-Test`

---

## Step 7 — Build thumbnails

**Script:** `scripts/Asset Extraction Pipeline/07-build-thumbs.ps1`  
**Input:** **output/metadata.db**, **output/images**.  
**Output:** **output/thumbs/ruid.png** — one per image-bearing asset (sprite, damageskin, avataritem, atlas). Animation clips use **thumbnail_ruid** in the DB (median frame); consumers use **thumbs/<thumbnail_ruid>.png**.  
**How:** Node **build-thumbs.js** reads assets from metadata.db, resizes images from output/images, writes to output/thumbs. **Without -Test:** it iterates DB RUIDs (sprite/damageskin/avataritem/atlas) and creates a thumb only when that RUID has a file in output/images. **With -Test:** it is driven by the images dir (one thumb per image, up to the test limit) so every extracted image gets a thumb and you don't get fewer thumbs than images due to DB order. **-SkipExisting** skips existing thumb files.  
**Run alone:** `.\scripts\Asset Extraction Pipeline\07-build-thumbs.ps1` `-OutDb` `-OutputDir` `-ImagesDir` `-ThumbsDir` `-SkipExisting` `-Test` `-Concurrency N`

---

## Archive

Legacy and alternate pipelines live under **archive/** (e.g. **archive/legacy-pipeline/**, **archive/new-pipeline/**). The **supported** path is **run-asset-extraction.ps1** + **scripts/Asset Extraction Pipeline/** only.
