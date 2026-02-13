# MSW Asset Downloader

**DISCLAIMER:** This repository **does not contain any game assets**. It provides scripts that help you organize and convert assets from your own local MapleStory Worlds Builder cache — files that the official Builder client downloads during normal use. No encryption is bypassed, no servers are exploited, and no protected content is extracted by any means other than reading the cache that the Builder itself creates on your machine. The resulting assets are **copyrighted intellectual property of Nexon** and may be used **only for creating MapleStory Worlds** in accordance with the official MapleStory Worlds Terms of Use and any applicable Nexon policies. Do not use this tool or extracted assets for unauthorized distribution, commercial use outside the platform, or any purpose that violates those terms. By using this repository you agree to comply with MapleStory Worlds' Terms of Use. **This software is provided as-is with no warranty; use at your own risk.**

---

Extract a catalog of MapleStory Worlds resources, cache them via the MSW Builder client, then extract images, audio, and a PGLite metadata DB or CSV for use in your own workflows to speed up development.

**How it works:** You create a MapleStory World, set up a local workspace, merge this repo into it, then run a script, load cache in the builder in a custom world, and run a final script. The catalog is written once to **RootDesk/MyDesk/** and shared by all steps automatically — no manual file copying or moving at any point.

**Known issues:** Image offsets are unverified; they passed initial testing but should have more testing before relying on them.

---

## Quick start (Steps 1–6)

Follow these steps **in order**. The whole process is: install tools, set up your workspace, download the catalog, cache the assets in Builder, then extract them.

> **Heads up:** Steps 4, 5, and 6 each take a **long time** on a full run (several hours each). Budget accordingly. Test mode is fast and recommended for your first run.

> **Shortcut:** A pre-built **resources.csv** is available on [Google Drive](https://drive.google.com/file/d/1dgrBW4qzl4mqzpbs24op4T1MLyC1s9zV/view?usp=sharing). If you use it, you can skip Step 4 entirely just by dropping it into **RootDesk/MyDesk/** and go to Step 5. The prebuilt CSV contains ~95% of all RUIDs, as of 2/11/2026. I highly recommend you do this rather than run step 4 yourself.

> **Recommended:** When you reach Steps 4 and 6, run them in **test mode** first (answer **Y** when prompted). This validates your setup without touching real output. Then run again with **N** for a full run. You can do this for both before running step 5, just be sure to run 4 before 6.

---

### Step 1 — Install prerequisites

Install these two things manually — everything else is prompted by the scripts automatically:

1. **PowerShell 7 or later** — `winget install Microsoft.PowerShell` or download from [PowerShell GitHub](https://github.com/PowerShell/PowerShell#get-powershell). The scripts cannot run without it.
2. **MapleStory Worlds Builder** — install from [MapleStory Worlds](https://maplestoryworlds.nexon.com). Needed for Step 5.

The scripts also need **Python 3.7+** and **Node.js**, but they will detect if these are missing and tell you how to install them. All other dependencies (Python packages, npm packages, DuckDB) are prompted for installation automatically when you run the scripts.

---

### Step 2 — Create a MapleStory World and local workspace

1. Go to [MapleStory Worlds](https://maplestoryworlds.nexon.com) or open the **Builder** and **create a new world**. Do **not** use an existing world — this process will overwrite project files.
2. **Create a local workspace** from that world's config. Follow the official guide: **[LocalWorkspace](https://maplestoryworlds-creators.nexon.com/en/docs?postId=1165)** (MapleStory Worlds Creator Center). This creates a folder on your PC — that folder is your **workspace**.
3. **Close the project in Builder** before continuing.

---

### Step 3 — Merge this repo into the workspace

1. Open two **File Explorer** windows side by side:
   - **Left:** This repo folder (the one you downloaded/cloned — it contains `run-metadata.ps1`, `run-extraction.ps1`, `RootDesk/`, `Metadata Downloader/`, `asset-extraction/`, etc.).
   - **Right:** Your workspace folder (from Step 2).
2. Select **everything** in the left window (Ctrl+A) and **drag it into** the right window.
3. When prompted, choose **Replace the files in the destination** (overwrite same-named files).

After this, your **workspace folder** contains everything: the Builder project files **and** all the scripts from this repo. You can close the repo folder; from now on, you work entirely in this workspace folder.

---
## Limiting what gets downloaded

You can shrink the catalog (and save time in Steps 4, 5, and 6) by limiting which categories or subcategories are scraped.

- **By subcategory:** Edit **Metadata Downloader/steps/category_subcategories.json**. Under each category’s `subcategories` object, remove the entries you don’t want (e.g. remove `"12": "background"` to skip background sprites). To skip the “all” segment for a category, remove its `"-1": "all"` entry. Only what’s listed is scraped; `-1` is scraped last when present. After editing, run **Step 0** again so `last_pages.csv` matches, then run Step 4 as usual.
- **By category:** When running the metadata script (Step 4), you can pass **--categories** (e.g. `0,1,3` to skip avatar items). From the **Metadata Downloader** folder: `python steps/1-collect.py -o path/to/output.csv --categories 0,1,3 ...` (see [Metadata Downloader/README.md](Metadata%20Downloader/README.md) for full options).


> If you do not **remove the `"-1":"all"` category**, you will STILL DOWNLOAD ALL ASSETS for that category. If you remove subcategories, it is **recommended** to remove the all subcategory as well. It keep it as a fallback to make sure you get everything, but it *significantly* increases the time step 4 takes to complete.
---

### Step 4 — Download the resource catalog

> **Shortcut:** You can skip this step entirely by downloading a pre-built **resources.csv** from [Google Drive](https://drive.google.com/file/d/1dgrBW4qzl4mqzpbs24op4T1MLyC1s9zV/view?usp=sharing) and placing it in your workspace at **RootDesk/MyDesk/resources.csv**. Then go straight to Step 5. This CSV contains ~95% of all RUIDs, as of 2/11/2026.

This step fetches metadata (RUIDs, categories, formats, tags) from the MapleStory Worlds API and writes **resources.csv** directly into **RootDesk/MyDesk/** inside your workspace.

> **This step takes a long time.** A full catalog download (all categories, with enrichment) will take **several hours** event on great hardware. Test mode is much faster and you should run it first to ensure the full version works. If test mode runs without issue, you are free to run step 4 in the background, just dont close Powershell.

1. Open **PowerShell 7**. It MUST be Powershell **7 or later** it will not work on a previous version.
2. `cd` to your **workspace folder**. To get the path: in File Explorer, open your workspace folder → click the **address bar** → **Ctrl+C** to copy. Then in PowerShell:

```powershell
cd "C:\paste\your\workspace\path\here"
```

   Press enter.

3. Run:

```powershell
.\run-metadata.ps1
```

4. Follow the prompts:
   - **Run in test mode?** — Choose **Y** for your first run (recommended) or **N** for a full run.
   - **Install dependencies?** — Choose **Y** if offered.
   - **\_ifwt token** — You need your login cookie. Go to https://maplestoryworlds.nexon.com and log in → press **F12** → **Application** tab → **Cookies** → copy the **\_ifwt** value → paste it in. Choose **Y** to save it for next time.
   - **Collect** — Choose **1** (create) for a first run. Pick categories (Enter = all).
   - **Enrich** — Choose **Y**. Set workers (0 = auto).
5. Wait for completion. The catalog is written to **RootDesk/MyDesk/resources.csv** (or **resources_test.csv** in test mode).

---

### Step 5 — Populate the resource cache (Builder)

This step loads the catalog in the MSW Builder and downloads every resource into a local cache that the extraction pipeline reads.

> **This step takes a long time.** The Builder downloads every resource in the catalog one by one. With a full catalog this can take **several hours**. I ran this overnight, as it will eat up all your RAM and make your system laggy. Progress is shown in the game UI. I noticed that even my ram hit >98%, MSW did not make my system crash. Your mileage may vary.

1. Open **MapleStory Worlds Builder**.
2. **Open your workspace folder** as the project (File → Open → select the workspace folder from Step 2).
3. Press **Play** to run the world. The in-game downloader (AssetRuidList + AssetPreloadRunner) reads the catalog from **RootDesk/MyDesk** and preloads assets in batches. Progress is shown in the game UI.
4. Let preload finish (or load what you need). The cache is stored at:

```
%LOCALAPPDATA%\..\LocalLow\nexon\MapleStory Worlds\resource_cache
```

**NOTICE:** you may see a popup while downloading saying you were disconnected from the world and to close the game. I noticed that my assets kept downloading with this popup in the way. However, if you close the popup, **IT WILL CLOSE YOUR GAME.** If at any point your game closes:
1. Make note of what row you are on in the UI (Row XX of YY)
2. Stop running the world
3. Reimport the world (hamburger menu at the top left of Hierarchy)
4. After reload, start the world again
5. Enter the row from step 1 to pick up from where you left off


---

### Step 6 — Extract assets

This step reads the catalog (from Step 4) and the cache (from Step 5) and extracts images, audio, thumbnails, and creates a metadata database or CSVs.

> **This step takes a long time.** Extracting and converting all sprites, audio clips, animation frames, and building the database can take **several hours** depending on catalog size and your hardware.

1. Open **PowerShell 7** and `cd` to your **workspace folder** (same folder as Step 4).
2. Run:

```powershell
.\run-extraction.ps1
```

3. Follow the prompts — the script auto-detects missing tools (Node.js, DuckDB, npm packages) and offers to install them:
   - **Run in test mode?** — Choose **Y** for your first run (recommended) or **N** for a full run.
   - **Output format** — Choose **1** (PGLite), **2** (CSV only), or **3** (both).
4. Wait for completion. Output is in **asset-extraction/output/**:
   - `images/` — extracted sprites and clip frames
   - `audio/` — extracted audio clips
   - `thumbs/` — generated thumbnails
   - **metadata/** — PGLite database directory with all asset metadata
   - `staging/` — intermediate CSVs

---

## Running from anywhere (one-liners)

If you don't want to `cd` first, you can run the scripts from any PowerShell window. Replace `<WORKSPACE>` with your workspace folder path:

```powershell
# Step 4 — download catalog
& "<WORKSPACE>\run-metadata.ps1"

# Step 6 — extract assets
& "<WORKSPACE>\run-extraction.ps1"
```

To get the path: in File Explorer, open your workspace folder → click the address bar → Ctrl+C.

---

## At a glance

| Step | What happens | Script / Action |
|------|-------------|----------------|
| **1** | Install prerequisites | PowerShell 7 + MSW Builder (scripts prompt for everything else) |
| **2** | Create world + local workspace | MSW website or Builder → [LocalWorkspace guide](https://maplestoryworlds-creators.nexon.com/en/docs?postId=1165) |
| **3** | Merge repo into workspace | Drag all files → overwrite same names |
| **4** | Download resource catalog | `.\run-metadata.ps1` → writes **RootDesk/MyDesk/resources.csv** |
| **5** | Populate resource cache | Open workspace in Builder → Play → wait for preload |
| **6** | Extract assets | `.\run-extraction.ps1` → writes **asset-extraction/output/** |

**Data flow:** Step 4 writes the catalog to **RootDesk/MyDesk/resources.csv**. Step 5 (Builder) reads that same file and fills the local cache. Step 6 reads both the catalog and cache to produce the final output. Nothing needs to be copied or moved.

---

## In-depth

### Downloader (MSW Builder)

**RootDesk/MyDesk** codeblocks: **AssetRuidList** (loads catalog from UserDataSet `resources`, filters by category/subcategory, exposes `ASSET_LIST` and batch APIs). **AssetPreloadRunner** (preloads in batches via `PreloadAsync`). **AssetProgressUpdater** / **StartLoadTrigger** (UI and trigger). The pipeline reads the same catalog as Builder (**RootDesk/MyDesk/resources.csv** from Step 4) and only extracts RUIDs that are in the cache.

### Pipeline overview

**Entry:** `asset-extraction/run-asset-extraction.ps1`. **Input:** **RootDesk/MyDesk/resources.csv** (from Step 4). **Cache:** `%LOCALAPPDATA%\..\LocalLow\nexon\MapleStory Worlds\resource_cache` (override with `-CacheDir` on step scripts). **Output:** `output/images/`, `output/audio/`, `output/thumbs/`, `output/metadata/` (PGLite data dir); staging CSVs in `output/staging/`.

**7 internal steps:** (1) Build catalog → staging. (2) Enrich with cache index. (3) Extract sprites + audio. (4) Build enc map for clips. (5) Extract clip frames. (6) Build final DB (and/or final CSVs). (7) Build thumbnails. Full details: [asset-extraction/README.md](asset-extraction/README.md).

### Running the pipeline directly (reference)

You can also run the pipeline directly from the `asset-extraction` folder instead of using `run-extraction.ps1`:

```powershell
cd asset-extraction
npm install
.\run-asset-extraction.ps1
```

**Paths (relative to asset-extraction root):**

| Path | Purpose |
|------|--------|
| Catalog | **RootDesk/MyDesk/resources.csv** (from Step 4); read automatically. |
| **output/** | Final result: images/, audio/, thumbs/, metadata/ (PGLite), staging/. |
| **output-test/**, **temp-test/** | Used only with **-Test**; main output/ and temp/ are unchanged. |
| **temp/** | Staging during extraction (e.g. offsets). |
| **logs/** | Pipeline and per-step logs. |

**Flags**

| Flag | Meaning |
|------|--------|
| **-Test** | All outputs go to **output-test/** and **temp-test/**; some steps apply row limits. |
| **-SkipExisting** | Skip a step if its main output already exists (steps 1–5, 7; step 6 always runs). |
| **-Workers N** | N = 0: half of logical cores (default). N > 0: that many workers. |
| **-StartAtStep N** | Run steps N through 7 (1–7). |
| **-OnlyStep N** | Run only step N (1–7). |
| **-AssetExtractionRoot "path"** | Use this folder as asset-extraction root (default: script directory). |
| **-NonInteractive** | No prompts; use RootDesk/MyDesk/resources.csv; output format defaults to PGLite. |
| **-OutputFormat pglite \| csv \| both** | **pglite** = metadata/ directory only (default). **csv** = 5 final_*.csv only; step 7 skipped. **both** = DB + CSVs. |

**Examples**

```powershell
.\run-asset-extraction.ps1 -Test -NonInteractive
.\run-asset-extraction.ps1 -SkipExisting -Workers 4
.\run-asset-extraction.ps1 -StartAtStep 3
.\run-asset-extraction.ps1 -OnlyStep 6
.\run-asset-extraction.ps1 -OutputFormat csv -NonInteractive
```

More details: [Metadata Downloader/README.md](Metadata%20Downloader/README.md), [asset-extraction/README.md](asset-extraction/README.md).
