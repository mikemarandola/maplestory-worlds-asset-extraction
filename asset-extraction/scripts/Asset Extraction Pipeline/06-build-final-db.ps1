# Step 6: Build final metadata.db. Phase A: walk output -> existing_paths.csv. Phase B: DuckDB join all -> 5 CSVs. Phase C: sqlite3 fresh DB .import.

param(
    [string] $OutDb = "",
    [string] $StagingDir = "",
    [string] $OutputDir = "",
    [ValidateSet("sqlite", "csv", "both", "")]
    [string] $OutputFormat = "",   # sqlite = build metadata.db; csv = final_*.csv only; both = DB and CSVs. Default from env or sqlite.
    [int] $Workers = 0,   # when > 0, DuckDB uses this many threads (e.g. 1 for single-thread)
    [switch] $SkipExisting,
    [switch] $Test
)

$ErrorActionPreference = "Stop"
$scriptRoot = $PSScriptRoot
. (Join-Path $scriptRoot "lib\duckdb-env.ps1")
Ensure-Sqlite3
Ensure-DuckDBTools   # node required for Phase A (walk-output-to-csv.js)
$assetExtRoot = Resolve-AssetExtRoot
$scriptsDir = Join-Path $assetExtRoot "scripts"
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$outputDirActual = if ([string]::IsNullOrEmpty($OutputDir)) { $outputDirBase } else { $OutputDir }
$stagingDirActual = if ([string]::IsNullOrEmpty($StagingDir)) { Join-Path $outputDirActual "staging" } else { $StagingDir }
$outDbPath = if ([string]::IsNullOrEmpty($OutDb)) { Join-Path $outputDirActual "metadata.db" } else { $OutDb }
$outDbTmp = $outDbPath + ".tmp"
$imagesDir = Join-Path $outputDirActual "images"
$audioDir = Join-Path $outputDirActual "audio"

$outputFormatActual = if ([string]::IsNullOrWhiteSpace($OutputFormat)) { if ($env:ASSET_EXTRACTION_OUTPUT_FORMAT) { $env:ASSET_EXTRACTION_OUTPUT_FORMAT } else { "sqlite" } } else { $OutputFormat }

$existingPathsCsv = Join-Path $stagingDirActual "existing_paths.csv"
$catalogCsv = Join-Path $stagingDirActual "catalog.csv"
$catalogEnrichedCsv = Join-Path $stagingDirActual "catalog_enriched.csv"
$offsetsCsv = Join-Path $stagingDirActual "offsets.csv"
$encKeysCsv = Join-Path $stagingDirActual "enc_keys.csv"
$frameIndexCsv = Join-Path $stagingDirActual "frame_index.csv"

foreach ($f in @($catalogCsv, $catalogEnrichedCsv, $offsetsCsv, $encKeysCsv, $frameIndexCsv)) {
    if (-not (Test-Path $f)) { Write-Error "Missing staging file: $f. Run steps 1-5 first."; exit 1 }
}

# Phase A: Walk output -> existing_paths.csv
Write-Host "Step 6 Phase A: Walking output dirs -> existing_paths.csv"
$walkScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\walk-output-to-csv.js"
if (-not (Test-Path $walkScript)) { Write-Error "walk-output-to-csv.js not found."; exit 1 }
& node $walkScript --images-dir $imagesDir --audio-dir $audioDir --out-csv $existingPathsCsv
if ($LASTEXITCODE -ne 0) { Write-Error "Phase A failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }

# Phase B: DuckDB build 5 final CSVs
Initialize-DuckDBStaging -StagingDir $stagingDirActual
$env = Get-DuckDBEnv
$duckDbThreads = if ($Workers -gt 0) { $Workers } else { $env.Threads }
$stagingForDuck = $stagingDirActual -replace '\\', '/'
$tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'
$sqlPath = Join-Path $scriptRoot "sql\06-build-final-db.sql"
$sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
$sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{staging\}', $stagingForDuck
$tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
try {
    Write-Host "Step 6 Phase B: DuckDB assemble and export 5 CSVs"
    & duckdb -init $tmpSql -no-stdin
    if ($LASTEXITCODE -ne 0) { Write-Error "Phase B failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue }

# Phase C: sqlite3 fresh DB
$finalTagNames = Join-Path $stagingDirActual "final_tag_names.csv"
$finalTags = Join-Path $stagingDirActual "final_tags.csv"
$finalAssets = Join-Path $stagingDirActual "final_assets.csv"
$finalAnimationFrames = Join-Path $stagingDirActual "final_animation_frames.csv"
$finalCacheLocations = Join-Path $stagingDirActual "final_cache_locations.csv"
foreach ($f in @($finalTagNames, $finalTags, $finalAssets, $finalAnimationFrames, $finalCacheLocations)) {
    if (-not (Test-Path $f)) { Write-Error "DuckDB did not produce: $f"; exit 1 }
}

if ($outputFormatActual -eq "csv") {
    # CSV only: Phase B already wrote final_*.csv; skip Phase C
    Write-Host "Step 6 Phase C: skipped (output format = CSV per table). Final tables: $stagingDirActual\final_*.csv"
    Write-Host "Step 6 done. Output: final_tag_names.csv, final_tags.csv, final_assets.csv, final_animation_frames.csv, final_cache_locations.csv"
    exit 0
}

# sqlite or both: run Phase C (metadata.db). CSVs already in staging from Phase B.
Write-Host "Step 6 Phase C: sqlite3 fresh DB (journal_mode=OFF)"
$createSqlPath = Join-Path $scriptRoot "sql\06-create-sqlite.sql"
$createSql = Get-Content -Path $createSqlPath -Raw -Encoding UTF8
$createSql = $createSql -replace '\{final_tag_names\}', ($finalTagNames -replace '\\', '/')
$createSql = $createSql -replace '\{final_tags\}', ($finalTags -replace '\\', '/')
$createSql = $createSql -replace '\{final_assets\}', ($finalAssets -replace '\\', '/')
$createSql = $createSql -replace '\{final_animation_frames\}', ($finalAnimationFrames -replace '\\', '/')
$createSql = $createSql -replace '\{final_cache_locations\}', ($finalCacheLocations -replace '\\', '/')
$tmpSqliteSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSqliteSql -Value $createSql -Encoding UTF8 -NoNewline
try {
    if (Test-Path $outDbTmp) { Remove-Item $outDbTmp -Force }
    & sqlite3 $outDbTmp ".read '$($tmpSqliteSql -replace "'", "''")'"
    if ($LASTEXITCODE -ne 0) { Write-Error "Phase C failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
    if (Test-Path $outDbPath) { Remove-Item $outDbPath -Force }
    Move-Item $outDbTmp $outDbPath -Force
    Write-Host "  Written: $outDbPath"
} finally {
    Remove-Item $tmpSqliteSql -Force -ErrorAction SilentlyContinue
    if (Test-Path $outDbTmp) { Remove-Item $outDbTmp -Force -ErrorAction SilentlyContinue }
}
Write-Host "Step 6 done. Output: $outDbPath"
exit 0
