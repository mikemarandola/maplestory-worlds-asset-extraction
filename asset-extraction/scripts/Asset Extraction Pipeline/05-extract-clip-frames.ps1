# Step 5: Extract clip frames. DuckDB exports clip_list + enc_ruid_map; Node.js parses .mod, writes frame_index.csv.

param(
    [string] $CacheDir = "",
    [string] $StagingDir = "",
    [int] $Workers = 0,   # when > 0, DuckDB uses this many threads (e.g. 1 for single-thread)
    [int] $Concurrency = 0,
    [switch] $SkipExisting,
    [switch] $Test
)

$ErrorActionPreference = "Stop"
$scriptRoot = $PSScriptRoot
. (Join-Path $scriptRoot "lib\duckdb-env.ps1")
$assetExtRoot = Resolve-AssetExtRoot
$scriptsDir = Join-Path $assetExtRoot "scripts"
. (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-parallelism.ps1")
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$outputDir = $outputDirBase
$stagingDirActual = if ([string]::IsNullOrEmpty($StagingDir)) { Join-Path $outputDir "staging" } else { $StagingDir }
$catalogEnrichedCsv = Join-Path $stagingDirActual "catalog_enriched.csv"
$encKeysCsv = Join-Path $stagingDirActual "enc_keys.csv"
$clipListCsv = Join-Path $stagingDirActual "clip_list.csv"
$encRuidMapCsv = Join-Path $stagingDirActual "enc_ruid_map.csv"
$frameIndexCsv = Join-Path $stagingDirActual "frame_index.csv"

if (-not (Test-Path $catalogEnrichedCsv) -or -not (Test-Path $encKeysCsv)) {
    Write-Error "catalog_enriched.csv and enc_keys.csv required. Run Steps 2 and 4 first."
    exit 1
}

if ($SkipExisting -and (Test-Path $frameIndexCsv)) {
    $lineCount = (Get-Content $frameIndexCsv -TotalCount 2 | Measure-Object -Line).Lines
    if ($lineCount -ge 2) { Write-Host "Step 5: frame_index.csv already exists. Skipping."; exit 0 }
}

if ([string]::IsNullOrEmpty($CacheDir)) {
    . (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-msw-cache-dir.ps1")
    $CacheDir = $script:MSWCacheDir
}
if ([string]::IsNullOrEmpty($CacheDir) -or -not (Test-Path $CacheDir)) {
    Write-Error "Cache directory not found. Pass -CacheDir."
    exit 1
}

Ensure-DuckDBTools
Initialize-DuckDBStaging -StagingDir $stagingDirActual
$env = Get-DuckDBEnv
$duckDbThreads = if ($Workers -gt 0) { $Workers } else { $env.Threads }
$tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'
$catalogForDuck = $catalogEnrichedCsv -replace '\\', '/'
$encKeysForDuck = $encKeysCsv -replace '\\', '/'
$clipListForDuck = $clipListCsv -replace '\\', '/'
$encRuidMapForDuck = $encRuidMapCsv -replace '\\', '/'
$sqlPath = Join-Path $scriptRoot "sql\05-export-clip-list.sql"
$sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
$sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{catalogEnrichedCsv\}', $catalogForDuck -replace '\{encKeysCsv\}', $encKeysForDuck -replace '\{clipListCsv\}', $clipListForDuck -replace '\{encRuidMapCsv\}', $encRuidMapForDuck
$tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
try {
    Write-Host "Step 5 Phase A: DuckDB export clip_list + enc_ruid_map"
    & duckdb -init $tmpSql -no-stdin
    if ($LASTEXITCODE -ne 0) { Write-Error "DuckDB failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue }

$concurrency = if ($Concurrency -gt 0) { $Concurrency } else { $script:AssetExtractionParallelism }
Write-Host "Step 5 Phase B: Node.js clip frame extraction -> frame_index.csv (concurrency: $concurrency)"
$extractScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\extract-clip-frames-db.js"
$nodeArgs = @("$extractScript", "--clip-list-csv", $clipListCsv, "--enc-ruid-map-csv", $encRuidMapCsv, "--out-csv", $frameIndexCsv, "--cache-dir", $CacheDir, "--concurrency", $concurrency)
if ($Test) { $nodeArgs += "--test" }
Push-Location $assetExtRoot
try {
    & node @nodeArgs
    if ($LASTEXITCODE -ne 0) { Write-Error "Clip frames failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Pop-Location }
$rows = (Get-Content $frameIndexCsv | Measure-Object -Line).Lines
Write-Host "Step 5 done. frame_index: $($rows - 1) rows. Output: $frameIndexCsv"
exit 0
