# Step 2: Enrich catalog with cache (Node.js walk + DuckDB join). Output: staging/catalog_enriched.csv + staging/cache_index.csv

param(
    [string] $CacheDir = "",
    [string] $StagingDir = "",
    [int] $Workers = 0,   # when > 0, DuckDB uses this many threads (e.g. 1 for single-thread)
    [switch] $SkipExisting,
    [switch] $Test
)

$ErrorActionPreference = "Stop"
$scriptRoot = $PSScriptRoot
. (Join-Path $scriptRoot "lib\duckdb-env.ps1")
$assetExtRoot = Resolve-AssetExtRoot
$scriptsDir = Join-Path $assetExtRoot "scripts"
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$outputDir = $outputDirBase
$stagingDirActual = if ([string]::IsNullOrEmpty($StagingDir)) { Join-Path $outputDir "staging" } else { $StagingDir }
$catalogCsv = Join-Path $stagingDirActual "catalog.csv"
$cacheIndexCsv = Join-Path $stagingDirActual "cache_index.csv"
$catalogEnrichedCsv = Join-Path $stagingDirActual "catalog_enriched.csv"

if (-not (Test-Path $catalogCsv)) {
    Write-Error "catalog.csv not found: $catalogCsv. Run Step 1 first."
    exit 1
}

if ($SkipExisting -and (Test-Path $catalogEnrichedCsv)) {
    $lineCount = (Get-Content $catalogEnrichedCsv -TotalCount 2 | Measure-Object -Line).Lines
    if ($lineCount -ge 2) { Write-Host "Step 2: catalog_enriched.csv already exists. Skipping."; exit 0 }
}

# Resolve cache dir (MSW default if not provided)
if ([string]::IsNullOrEmpty($CacheDir)) {
    . (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-msw-cache-dir.ps1")
    $CacheDir = $script:MSWCacheDir
}
if ([string]::IsNullOrEmpty($CacheDir) -or -not (Test-Path $CacheDir)) {
    Write-Error "Cache directory not found: $CacheDir. Pass -CacheDir or set MSW cache."
    exit 1
}

if (-not (Test-Path $stagingDirActual)) { New-Item -ItemType Directory -Path $stagingDirActual -Force | Out-Null }
Ensure-DuckDBTools
Initialize-DuckDBStaging -StagingDir $stagingDirActual
$env = Get-DuckDBEnv
$duckDbThreads = if ($Workers -gt 0) { $Workers } else { $env.Threads }

# Phase A: Node.js cache walk -> cache_index.csv
# Test with -SampleAllCategories: full cache scan, sample per category (sprite, audioclip, animationclip, avataritem, etc.) so pipeline exercises all types.
# Test without: only cache entries for catalog RUIDs, then sample per category (may be only sprite if catalog is sprite-only).
Write-Host "Step 2 Phase A: Walking cache -> cache_index.csv"
$walkScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\walk-cache-to-csv.js"
if (-not (Test-Path $walkScript)) { Write-Error "walk-cache-to-csv.js not found: $walkScript"; exit 1 }
$nodeArgs = @($walkScript, "--cache-dir", $CacheDir, "--out-csv", $cacheIndexCsv)
if ($Test) {
    $nodeArgs += "--test"
    $nodeArgs += "--sample-all-categories"
}
Push-Location $assetExtRoot
try {
    & node @nodeArgs
    if ($LASTEXITCODE -ne 0) { Write-Error "Phase A failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Pop-Location }

# Test only: augment catalog with RUIDs from cache_index that are not in catalog, so join produces enriched rows for audio/clip/avatar etc.
$catalogForJoinCsv = $catalogCsv
if ($Test) {
    $cacheIndexData = Import-Csv -Path $cacheIndexCsv -Encoding UTF8
    $catalogData = Import-Csv -Path $catalogCsv -Encoding UTF8
    $catalogRuidSet = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($r in $catalogData) { $catalogRuidSet.Add(($r.ruid -replace '^\s+|\s+$', '')) | Out-Null }
    $seen = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $augmentRows = [System.Collections.Generic.List[object]]::new()
    foreach ($row in $cacheIndexData) {
        $ruid = ($row.ruid -replace '^\s+|\s+$', '')
        if ([string]::IsNullOrEmpty($ruid)) { continue }
        if ($catalogRuidSet.Contains($ruid)) { continue }
        if ($seen.Add($ruid)) {
            $at = ($row.asset_type -replace '^\s+|\s+$', '') -replace '^$', 'unknown'
            $augmentRows.Add([PSCustomObject]@{
                ruid = $ruid
                category = $at
                subcategory = 'test'
                output_subdir = "$at/test"
                asset_type = 'mod'
                tags = ''
                tags_normalized = ''
            })
        }
    }
    if ($augmentRows.Count -gt 0) {
        $catalogForJoinCsv = Join-Path $stagingDirActual "catalog_for_join.csv"
        $catalogData + $augmentRows | Export-Csv -Path $catalogForJoinCsv -Encoding UTF8 -NoTypeInformation
        Write-Host "  Test: augmented catalog with $($augmentRows.Count) RUID(s) from cache_index -> catalog_for_join.csv"
    }
}

# Phase B: DuckDB join -> catalog_enriched.csv
Write-Host "Step 2 Phase B: DuckDB join -> catalog_enriched.csv"
$catalogForDuck = $catalogForJoinCsv -replace '\\', '/'
$cacheIndexForDuck = $cacheIndexCsv -replace '\\', '/'
$outputForDuck = $catalogEnrichedCsv -replace '\\', '/'
$tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'

$sqlPath = Join-Path $scriptRoot "sql\02-enrich-catalog.sql"
$sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
$sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{catalogCsv\}', $catalogForDuck -replace '\{cacheIndexCsv\}', $cacheIndexForDuck -replace '\{outputCsv\}', $outputForDuck

$tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
try {
    & duckdb -init $tmpSql -no-stdin
    if ($LASTEXITCODE -ne 0) { Write-Error "Phase B failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
    $rows = (Get-Content $catalogEnrichedCsv | Measure-Object -Line).Lines
    Write-Host "  catalog_enriched rows: $($rows - 1) (excluding header)"
} finally {
    Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue
}
Write-Host "Step 2 done. Output: $catalogEnrichedCsv"
exit 0
