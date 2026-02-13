# Step 4: Build enc map. DuckDB exports sprite_list.csv; Node.js reads .mod bytes 3-18, writes enc_keys.csv.

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
$spriteListCsv = Join-Path $stagingDirActual "sprite_list.csv"
$encKeysCsv = Join-Path $stagingDirActual "enc_keys.csv"

if (-not (Test-Path $catalogEnrichedCsv)) {
    Write-Error "catalog_enriched.csv not found. Run Step 2 first."
    exit 1
}

if ($SkipExisting -and (Test-Path $encKeysCsv)) {
    $lineCount = (Get-Content $encKeysCsv -TotalCount 2 | Measure-Object -Line).Lines
    if ($lineCount -ge 2) { Write-Host "Step 4: enc_keys.csv already exists. Skipping."; exit 0 }
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
$catalogForDuck = $catalogEnrichedCsv -replace '\\', '/'
$spriteListForDuck = $spriteListCsv -replace '\\', '/'
$tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'
$sqlPath = Join-Path $scriptRoot "sql\04-export-sprite-list.sql"
$sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
$sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{catalogEnrichedCsv\}', $catalogForDuck -replace '\{outputCsv\}', $spriteListForDuck
$tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
try {
    Write-Host "Step 4 Phase A: DuckDB export sprite_list.csv"
    & duckdb -init $tmpSql -no-stdin
    if ($LASTEXITCODE -ne 0) { Write-Error "DuckDB failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue }

$concurrency = if ($Concurrency -gt 0) { $Concurrency } else { $script:AssetExtractionParallelism }
Write-Host "Step 4 Phase B: Node.js enc key extraction -> enc_keys.csv (concurrency: $concurrency)"
$encMapScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\build-enc-map-db.js"
$nodeArgs = @("$encMapScript", "--input-csv", $spriteListCsv, "--out-csv", $encKeysCsv, "--cache-dir", $CacheDir, "--concurrency", $concurrency)
if ($Test) { $nodeArgs += "--test" }
Push-Location $assetExtRoot
try {
    & node @nodeArgs
    if ($LASTEXITCODE -ne 0) { Write-Error "enc map failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Pop-Location }
$rows = (Get-Content $encKeysCsv | Measure-Object -Line).Lines
Write-Host "Step 4 done. enc_keys: $($rows - 1) rows. Output: $encKeysCsv"
exit 0
