# Step 1: Build catalog from resources CSV (DuckDB only). Output: output/staging/catalog.csv
# Replaces new-pipeline steps 1+2 (load raw + build catalog). CSV must have: RUID, Category, Subcategory, Format, Tags.

param(
    [string] $CsvPath = "",
    [string] $CsvSearchDir = "",
    [string] $StagingDir = "",
    [int] $Workers = 0,   # when > 0, DuckDB uses this many threads (e.g. 1 for single-thread)
    [switch] $SkipExisting,
    [switch] $Test,
    [int] $TestLimit = 5000
)

$ErrorActionPreference = "Stop"
$scriptRoot = $PSScriptRoot
. (Join-Path $scriptRoot "lib\duckdb-env.ps1")
$assetExtRoot = Resolve-AssetExtRoot
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$outputDir = $outputDirBase
$stagingDirActual = if ([string]::IsNullOrEmpty($StagingDir)) { Join-Path $outputDir "staging" } else { $StagingDir }
$catalogOut = Join-Path $stagingDirActual "catalog.csv"

if ($SkipExisting -and (Test-Path $catalogOut)) {
    $lineCount = (Get-Content $catalogOut -TotalCount 2 | Measure-Object -Line).Lines
    if ($lineCount -ge 2) { Write-Host "Step 1: catalog.csv already exists. Skipping."; exit 0 }
}

# Default catalog: RootDesk/MyDesk/resources.csv (created by Step 1 metadata extraction)
$catalogDefault = Join-Path $assetExtRoot "..\RootDesk\MyDesk\resources.csv"
$searchDir = if ([string]::IsNullOrEmpty($CsvSearchDir)) { (Split-Path $catalogDefault -Parent) } else { $CsvSearchDir }
if ([string]::IsNullOrEmpty($CsvPath)) {
    if (Test-Path $catalogDefault) {
        $CsvPath = [System.IO.Path]::GetFullPath((Resolve-Path $catalogDefault).Path)
    } elseif (Test-Path $searchDir) {
        $csvFiles = @(Get-ChildItem -Path $searchDir -Filter "*.csv" -File -ErrorAction SilentlyContinue | Sort-Object Name)
        if ($csvFiles.Count -gt 0) { $CsvPath = $csvFiles[0].FullName }
    }
    if ([string]::IsNullOrEmpty($CsvPath)) {
        Write-Error "Catalog not found. Run Step 1 (metadata extraction) first to create RootDesk/MyDesk/resources.csv, or pass -CsvPath."
        exit 1
    }
} else {
    $CsvPath = [System.IO.Path]::GetFullPath((Resolve-Path $CsvPath).Path)
}
if (-not (Test-Path $CsvPath)) { Write-Error "CSV not found: $CsvPath"; exit 1 }

if (-not (Test-Path $stagingDirActual)) { New-Item -ItemType Directory -Path $stagingDirActual -Force | Out-Null }
Ensure-DuckDBTools
Initialize-DuckDBStaging -StagingDir $stagingDirActual
$env = Get-DuckDBEnv
$duckDbThreads = if ($Workers -gt 0) { $Workers } else { $env.Threads }

$inputCsvForDuck = $CsvPath -replace '\\', '/'
$outputCsvForDuck = $catalogOut -replace '\\', '/'
$tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'

$sqlPath = Join-Path $scriptRoot "sql\01-build-catalog.sql"
$sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
$limitClause = ""
if ($Test -and $TestLimit -gt 0) { $limitClause = " LIMIT $TestLimit" }
$sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{inputCsv\}', $inputCsvForDuck -replace '\{outputCsv\}', $outputCsvForDuck -replace '\{limitClause\}', $limitClause

$tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
try {
    Write-Host "Step 1: Build catalog (DuckDB). Input: $([System.IO.Path]::GetFileName($CsvPath)) -> staging/catalog.csv"
    Write-Host "  Threads: $duckDbThreads, Memory: $($env.MemoryLimit)"
    & duckdb -init $tmpSql -no-stdin
    if ($LASTEXITCODE -ne 0) { Write-Error "DuckDB failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
    $rows = (Get-Content $catalogOut | Measure-Object -Line).Lines
    Write-Host "  Catalog rows: $($rows - 1) (excluding header)"
} finally {
    Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue
}
Write-Host "Step 1 done. Output: $catalogOut"
exit 0
