# Step 3: Extract sprites and audio. DuckDB exports extract_list.csv; existing 04-extract uses it and writes offsets to staging.

param(
    [string] $CacheDir = "",
    [string] $OutDir = "",
    [string] $AudioOutDir = "",
    [string] $StagingDir = "",
    [int] $Workers = 0,
    [int] $ThrottleLimit = 0,
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
$tempDirBase = if ($env:ASSET_EXTRACTION_TEMP_DIR) { $env:ASSET_EXTRACTION_TEMP_DIR } else { Join-Path $assetExtRoot "temp" }
$stagingDirActual = if ([string]::IsNullOrEmpty($StagingDir)) { Join-Path $outputDir "staging" } else { $StagingDir }
$catalogEnrichedCsv = Join-Path $stagingDirActual "catalog_enriched.csv"
$extractListCsv = Join-Path $stagingDirActual "extract_list.csv"
$offsetsCsv = Join-Path $stagingDirActual "offsets.csv"
$tempDir = $tempDirBase
$offsetsJsonl = Join-Path $tempDir "offsets-staging.jsonl"

if (-not (Test-Path $catalogEnrichedCsv)) {
    Write-Error "catalog_enriched.csv not found: $catalogEnrichedCsv. Run Step 2 first."
    exit 1
}

# Resolve cache dir
if ([string]::IsNullOrEmpty($CacheDir)) {
    . (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-msw-cache-dir.ps1")
    $CacheDir = $script:MSWCacheDir
}
if ([string]::IsNullOrEmpty($CacheDir) -or -not (Test-Path $CacheDir)) {
    Write-Error "Cache directory not found. Pass -CacheDir or set MSW cache."
    exit 1
}

$outDirActual = if ([string]::IsNullOrEmpty($OutDir)) { Join-Path $outputDir "images" } else { $OutDir }
$audioOutActual = if ([string]::IsNullOrEmpty($AudioOutDir)) { Join-Path $outputDir "audio" } else { $AudioOutDir }

# Phase A: DuckDB export extract_list.csv
if ($SkipExisting -and (Test-Path $extractListCsv)) {
    $lineCount = (Get-Content $extractListCsv -TotalCount 2 | Measure-Object -Line).Lines
    if ($lineCount -ge 2) { Write-Host "Step 3 Phase A: extract_list.csv exists. Skipping export." }
} else {
    Ensure-DuckDBTools
    Initialize-DuckDBStaging -StagingDir $stagingDirActual
    $env = Get-DuckDBEnv
    $duckDbThreads = if ($Workers -gt 0) { $Workers } else { $env.Threads }
    $catalogForDuck = $catalogEnrichedCsv -replace '\\', '/'
    $extractListForDuck = $extractListCsv -replace '\\', '/'
    $tempDirForDuck = (Join-Path $stagingDirActual "duckdb_temp") -replace '\\', '/'
    $sqlPath = Join-Path $scriptRoot "sql\03-export-extract-list.sql"
    $sqlContent = Get-Content -Path $sqlPath -Raw -Encoding UTF8
    $sqlContent = $sqlContent -replace '\{threads\}', $duckDbThreads -replace '\{memLimit\}', $env.MemoryLimit -replace '\{tempDir\}', $tempDirForDuck -replace '\{catalogEnrichedCsv\}', $catalogForDuck -replace '\{outputCsv\}', $extractListForDuck
    $tmpSql = [System.IO.Path]::GetTempFileName() + ".sql"
    Set-Content -Path $tmpSql -Value $sqlContent -Encoding UTF8 -NoNewline
    try {
        Write-Host "Step 3 Phase A: DuckDB export extract_list.csv"
        & duckdb -init $tmpSql -no-stdin
        if ($LASTEXITCODE -ne 0) { Write-Error "DuckDB failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
    } finally { Remove-Item $tmpSql -Force -ErrorAction SilentlyContinue }
}

$extractListRows = (Get-Content $extractListCsv -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
$extractListDataRows = [Math]::Max(0, $extractListRows - 1)
if ($extractListDataRows -eq 0) {
    Write-Error "extract_list.csv has 0 rows. Extraction (sprites/audio) will produce nothing. The catalog RUIDs (RootDesk/MyDesk/resources.csv) must exist in the MSW cache. Use a catalog CSV from the same machine where the MSW Builder cache is populated (e.g. Resource Storage) so the cache contains those assets, then run again."
    exit 1
}
Write-Host "  extract_list rows: $extractListDataRows (sprites/audio to extract)"

# Phase B: Run extraction (helper script with CSV input, StagingOnly)
Write-Host "Step 3 Phase B: Extract sprites and audio (offsets to staging)"
$extractScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\extract-sprites-to-output.ps1"
if (-not (Test-Path $extractScript)) { Write-Error "extract-sprites-to-output.ps1 not found: $extractScript"; exit 1 }
$params = @{
    ExtractListCsv = $extractListCsv
    CacheDir = $CacheDir
    OutDir = $outDirActual
    AudioOutDir = $audioOutActual
    StagingOnly = $true
}
if ($SkipExisting) { $params["SkipExisting"] = $true }
if ($Test) { $params["Test"] = $true }
if ($env:ASSET_EXTRACTION_TEMP_DIR) { $params["TempDir"] = $env:ASSET_EXTRACTION_TEMP_DIR }
if ($ThrottleLimit -gt 0) { $params["ThrottleLimit"] = $ThrottleLimit }
if ($Workers -gt 0) { $params["Workers"] = $Workers }
Push-Location $assetExtRoot
try {
    & $extractScript @params
    if ($LASTEXITCODE -ne 0) { Write-Error "Extraction failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Pop-Location }

# Phase C: Convert offsets JSONL to staging/offsets.csv (quote fields that may contain commas)
if (Test-Path $offsetsJsonl) {
    Write-Host "Step 3 Phase C: Writing offsets to staging/offsets.csv"
    "ruid,output_subdir,offset_x,offset_y" | Set-Content -Path $offsetsCsv -Encoding UTF8
    Get-Content $offsetsJsonl -Encoding UTF8 | ForEach-Object {
        $o = $_ | ConvertFrom-Json
        $sub = $o.output_subdir -replace '"', '""'; if ($sub -match '[,"\r\n]') { $sub = "`"$sub`"" }
        "$($o.ruid),$sub,$($o.offset_x),$($o.offset_y)"
    } | Add-Content -Path $offsetsCsv -Encoding UTF8
    $rows = (Get-Content $offsetsCsv | Measure-Object -Line).Lines
    Write-Host "  Offsets: $($rows - 1) rows"
} else {
    Set-Content -Path $offsetsCsv -Value "ruid,output_subdir,offset_x,offset_y" -Encoding UTF8
    Write-Host "  No offset rows (no sprites extracted or staging file missing)."
}
Write-Host "Step 3 done. Output: images, audio, $offsetsCsv"
exit 0
