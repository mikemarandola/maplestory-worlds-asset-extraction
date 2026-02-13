# Step 7: Build thumbnails. Reads from final PGLite metadata dir (or staging). Calls existing build-thumbs helper.

param(
    [string] $OutDb = "",
    [string] $OutputDir = "",
    [string] $StagingDir = "",
    [string] $ImagesDir = "",
    [string] $ThumbsDir = "",
    [int] $Concurrency = 0,   # 0 = use build-thumbs default (16); from runner -Workers
    [switch] $SkipExisting,
    [switch] $Test
)

$ErrorActionPreference = "Stop"
$scriptRoot = $PSScriptRoot
. (Join-Path $scriptRoot "lib\duckdb-env.ps1")
Ensure-DuckDBTools   # node required for build-thumbs.js
$assetExtRoot = Resolve-AssetExtRoot
$scriptsDir = Join-Path $assetExtRoot "scripts"
. (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-parallelism.ps1")
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$outputDirActual = if ([string]::IsNullOrEmpty($OutputDir)) { $outputDirBase } else { $OutputDir }
$metadataDb = if ([string]::IsNullOrEmpty($OutDb)) { Join-Path $outputDirActual "metadata" } else { $OutDb }
$imagesDirActual = if ([string]::IsNullOrEmpty($ImagesDir)) { Join-Path $outputDirActual "images" } else { $ImagesDir }
$thumbsDirActual = if ([string]::IsNullOrEmpty($ThumbsDir)) { Join-Path $outputDirActual "thumbs" } else { $ThumbsDir }

if (-not (Test-Path $metadataDb)) {
    Write-Error "metadata directory not found: $metadataDb. Run Step 6 first."
    exit 1
}

$thumbsScript = Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\build-thumbs.js"
if (-not (Test-Path $thumbsScript)) { Write-Error "build-thumbs.js not found: $thumbsScript"; exit 1 }

$concurrency = if ($Concurrency -gt 0) { $Concurrency } else { $script:AssetExtractionParallelism }
Write-Host "Step 7: Build thumbnails from PGLite metadata (concurrency: $concurrency)"
$nodeArgs = @("$thumbsScript", "--db", $metadataDb, "--images-dir", $imagesDirActual, "--thumbs-dir", $thumbsDirActual, "--concurrency", $concurrency)
if ($Test) { $nodeArgs += "--test" }
if ($SkipExisting) { $nodeArgs += "--skip-existing" }
Push-Location $assetExtRoot
try {
    & node @nodeArgs
    if ($LASTEXITCODE -ne 0) { Write-Error "Step 7 failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }
} finally { Pop-Location }
Write-Host "Step 7 done. Thumbnails: $thumbsDirActual"
exit 0
