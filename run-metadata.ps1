#Requires -Version 7
# Step 1: Metadata extraction. Run from the repo root (the folder that contains "Metadata Downloader").
# Prompts whether to run in test mode. All paths are relative to this repo.
# Usage: .\run-metadata.ps1

$ErrorActionPreference = "Stop"
$repoRoot = $PSScriptRoot
$scriptPath = Join-Path $repoRoot "Metadata Downloader\metadata-extractor.ps1"
if (-not (Test-Path $scriptPath)) {
    Write-Error "Not found: $scriptPath. Run this script from the repo root (the folder that contains 'Metadata Downloader')."
    exit 1
}

Write-Host "Step 1: Metadata extraction" -ForegroundColor Cyan
$r = Read-Host "Run in test mode? (catalog → RootDesk/MyDesk/resources_test.csv; step 0 data in Metadata Downloader/test/) [y/N]"
$test = $r -match '^[yY]'
if ($test) {
    & $scriptPath -Test
} else {
    & $scriptPath
}
exit $LASTEXITCODE
