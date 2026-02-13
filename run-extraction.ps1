#Requires -Version 7
# Step 3: Asset extraction pipeline. Run from the repo root (the folder that contains "asset-extraction").
# Prompts whether to run in test mode, then delegates to run-asset-extraction.ps1 which handles all
# remaining prompts (prerequisites, output format, trigram search, etc.).
# Usage: .\run-extraction.ps1

$ErrorActionPreference = "Stop"
$repoRoot = $PSScriptRoot
$assetExtDir = Join-Path $repoRoot "asset-extraction"
$scriptPath = Join-Path $assetExtDir "run-asset-extraction.ps1"
if (-not (Test-Path $scriptPath)) {
    Write-Error "Not found: $scriptPath. Run this script from the repo root (the folder that contains 'asset-extraction')."
    exit 1
}

Write-Host "Step 3: Asset extraction pipeline" -ForegroundColor Cyan
$r = Read-Host "Run in test mode? (outputs in asset-extraction/output-test/; does not touch real output) [y/N]"
$test = $r -match '^[yY]'

if ($test) {
    & $scriptPath -Test
} else {
    & $scriptPath
}
exit $LASTEXITCODE
