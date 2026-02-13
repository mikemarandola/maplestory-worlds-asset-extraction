#Requires -Version 7
# Step 3: Asset extraction pipeline. Run from the repo root (the folder that contains "asset-extraction").
# Prompts whether to run in test mode. In test mode, prompts to install npm dependencies if missing.
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

# In test mode we pass -NonInteractive, so the pipeline won't prompt for npm install. Check and prompt here instead.
$nodeModules = Join-Path $assetExtDir "node_modules"
$keyDeps = @("sharp", "parse-dds", "decode-dxt", "papaparse", "better-sqlite3")
$missingDeps = @($keyDeps | Where-Object { -not (Test-Path (Join-Path $nodeModules $_)) })
if ($missingDeps.Count -gt 0) {
    Write-Host "npm dependencies missing: $($missingDeps -join ', ')." -ForegroundColor Yellow
    $install = Read-Host "Run 'npm install' in asset-extraction now? (Y/n)"
    if ($install -notmatch '^[nN]$') {
        Push-Location $assetExtDir
        try {
            & npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Error "npm install failed (exit $LASTEXITCODE)."
                exit $LASTEXITCODE
            }
            Write-Host "npm install completed." -ForegroundColor Green
        } finally { Pop-Location }
    } else {
        Write-Error "From asset-extraction folder run: npm install"
        exit 1
    }
}

if ($test) {
    & $scriptPath -Test -NonInteractive
} else {
    & $scriptPath
}
exit $LASTEXITCODE
