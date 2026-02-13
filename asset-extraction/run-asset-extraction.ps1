# Run the asset extraction pipeline (7 steps). Same prereqs as run-all-steps; uses scripts/Asset Extraction Pipeline/.
# All paths are relative to the asset-extraction root (script directory or -AssetExtractionRoot). Run from any CWD.
# -Test: uses same catalog (RootDesk/MyDesk/resources.csv); ALL step outputs go to output-test/ and temp-test/ (never overwrites output/ or temp/).
# Usage: .\run-asset-extraction.ps1 -Test -NonInteractive
#        .\run-asset-extraction.ps1 -AssetExtractionRoot "C:\path\to\asset-extraction"
#        .\run-asset-extraction.ps1 -SkipExisting -Workers 4

param(
    [string] $AssetExtractionRoot = "",  # default: directory containing this script; set to run from another location
    [switch] $Test,
    [switch] $SkipExisting,
    [int] $Workers = 0,
    [int] $StartAtStep = 1,
    [int] $OnlyStep = 0,
    [switch] $NonInteractive,
    [ValidateSet("sqlite", "csv", "both", "")]
    [string] $OutputFormat = ""  # default: prompt (or sqlite when -NonInteractive). sqlite = metadata.db; csv = final_*.csv only; both = both
)

$ErrorActionPreference = "Stop"

# --- Resolve asset-extraction root: all paths are relative to this folder (where the user stores the project) ---
$HomeDir = if ([string]::IsNullOrWhiteSpace($AssetExtractionRoot)) { $PSScriptRoot } else { [System.IO.Path]::GetFullPath($AssetExtractionRoot) }
if (-not (Test-Path $HomeDir)) {
    Write-Error "Asset-extraction root not found: $HomeDir"
    exit 1
}

# If key paths missing and interactive, prompt for root
$scriptsDir = Join-Path $HomeDir "scripts"
$pipelineDir = Join-Path $scriptsDir "Asset Extraction Pipeline"
$packageJson = Join-Path $HomeDir "package.json"
if ((-not (Test-Path $pipelineDir) -or -not (Test-Path $packageJson)) -and -not $NonInteractive) {
    Write-Host "Asset-extraction root appears invalid (missing scripts/Asset Extraction Pipeline or package.json)."
    Write-Host "Current root: $HomeDir"
    $prompt = Read-Host "Enter full path to asset-extraction folder (or press Enter to exit)"
    if (-not [string]::IsNullOrWhiteSpace($prompt)) {
        $HomeDir = [System.IO.Path]::GetFullPath($prompt.Trim().Trim('"'))
        $scriptsDir = Join-Path $HomeDir "scripts"
        $pipelineDir = Join-Path $scriptsDir "Asset Extraction Pipeline"
        $packageJson = Join-Path $HomeDir "package.json"
    }
    if (-not (Test-Path $pipelineDir) -or -not (Test-Path $packageJson)) {
        Write-Error "Valid asset-extraction root not found. Need folder containing scripts/Asset Extraction Pipeline and package.json."
        exit 1
    }
}

if (-not (Test-Path $pipelineDir)) {
    Write-Error "Asset Extraction Pipeline folder not found: $pipelineDir"
    exit 1
}

$wingetAvailable = Get-Command winget -ErrorAction SilentlyContinue

function Invoke-InstallPrompt {
    param([string]$Name, [string]$InstallCommand, [scriptblock]$InstallScript = $null, [string]$ManualUrl = "")
    if ($NonInteractive) {
        # NonInteractive: try to install automatically if winget is available
        if ($InstallCommand -match 'winget' -and $wingetAvailable) {
            Write-Host "$Name was not found. Attempting install: $InstallCommand" -ForegroundColor Yellow
            Invoke-Expression $InstallCommand
            if ($LASTEXITCODE -eq 0) {
                Refresh-EnvPath
                return $true
            }
        }
        return $false
    }
    Write-Host "$Name was not found."
    if ($ManualUrl) { Write-Host "Manual: $ManualUrl" }
    if (-not $wingetAvailable -and $InstallCommand -match 'winget') {
        Write-Host "winget is not available. Install the dependency manually, then run this script again."
        return $false
    }
    $r = Read-Host "Install now? (Y/n)"
    if ($r -match '^[nN]$') { return $false }
    if ($InstallScript) {
        & $InstallScript
        return $LASTEXITCODE -eq 0
    }
    Write-Host "Running: $InstallCommand"
    Invoke-Expression $InstallCommand
    return $LASTEXITCODE -eq 0
}

function Refresh-EnvPath {
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

# Required CLIs (all installed via winget when missing; prompt or auto-install):
#   1. PowerShell 7+     winget: Microsoft.PowerShell
#   2. Node.js (LTS)    winget: OpenJS.NodeJS.LTS  (includes npm)
#   3. DuckDB CLI       winget: DuckDB.cli
#   4. sqlite3 CLI      winget: SQLite.SQLite
#   5. npm packages     node_modules via npm install (sharp, parse-dds, decode-dxt, papaparse, better-sqlite3)

# --- 1. PowerShell 7+ (required) ---
$psMajor = $PSVersionTable.PSVersion.Major
if ($psMajor -lt 7) {
    $installed = Invoke-InstallPrompt -Name "PowerShell 7" -InstallCommand "winget install -e --id Microsoft.PowerShell --accept-package-agreements" -ManualUrl "https://github.com/PowerShell/PowerShell#get-powershell"
    if (-not $installed) {
        Write-Error "This script requires PowerShell 7 or later. You are running $psMajor. Install and run: pwsh -File run-asset-extraction.ps1"
        exit 1
    }
    Write-Host "PowerShell 7 was installed. Please close this window, open a new terminal, and run: pwsh -File run-asset-extraction.ps1"
    exit 0
}

# --- 2. Node.js (required) ---
$nodeExe = Get-Command node -ErrorAction SilentlyContinue
if (-not $nodeExe) {
    $installed = Invoke-InstallPrompt -Name "Node.js" -InstallCommand "winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements" -ManualUrl "https://nodejs.org"
    if (-not $installed) {
        Write-Error "Node.js is required. Install from https://nodejs.org and ensure 'node' is in PATH."
        exit 1
    }
    Refresh-EnvPath
    $nodeExe = Get-Command node -ErrorAction SilentlyContinue
    if (-not $nodeExe) {
        if ($NonInteractive) {
            Write-Error "Node.js was installed but is not yet in PATH. Close and reopen the terminal, then run this script again."
        } else {
            Write-Host "Node.js was installed. If 'node' is not found, close and reopen the terminal, then run this script again."
        }
        exit ($NonInteractive ? 1 : 0)
    }
}

# --- 2b. npm (required; usually with Node.js LTS) ---
$npmExe = Get-Command npm -ErrorAction SilentlyContinue
if (-not $npmExe) {
    Refresh-EnvPath
    $npmExe = Get-Command npm -ErrorAction SilentlyContinue
}
if (-not $npmExe) {
    Write-Error "npm was not found. It normally comes with Node.js. Install Node.js LTS: winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements (then close and reopen the terminal)."
    exit 1
}

# --- 3. DuckDB CLI (required for steps 1-6) ---
$duckdbExe = Get-Command duckdb -ErrorAction SilentlyContinue
if (-not $duckdbExe) {
    $installed = Invoke-InstallPrompt -Name "DuckDB CLI" -InstallCommand "winget install -e --id DuckDB.cli --accept-package-agreements" -ManualUrl "https://duckdb.org/docs/installation/"
    if (-not $installed) {
        Write-Error "DuckDB CLI is required. Install: winget install DuckDB.cli (or https://duckdb.org/docs/installation/)"
        exit 1
    }
    Refresh-EnvPath
    $duckdbExe = Get-Command duckdb -ErrorAction SilentlyContinue
    if (-not $duckdbExe) {
        if ($NonInteractive) {
            Write-Error "DuckDB CLI was installed but is not yet in PATH. Close and reopen the terminal, then run this script again."
        } else {
            Write-Host "DuckDB was installed. If 'duckdb' is not found, close and reopen the terminal, then run this script again."
        }
        exit ($NonInteractive ? 1 : 0)
    }
}

# --- 4. sqlite3 CLI (required for step 6) ---
$sqlite3Exe = Get-Command sqlite3 -ErrorAction SilentlyContinue
if (-not $sqlite3Exe) {
    $installed = Invoke-InstallPrompt -Name "sqlite3 CLI" -InstallCommand "winget install -e --id SQLite.SQLite --accept-package-agreements" -ManualUrl "https://sqlite.org/download.html"
    if (-not $installed) {
        Write-Error "sqlite3 CLI is required for step 6. Install: winget install SQLite.SQLite or https://sqlite.org/download.html"
        exit 1
    }
    Refresh-EnvPath
    $sqlite3Exe = Get-Command sqlite3 -ErrorAction SilentlyContinue
    if (-not $sqlite3Exe) {
        if ($NonInteractive) {
            Write-Error "sqlite3 CLI was installed but is not yet in PATH. Close and reopen the terminal, then run this script again."
        } else {
            Write-Host "SQLite was installed. If 'sqlite3' is not found, close and reopen the terminal, then run this script again."
        }
        exit ($NonInteractive ? 1 : 0)
    }
}

# --- 5. package.json and npm dependencies ---
if (-not (Test-Path $packageJson)) {
    Write-Error "package.json not found at: $packageJson. Run from asset-extraction root or pass -AssetExtractionRoot."
    exit 1
}
$nodeModules = Join-Path $HomeDir "node_modules"
$keyDeps = @("sharp", "parse-dds", "decode-dxt", "papaparse", "better-sqlite3")
$missingDeps = @($keyDeps | Where-Object { -not (Test-Path (Join-Path $nodeModules $_)) })
if ($missingDeps.Count -gt 0) {
    $doInstall = $false
    if ($NonInteractive) {
        Write-Host "npm dependencies missing: $($missingDeps -join ', '). Running npm install..." -ForegroundColor Yellow
        $doInstall = $true
    } else {
        Write-Host "npm dependencies missing: $($missingDeps -join ', ')."
        $r = Read-Host "Run 'npm install' now? (Y/n)"
        $doInstall = ($r -notmatch '^[nN]$')
    }
    if (-not $doInstall) {
        Write-Error "From asset-extraction root run: npm install"
        exit 1
    }
    Push-Location $HomeDir
    try {
        & npm install
        if ($LASTEXITCODE -ne 0) {
            Write-Error "npm install failed (exit $LASTEXITCODE)."
            exit $LASTEXITCODE
        }
    } finally { Pop-Location }
    $missingDeps = @($keyDeps | Where-Object { -not (Test-Path (Join-Path $nodeModules $_)) })
    if ($missingDeps.Count -gt 0) {
        Write-Error "After npm install, some deps are still missing: $($missingDeps -join ', '). Check package.json and run npm install again."
        exit 1
    }
    Write-Host "npm install completed."
}

$logsDir = Join-Path $HomeDir "logs"
if (-not (Test-Path $logsDir)) { New-Item -ItemType Directory -Path $logsDir -Force | Out-Null }
$logTimestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$pipelineLogPath = Join-Path $logsDir "asset-extraction-$logTimestamp.log"

# Step scripts resolve paths from this root when set
$env:ASSET_EXTRACTION_ROOT = $HomeDir

# Test mode: use output-test and temp-test only — NEVER overwrite actual output
if ($Test) {
    $env:ASSET_EXTRACTION_TEST_MODE = "1"
    $env:ASSET_EXTRACTION_OUTPUT_DIR = (Join-Path $HomeDir "output-test")
    $env:ASSET_EXTRACTION_TEMP_DIR = (Join-Path $HomeDir "temp-test")
    $testOutputDir = $env:ASSET_EXTRACTION_OUTPUT_DIR
    $testStagingDir = Join-Path $testOutputDir "staging"
    $testTempDir = $env:ASSET_EXTRACTION_TEMP_DIR
    if (-not (Test-Path $testOutputDir)) { New-Item -ItemType Directory -Path $testOutputDir -Force | Out-Null }
    if (-not (Test-Path $testStagingDir)) { New-Item -ItemType Directory -Path $testStagingDir -Force | Out-Null }
    if (-not (Test-Path $testTempDir)) { New-Item -ItemType Directory -Path $testTempDir -Force | Out-Null }
    Write-Host "Test mode: normal input; all step outputs -> output-test/ and temp-test/ (main pipeline output unchanged)"
} else {
    $env:ASSET_EXTRACTION_TEST_MODE = $null
    $env:ASSET_EXTRACTION_OUTPUT_DIR = $null
    $env:ASSET_EXTRACTION_TEMP_DIR = $null
}

. (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-parallelism.ps1")
. (Join-Path $scriptsDir "Asset Extraction Pipeline\helper-scripts\_get-msw-cache-dir.ps1")
$effectiveWorkers = if ($Workers -ne 0) { $Workers } else { $script:AssetExtractionParallelism }

$outputDir = Join-Path $HomeDir "output"
$stagingDir = Join-Path $outputDir "staging"
if (-not (Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir -Force | Out-Null }
# Catalog CSV from RootDesk/MyDesk. Hardcoded names: test mode tries resources_test.csv then resources.csv; non-test only resources.csv.
$myDeskDir = Join-Path $HomeDir "..\RootDesk\MyDesk"
$resourcesCsv = Join-Path $myDeskDir "resources.csv"
$resourcesTestCsv = Join-Path $myDeskDir "resources_test.csv"
if ($Test) {
    $chosenCsv = if (Test-Path $resourcesTestCsv) { $resourcesTestCsv } elseif (Test-Path $resourcesCsv) { $resourcesCsv } else { $null }
    if (-not $chosenCsv -and $StartAtStep -le 1) {
        Write-Error "Run Step 1 in test mode first, or ensure RootDesk/MyDesk/resources_test.csv or resources.csv exists."
        exit 1
    }
} else {
    $chosenCsv = if (Test-Path $resourcesCsv) { $resourcesCsv } else { $null }
    if (-not $chosenCsv -and $StartAtStep -le 1) {
        Write-Error "Run Step 1 first. Catalog not found: RootDesk/MyDesk/resources.csv"
        exit 1
    }
}

# Final output format: SQLite DB or CSV per table (prompt unless -OutputFormat or -NonInteractive)
if ([string]::IsNullOrWhiteSpace($OutputFormat)) {
    if ($NonInteractive) {
        $OutputFormat = "sqlite"
    } else {
        Write-Host "Final output format:"
        Write-Host "  1 = SQLite DB (output/metadata.db)"
        Write-Host "  2 = CSV per table (output/staging/final_*.csv only; no metadata.db)"
        Write-Host "  3 = Both (metadata.db and final_*.csv)"
        $r = Read-Host "Choice (1, 2, or 3) [1]"
        if ([string]::IsNullOrWhiteSpace($r) -or $r -eq "1") { $OutputFormat = "sqlite" }
        elseif ($r -eq "2") { $OutputFormat = "csv" }
        elseif ($r -eq "3") { $OutputFormat = "both" }
        else { $OutputFormat = "sqlite" }
    }
}
$env:ASSET_EXTRACTION_OUTPUT_FORMAT = $OutputFormat

# FTS5 trigram prompt: fast wildcard tag search (only relevant when building a DB)
$enableFts5 = $false
if ($OutputFormat -ne "csv") {
    if ($NonInteractive) {
        $enableFts5 = $true
    } else {
        Write-Host ""
        Write-Host "Tag search mode (for the metadata DB):"
        Write-Host "  1 = Prefix only  (e.g. 'sword...' — fast with standard indexes, no extra DB size)"
        Write-Host "  2 = Full wildcard (e.g. '...sword...' — uses FTS5 trigram index; adds ~1-3 GB to DB)"
        $r = Read-Host "Choice (1 or 2) [2]"
        $enableFts5 = ($r -ne "1")
    }
}
$env:ASSET_EXTRACTION_FTS5_TRIGRAM = if ($enableFts5) { "1" } else { "0" }

# Confirm before running unless -NonInteractive (avoids accidentally overwriting output/ on normal run)
if (-not $NonInteractive) {
    if ($Test) {
        $r = Read-Host "Run in TEST mode (output-test/ and temp-test/ only; output/ and temp/ unchanged). Continue? (Y/n)"
        if ($r -match '^[nN]$') { exit 0 }
    } else {
        $r = Read-Host "This will write to output/ and temp/. Continue? (y/N)"
        if ($r -notmatch '^[yY]') { exit 0 }
    }
}

$runOnlyOneStep = ($OnlyStep -ge 1 -and $OnlyStep -le 7)
if ($runOnlyOneStep) { $StartAtStep = $OnlyStep }

Write-Host "Asset extraction pipeline (7 steps). Home: $HomeDir"
$fts5Label = if ($enableFts5) { "yes" } else { "no" }
Write-Host "Test=$Test SkipExisting=$SkipExisting Workers=$effectiveWorkers OutputFormat=$OutputFormat Fts5Trigram=$fts5Label"
if ($runOnlyOneStep) { Write-Host "OnlyStep: $OnlyStep" }
elseif ($StartAtStep -gt 1) { Write-Host "StartAtStep: $StartAtStep" }
if ($chosenCsv) { Write-Host "Catalog CSV: $([System.IO.Path]::GetFileName($chosenCsv))" }
Write-Host "Log: $pipelineLogPath"
Write-Host ""

$commonArgs = @{ Workers = $effectiveWorkers; ThrottleLimit = $effectiveWorkers; Concurrency = $effectiveWorkers }
if ($SkipExisting) { $commonArgs["SkipExisting"] = $true }
if ($Test) { $commonArgs["Test"] = $true }

$allSteps = @(
    @{ N = 1; Name = "Build catalog";           Script = "01-build-catalog.ps1";       Extra = @{} },
    @{ N = 2; Name = "Enrich with cache";       Script = "02-enrich-catalog.ps1";      Extra = @{} },
    @{ N = 3; Name = "Extract sprites + audio";  Script = "03-extract-sprites-audio.ps1"; Extra = @{} },
    @{ N = 4; Name = "Build enc map";           Script = "04-build-enc-map.ps1";      Extra = @{} },
    @{ N = 5; Name = "Extract clip frames";     Script = "05-extract-clip-frames.ps1"; Extra = @{} },
    @{ N = 6; Name = "Build final DB";         Script = "06-build-final-db.ps1";      Extra = @{} },
    @{ N = 7; Name = "Build thumbnails";       Script = "07-build-thumbs.ps1";        Extra = @{} }
)

$stepsToRun = if ($runOnlyOneStep) {
    @($allSteps | Where-Object { $_.N -eq $OnlyStep })
} else {
    @($allSteps | Where-Object { $_.N -ge $StartAtStep })
}
if ($stepsToRun.Count -eq 0) {
    Write-Error "No steps to run. StartAtStep=$StartAtStep OnlyStep=$OnlyStep (use 1-7)."
    exit 1
}

$header = @"
================================================================================
Asset extraction pipeline (7 steps) - $logTimestamp
Started: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ss')
Test=$Test SkipExisting=$SkipExisting Workers=$effectiveWorkers Fts5Trigram=$enableFts5
StartAtStep=$StartAtStep OnlyStep=$OnlyStep (steps: $($stepsToRun.Count))
================================================================================

"@
Set-Content -Path $pipelineLogPath -Value $header -Encoding UTF8

$stepIndex = 0
foreach ($s in $stepsToRun) {
    $scriptPath = Join-Path $pipelineDir $s.Script
    if (-not (Test-Path $scriptPath)) {
        Write-Error "Script not found: $scriptPath"
        exit 1
    }
    $stepNumStr = $s.N.ToString('00')
    $stepLogPath = Join-Path $logsDir "step$stepNumStr-$logTimestamp.log"
    Write-Host "--- Step $($s.N): $($s.Name) ---"
    $stepLogHeader = "=== Step $($s.N): $($s.Name) === $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ss')`n"
    Add-Content -Path $pipelineLogPath -Value "`n$stepLogHeader" -Encoding UTF8 -NoNewline
    Set-Content -Path $stepLogPath -Value $stepLogHeader -Encoding UTF8 -NoNewline

    # Step 7 requires metadata.db; skip only when output format is CSV (not sqlite or both)
    if ($s.N -eq 7 -and $OutputFormat -eq "csv") {
        Write-Host "Step 7 skipped (output format is CSV per table; thumbnails require metadata.db)."
        $stepNumStr = $s.N.ToString('00')
        $stepLogPath = Join-Path $logsDir "step$stepNumStr-$logTimestamp.log"
        $skipMsg = "=== Step 7: Build thumbnails === (skipped; OutputFormat=csv)`n"
        Set-Content -Path $stepLogPath -Value $skipMsg -Encoding UTF8 -NoNewline
        Add-Content -Path $pipelineLogPath -Value "`n$skipMsg" -Encoding UTF8
        continue
    }

    $allParams = @{}
    foreach ($k in $commonArgs.Keys) { $allParams[$k] = $commonArgs[$k] }
    foreach ($k in $s.Extra.Keys) { $allParams[$k] = $s.Extra[$k] }
    if ($s.N -eq 1 -and $chosenCsv) { $allParams["CsvPath"] = $chosenCsv }
    if ($s.N -eq 6) { $allParams["OutputFormat"] = $OutputFormat; if ($enableFts5) { $allParams["EnableFts5"] = $true } }
    # When not Test, point steps 6 and 7 at real output; when Test they use env ASSET_EXTRACTION_OUTPUT_DIR
    if (-not $Test) {
        if ($s.N -eq 6) { $allParams["OutDb"] = Join-Path $HomeDir "output\metadata.db" }
        if ($s.N -eq 7) { $allParams["OutDb"] = Join-Path $HomeDir "output\metadata.db" }
    }

    Push-Location $HomeDir
    try {
        & $scriptPath @allParams 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() } else { $_ }
        } | Tee-Object -FilePath $pipelineLogPath -Append | Tee-Object -FilePath $stepLogPath -Append
        $exitLine = "`n[Step $($s.N) exit code: $LASTEXITCODE]`n"
        Add-Content -Path $stepLogPath -Value $exitLine -Encoding UTF8 -NoNewline
        Add-Content -Path $pipelineLogPath -Value $exitLine -Encoding UTF8 -NoNewline
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[PIPELINE] Step $($s.N) failed (exit $LASTEXITCODE)" -ForegroundColor Red
            Write-Host "Per-step log: $stepLogPath"
            Pop-Location
            exit $LASTEXITCODE
        }
    } catch {
        Pop-Location
        Add-Content -Path $pipelineLogPath -Value "Exception: $($_.Exception.Message)" -Encoding UTF8
        throw
    }
    Pop-Location
    Write-Host ""
    $stepIndex++
}

Add-Content -Path $pipelineLogPath -Value "`n================================================================================`nCompleted: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ss')`n================================================================================`n" -Encoding UTF8
Write-Host "All $($stepsToRun.Count) step(s) completed. Log: $pipelineLogPath"

# Clean up temp folder used for this run (intermediate files; staging/output are kept)
$tempDirToClean = if ($Test) { Join-Path $HomeDir "temp-test" } else { Join-Path $HomeDir "temp" }
if (Test-Path $tempDirToClean) {
    Get-ChildItem -Path $tempDirToClean -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "Temp cleaned: $tempDirToClean"
}
exit 0
