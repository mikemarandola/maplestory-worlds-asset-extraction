#Requires -Version 7
# MapleStory Worlds metadata extractor: Collect + Enrich launcher.
# All paths relative to this script's directory (where the user stores the Metadata Downloader folder). No absolute paths.
# Order: env check → deps check (offer install) → intro → Workers → Collect → Enrich → Test → Token → Invoke Python.
# Usage: .\metadata-extractor.ps1 [-FromStep 0|1|2] [-Test]
#   Output: RootDesk/MyDesk/resources.csv (or resources_test.csv in test mode). Step 0 last_pages.csv stays under Metadata Downloader (or test/ in test mode).
#   -Test: test mode → MyDesk/resources_test.csv; step 0 skips full probe (page 1 only, subcategory "all"); collect = one page per category; enrich = 50 per category.

param(
    [int]$FromStep = 0,   # 0 = run from step 0 (find last pages); 1 = start at step 1 (collect); 2 = start at step 2 (enrich only)
    [switch]$Test         # Test mode: output MyDesk/resources_test.csv; step 0 uses test/; one page per segment (collect), 50 per category (enrich)
)

$ErrorActionPreference = "Stop"
# Use PSScriptRoot so the script directory is always absolute (avoids wrong path when invoked with a relative path from another folder)
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { [System.IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path)) }
$MyDeskDirTop = Join-Path $ScriptDir "..\RootDesk\MyDesk"
$ResourcesCsv = Join-Path $MyDeskDirTop "resources.csv"
$ResourcesTestCsv = Join-Path $MyDeskDirTop "resources_test.csv"
$EnvFolder = Join-Path $ScriptDir "env"
$TokenFile = Join-Path $EnvFolder ".ifwt"
$StepsDir = Join-Path $ScriptDir "steps"
$Step0Py = Join-Path $StepsDir "0-find_last_pages.py"
$CollectPy = Join-Path $StepsDir "1-collect.py"
$EnrichPy = Join-Path $StepsDir "2-enrich.py"
$LastPagesCsv = Join-Path $ScriptDir "last_pages.csv"
$RequirementsTxt = Join-Path $ScriptDir "requirements.txt"
$TestDir = Join-Path $ScriptDir "test"

# ---------- Environment check ----------
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "PowerShell 7 or above is required. Current: $($PSVersionTable.PSVersion)"
    exit 1
}

$pythonCmd = $null
foreach ($cmd in @("python", "py")) {
    try {
        $v = & $cmd --version 2>&1
        if ($LASTEXITCODE -eq 0 -or $v -match "Python") { $pythonCmd = $cmd; break }
    } catch {}
}
if (-not $pythonCmd) {
    Write-Host "Python not found. Install Python 3 and ensure it is on your PATH (e.g. python --version or py -3 --version)."
    exit 1
}

# ---------- Dependency check (steps 0, 1, 2 + _api) ----------
# Step 0: argparse, csv, json, os, sys, time, importlib.util; _api uses urllib, optional browser_cookie3
# Step 1: argparse, csv, json, math, os, re, sys, tempfile, threading, time, collections.deque, concurrent.futures, urllib.request, urllib.error; _api
# Step 2: argparse, csv, json, os, queue, sys, tempfile, threading, time, concurrent.futures, urllib.request, urllib.error; _api
$requiredCheck = "import argparse, csv, json, os, sys, time, re, math, tempfile, threading, queue; from collections import deque; from concurrent.futures import ThreadPoolExecutor, as_completed; from urllib.request import Request, urlopen; from urllib.error import HTTPError, URLError; import importlib.util"
$requiredErr = & $pythonCmd -c $requiredCheck 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Required Python modules missing (used by steps 0, 1, 2):" -ForegroundColor Red
    Write-Host $requiredErr
    Write-Host "These are standard library modules. Ensure you are using Python 3.7+."
    exit 1
}

$optionalMissing = $false
$optionalErr = & $pythonCmd -c "import browser_cookie3" 2>&1
if ($LASTEXITCODE -ne 0) {
    $optionalMissing = $true
}

if ($optionalMissing -or -not (Test-Path $RequirementsTxt)) {
    if ($optionalMissing) {
        Write-Host ""
        Write-Host "---------- Dependencies ----------" -ForegroundColor Cyan
        Write-Host "Optional package 'browser_cookie3' is not installed (used to read _ifwt from browser)."
        if (Test-Path $RequirementsTxt) {
            Write-Host "Install all dependencies from requirements.txt? [1] Yes  [2] No (you can paste your _ifwt token when prompted) (Enter = 1):"
            $install = (Read-Host).Trim()
            if ([string]::IsNullOrWhiteSpace($install) -or $install -eq "1") {
                Write-Host "Running: $pythonCmd -m pip install -r requirements.txt"
                & $pythonCmd -m pip install -r requirements.txt 2>&1 | ForEach-Object { Write-Host $_ }
                if ($LASTEXITCODE -ne 0) {
                    Write-Host "pip install reported an error. You can still run by pasting your _ifwt token when prompted." -ForegroundColor Yellow
                } else {
                    $optionalErr2 = & $pythonCmd -c "import browser_cookie3" 2>&1
                    if ($LASTEXITCODE -eq 0) {
                        Write-Host "browser_cookie3 installed successfully." -ForegroundColor Green
                        $optionalMissing = $false
                    }
                }
            }
        } else {
            Write-Host "To install: pip install browser-cookie3"
            Write-Host "Script can still run if you paste your _ifwt token when prompted."
        }
    }
} else {
    # requirements.txt exists and browser_cookie3 is already installed; optionally verify full requirements
    Write-Host ""
    Write-Host "---------- Dependencies ----------" -ForegroundColor Cyan
    Write-Host "Checking that all packages from requirements.txt are installed..."
    $pipCheck = & $pythonCmd -m pip check 2>&1
    if ($LASTEXITCODE -ne 0 -or $pipCheck -match "missing|requires") {
        Write-Host "Some installed packages may have missing dependencies. Install/upgrade from requirements.txt? [1] Yes  [2] No (Enter = 1):"
        $install = (Read-Host).Trim()
        if ([string]::IsNullOrWhiteSpace($install) -or $install -eq "1") {
            Write-Host "Running: $pythonCmd -m pip install -r requirements.txt"
            & $pythonCmd -m pip install -r requirements.txt 2>&1 | ForEach-Object { Write-Host $_ }
        }
    } else {
        Write-Host "All dependencies OK." -ForegroundColor Green
    }
}

# ---------- Validate -FromStep ----------
if ($FromStep -lt 0 -or $FromStep -gt 2) {
    Write-Host "Invalid -FromStep. Use 0 (full), 1 (start at collect), or 2 (enrich only)."
    exit 1
}
if ($FromStep -ge 1) {
    Write-Host "Run from step $FromStep (step 0 will be skipped)." -ForegroundColor Yellow
}

# Test mode: -Test flag forces test; otherwise we prompt later
$testModeByFlag = $Test

# ---------- Intro and overwrite warning ----------
Write-Host @"

Duration: This process can take a long time—up to 8 hours or more on a high-end system, depending on options and network. You can run collect only, enrich only, or both.

Step 0 – Find last pages: Runs before collect. Finds the actual last page with data for every category and subcategory, and writes last_pages.csv. Step 1 requires this file.

Step 1 – Collect RUIDs: Fetches the list of resource IDs (RUIDs) from the MapleStory Worlds site by category and page. It writes RUID, Category, and Subcategory to resources.csv. Date, Format, and Tags are left empty for the enrich step. You can choose to skip collect, only add new RUIDs (append), or overwrite and collect everything from scratch. You can also limit which categories to collect (e.g. audioclip only).

Step 2 – Enrich: Reads resources.csv and, for each row that is missing Date, Format, or Tags, fetches that data from the API and fills it in. Enrichment never deletes or overwrites existing data—it only adds enrichment where it is missing.

Note: If you run collect (option 2 or 3), the output will overwrite resources.csv if it already exists. Back up the file if you need to keep it.

"@

# ---------- Workers ----------
Write-Host ""
Write-Host "---------- Workers ----------" -ForegroundColor Cyan
Write-Host "Warning: Running more than 8 workers may cause more failed pages, missed RUIDs, and could require running this script again to capture missing data."
$workersInput = (Read-Host "How many workers? (based on your system's core count; 0 = auto decides) (Enter = 0)").Trim()
$workersParam = if ([string]::IsNullOrWhiteSpace($workersInput)) { 0 } elseif ($workersInput -match '^\d+$') { [int]$workersInput } else { 0 }
$workersArg = if ($workersParam -eq 0) { "auto" } else { [string]$workersParam }

# ---------- Collect ----------
Write-Host ""
Write-Host "---------- Collect (Step 1) ----------" -ForegroundColor Cyan
$runCollect = $false
$collectMode = "skip"   # skip | append | overwrite | create
$categoriesArg = "0,1,3,25"   # API IDs: default all

if (-not (Test-Path $ResourcesCsv)) {
    $c = (Read-Host "resources.csv does not exist. [1] Create list (collect from scratch)  [2] Exit (Enter = 1)").Trim()
    if ($c -eq "2") {
        Write-Host "Exiting."
        exit 0
    }
    $runCollect = $true
    $collectMode = "create"
} else {
    Write-Host "Collect fetches RUIDs by category and page."
    Write-Host "Do you want to collect RUIDs? [1] Skip  [2] Only new (append)  [3] Overwrite all:"
    $c = Read-Host
    if ($c -eq "2") { $runCollect = $true; $collectMode = "append" }
    if ($c -eq "3") { $runCollect = $true; $collectMode = "overwrite" }
}

if ($runCollect) {
    $catInput = (Read-Host "Which categories to collect? (comma-separated, e.g. 1,2,3; or Enter for all). 1=Sprite 2=Audio clip 3=Animation 4=Avatar item").Trim()
    $valid = @()
    foreach ($part in ($catInput -split "[,\s]+")) {
        $p = $part.Trim()
        if ($p -eq "") { continue }
        if ($p -match '^[1-4]$') { $valid += [int]$p }
    }
    if ($valid.Count -gt 0) {
        $apiMap = @{ 1 = 0; 2 = 1; 3 = 3; 4 = 25 }
        $categoriesArg = ($valid | ForEach-Object { $apiMap[$_] }) | Sort-Object -Unique
        $categoriesArg = $categoriesArg -join ","
    }
}

# ---------- Enrich ----------
Write-Host ""
Write-Host "---------- Enrich (Step 2) ----------" -ForegroundColor Cyan
$runEnrich = $false
Write-Host "Enrichment fetches Date, Format, and Tags from the API for each RUID that is missing them. It does not delete or overwrite existing data—only fills in missing enrichment."
$e = (Read-Host "Do you want to run data enrichment? [1] Yes  [2] No (Enter = 1)").Trim()
if ($e -ne "2") { $runEnrich = $true }

# ---------- Test ----------
Write-Host ""
Write-Host "---------- Test run ----------" -ForegroundColor Cyan
if ($testModeByFlag) {
    $testMode = $true
    Write-Host "Test mode (-Test): output goes to RootDesk/MyDesk/resources_test.csv. Step 0 skips full probe (page 1 only, subcategory 'all'). Collect = one page per category from 'all'; Enrich = 50 per category." -ForegroundColor Yellow
} else {
    $testInput = (Read-Host "Do you want to run a quick test? (Output: MyDesk/resources_test.csv; step 0 skips full probe; collect = one page per category from 'all'; enrich = 50 per category.) [1] Yes  [2] No (Enter = 1)").Trim()
    $testMode = ($testInput -ne "2")
}
if ($testMode) {
    if (-not (Test-Path $MyDeskDirTop)) { New-Item -ItemType Directory -Path $MyDeskDirTop -Force | Out-Null }
    if (-not (Test-Path $TestDir)) {
        New-Item -ItemType Directory -Path $TestDir -Force | Out-Null
        Write-Host "Created test directory for step 0: $TestDir" -ForegroundColor Green
    }
}

# ---------- Token (required) ----------
Write-Host ""
Write-Host "---------- Token (required) ----------" -ForegroundColor Cyan
$token = $null
if (Test-Path $TokenFile) {
    $t = (Read-Host "Use saved token? [1] Yes  [2] No (paste new) (Enter = 1)").Trim()
    if ($t -ne "2") {
        $token = Get-Content $TokenFile -Raw
        $token = $token.Trim()
    }
}
if (-not $token) {
    Write-Host @"

To get your _ifwt token:
  1. Log in at https://maplestoryworlds.nexon.com in your browser.
  2. Open DevTools (F12) → Application (Chrome/Edge) or Storage (Firefox) → Cookies → select the site.
  3. Find the _ifwt row and copy its value (the whole string).

Example format (do not use a real token):
  ias:wt:1234567890123:1234567890@aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:AWT

"@
    do {
        $token = Read-Host "Paste your _ifwt token"
        $token = ($token -replace '^[\s"]+|\s"+$').Trim()
        if (-not $token) { Write-Host "Token cannot be empty." }
    } while (-not $token)
    Write-Host "Save token? [1] Yes  [2] No:"
    $save = Read-Host
    if ($save -eq "1") {
        if (-not (Test-Path $EnvFolder)) { New-Item -ItemType Directory -Path $EnvFolder | Out-Null }
        Set-Content -Path $TokenFile -Value $token -NoNewline
    }
}

# Set token for Python (env var; steps use --no-browser and read MSW_IFWT)
$env:MSW_IFWT = $token
# Unbuffer Python output so progress appears immediately
$env:PYTHONUNBUFFERED = "1"

# ---------- Invoke ----------
# Output location: RootDesk/MyDesk (hardcoded names resources.csv or resources_test.csv). No copy; write directly there.
if (-not (Test-Path $MyDeskDirTop)) { New-Item -ItemType Directory -Path $MyDeskDirTop -Force | Out-Null }
if ($testMode) {
    $collectOut = Join-Path $MyDeskDirTop "resources_test.csv"
    $enrichOut = Join-Path $MyDeskDirTop "resources_test.csv"
    $outCsv = $enrichOut
} else {
    $collectOut = Join-Path $MyDeskDirTop "resources.csv"
    $enrichOut = Join-Path $MyDeskDirTop "resources.csv"
    $outCsv = $enrichOut
}

function ShowErrorAndMaybeTokenPrompt {
    param([string]$Phase, [string]$ErrText)
    Write-Host "`n--- Error output ---`n$ErrText`n---"
    Write-Host "This error might be a token issue. Try with new token?"
    Write-Host "Update token? [1] Yes  [2] Exit:"
    $u = Read-Host
    if ($u -ne "1") {
        Write-Host "Exiting."
        exit 1
    }
    $script:token = Read-Host "Paste your _ifwt token"
    $script:token = ($script:token -replace '^[\s"]+|\s"+$').Trim()
    if (-not $script:token) {
        Write-Host "Token cannot be empty. Exiting."
        exit 1
    }
    Write-Host "Save token? [1] Yes  [2] No:"
    $save = Read-Host
    if ($save -eq "1") {
        if (-not (Test-Path $EnvFolder)) { New-Item -ItemType Directory -Path $EnvFolder | Out-Null }
        Set-Content -Path $TokenFile -Value $script:token -NoNewline
    }
    $env:MSW_IFWT = $script:token
}

# Overwrite: remove existing file so collect starts fresh (only the output we're writing to)
if ($runCollect -and $collectMode -eq "overwrite" -and (Test-Path $collectOut)) {
    Remove-Item $collectOut -Force
}

# Relative paths for use inside ScriptDir (when we Push-Location). Step 0 still uses test/ or . for last_pages.csv.
$testDirRel = "test"
$myDeskRel = "..\RootDesk\MyDesk"
$collectOutRel = if ($testMode) { Join-Path $myDeskRel "resources_test.csv" } else { Join-Path $myDeskRel "resources.csv" }
$enrichOutRel = $collectOutRel

# Run Python from script directory so relative paths resolve correctly
Push-Location $ScriptDir
try {
    while ($true) {
        # Test mode: remove MyDesk/resources_test.csv so collect starts fresh; step 0 last_pages stays in test/
        if ($testMode) {
            if ($FromStep -le 0 -and (Test-Path (Join-Path $testDirRel "last_pages.csv"))) { Remove-Item (Join-Path $testDirRel "last_pages.csv") -Force }
            if ($runCollect -and $FromStep -le 1 -and (Test-Path $collectOutRel)) { Remove-Item $collectOutRel -Force }
        }

        if ($runCollect -and $FromStep -le 1) {
            if ($FromStep -le 0) {
                Write-Host ""
                Write-Host "========================================" -ForegroundColor Green
                Write-Host " Step 0: Find last pages " -ForegroundColor Green
                Write-Host "========================================" -ForegroundColor Green
                Write-Host ""
                $step0Out = if ($testMode) { Join-Path $testDirRel "last_pages.csv" } else { "last_pages.csv" }
                $step0Args = @($Step0Py, "-o", $step0Out, "-v")
                if ($testMode) { $step0Args += "--test" }
                $step0Output = & $pythonCmd $step0Args 2>&1 | ForEach-Object { Write-Host $_; $_ }
                $step0Str = $step0Output | Out-String
                if ($LASTEXITCODE -ne 0) {
                    if ($step0Str -match "401|unauthorized|code.*-1|session expired|auth") {
                        Pop-Location
                        ShowErrorAndMaybeTokenPrompt "Step 0 (Find last pages)" $step0Str
                        Push-Location $ScriptDir
                        continue
                    }
                    Pop-Location
                    exit $LASTEXITCODE
                }
            }
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host " Step 1: Collect RUIDs " -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            Write-Host ""
            $collectArgs = @($CollectPy, "-o", $collectOutRel, "--categories", $categoriesArg, "--no-browser", "-v")
            if ($testMode) {
                $collectArgs += "--test"
                $collectArgs += "--last-pages"; $collectArgs += (Join-Path $testDirRel "last_pages.csv")
                $collectArgs += "--workers"; $collectArgs += "1"
            }
            if (-not $testMode -and $workersArg -ne "auto") { $collectArgs += "--workers"; $collectArgs += $workersArg }
            $collectOutput = & $pythonCmd $collectArgs 2>&1 | ForEach-Object { Write-Host $_; $_ }
            $collectStr = $collectOutput | Out-String
            if ($LASTEXITCODE -ne 0) {
                if ($collectStr -match "401|unauthorized|code.*-1|session expired|auth") {
                    Pop-Location
                    ShowErrorAndMaybeTokenPrompt "Collect" $collectStr
                    Push-Location $ScriptDir
                    continue
                }
                Pop-Location
                exit $LASTEXITCODE
            }
        }

        if ($runEnrich) {
            # Enrich input = same file as collect output (RootDesk/MyDesk)
            $enrichInputPath = $collectOutRel
            if (-not (Test-Path $enrichInputPath)) {
                Write-Host "Enrich input file not found: $enrichInputPath"
                Pop-Location
                exit 1
            }
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host " Step 2: Enrich " -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            $workersVal = if ($workersArg -eq "auto") { "4" } else { $workersArg }
            if ($testMode) {
                Write-Host "Test mode: enriching up to 50 rows per category from MyDesk/resources_test.csv." -ForegroundColor Yellow
            }
            Write-Host ""
            # Pass input as first positional. Test mode: --test => 50 per category
            $enrichArgs = @($EnrichPy, $enrichInputPath, "-o", $enrichOutRel, "--workers", $workersVal, "--no-browser", "-v")
            if ($testMode) { $enrichArgs += "--test" }
            $enrichOutput = & $pythonCmd $enrichArgs 2>&1 | ForEach-Object { Write-Host $_; $_ }
            $enrichStr = $enrichOutput | Out-String
            if ($LASTEXITCODE -ne 0) {
                if ($enrichStr -match "401|unauthorized|code.*-1|session expired|auth") {
                    Pop-Location
                    ShowErrorAndMaybeTokenPrompt "Enrich" $enrichStr
                    Push-Location $ScriptDir
                    continue
                }
                Pop-Location
                exit $LASTEXITCODE
            }
        }

        break
    }
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Done " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "Output: $outCsv"
