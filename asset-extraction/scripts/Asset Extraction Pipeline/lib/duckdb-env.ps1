# Shared DuckDB pipeline environment: hardware detection, tool checks, temp cleanup.
# Dot-source from step scripts: . (Join-Path $PSScriptRoot "lib\duckdb-env.ps1")
# Then call Get-DuckDBEnv, Ensure-DuckDBTools, and optionally Initialize-DuckDBStaging.

$ErrorActionPreference = "Stop"

function Get-DuckDBEnv {
    # Dynamic fallbacks: use .NET when CIM unavailable (e.g. non-Windows or no WMI)
    $cores = [Math]::Max(1, [Environment]::ProcessorCount)
    $memGB = 2
    try {
        $cpu = Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue
        if ($cpu) { $c = ($cpu | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum; if ($c -gt 0) { $cores = $c } }
        $ram = (Get-CimInstance Win32_ComputerSystem -ErrorAction SilentlyContinue).TotalPhysicalMemory
        if ($ram -and $ram -gt 0) { $memGB = [Math]::Max(2, [Math]::Floor(($ram / 1GB) * 0.5)) }
    } catch { }
    return @{ Threads = $cores; MemoryLimit = "${memGB}GB" }
}

function Ensure-DuckDBTools {
    if (-not (Get-Command duckdb -ErrorAction SilentlyContinue)) {
        Write-Error "DuckDB CLI not found. Install: winget install -e --id DuckDB.cli --accept-package-agreements (or https://duckdb.org/docs/installation/)"
        exit 1
    }
    if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
        Write-Error "Node.js not found. Install: winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements (or https://nodejs.org)"
        exit 1
    }
}

function Ensure-Sqlite3 {
    if (-not (Get-Command sqlite3 -ErrorAction SilentlyContinue)) {
        Write-Error "sqlite3 CLI not found. Install: winget install -e --id SQLite.SQLite --accept-package-agreements (or https://sqlite.org/download.html)"
        exit 1
    }
}

function Initialize-DuckDBStaging {
    param([string] $StagingDir)
    $duckdbTemp = Join-Path $StagingDir "duckdb_temp"
    if (Test-Path $duckdbTemp) {
        Remove-Item -Path $duckdbTemp -Recurse -Force -ErrorAction SilentlyContinue
    }
    $null = New-Item -ItemType Directory -Path $duckdbTemp -Force
}

function Resolve-AssetExtRoot {
    # All paths relative to where the user stores the project. Prefer runner-set root; else derive from script location.
    if ($env:ASSET_EXTRACTION_ROOT -and (Test-Path -LiteralPath $env:ASSET_EXTRACTION_ROOT)) {
        return (Resolve-Path -LiteralPath $env:ASSET_EXTRACTION_ROOT).Path
    }
    $scriptRoot = $PSScriptRoot
    if ($scriptRoot -match 'Asset Extraction Pipeline\\lib$') {
        return (Resolve-Path (Join-Path $scriptRoot "..\..\..")).Path
    }
    if ($scriptRoot -match 'Asset Extraction Pipeline$') {
        return (Resolve-Path (Join-Path $scriptRoot "..\..")).Path
    }
    return (Resolve-Path (Join-Path $scriptRoot "..\..")).Path
}
