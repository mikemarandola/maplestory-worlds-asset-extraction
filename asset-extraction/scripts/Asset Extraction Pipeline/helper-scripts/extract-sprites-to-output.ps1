# Helper: Extract sprites (images) and audio from the cache. Used by DuckDB pipeline step 3 and (when run from archive) legacy step 4.
# One output PNG per image RUID (dedupe by ruid, output_subdir). Sprites -> dds-to-png-batch.js; damageskin/avataritem/atlas -> extract-image-batch.js; audioclips -> OGG.
# DB path: reads catalog_enriched from output/metadata.db; writes offset_x, offset_y back to DB. CSV path (DuckDB): -ExtractListCsv + -StagingOnly.

param(
    [string] $DbPath = "",        # default: output/metadata.db (required unless ExtractListCsv is set)
    [string] $ExtractListCsv = "", # DuckDB pipeline: use this CSV instead of DB stream (then CacheDir required)
    [string] $CacheDir = "",     # default: current user's MSW cache (see _get-msw-cache-dir.ps1)
    [string] $OutDir = "",       # default: asset-extraction/output/images
    [string] $AudioOutDir = "", # default: asset-extraction/output/audio; audioclips -> <Category>/<Subcategory>/<ruid>.ogg
    [string[]] $AssetTypes = @(), # only extract these types: sprite, audioclip. If empty, extract both (or use -AudioOnly for audio only).
    [switch] $SkipExisting,
    [switch] $AudioOnly,         # only extract audio (skip sprites); same as -AssetTypes audioclip
    [switch] $Test,              # limit to first 50 sprites and 50 audio (for quick verification)
    [int] $ProgressEvery = 500,
    [int] $BatchSize = 0,         # 0 = from CPU count (50–200); sprites per Node process
    [int] $ThrottleLimit = 0,    # 0 = default (half of logical cores); 1 = sequential; >1 = parallel batches
    [int] $Workers = 0,          # override: if set, used instead of ThrottleLimit (single worker flag for pipeline)
    [switch] $StagingOnly,       # DuckDB/new-pipeline: write offsets to temp/offsets-staging.jsonl only; do not flush to DB
    [string] $TempDir = ""       # when set (e.g. test mode), write offsets-staging.jsonl here instead of asset-extraction/temp
)

$ErrorActionPreference = "Stop"
# Paths relative to where the user stores the project. This script lives in helper-scripts/; asset-extraction root = three levels up.
$scriptRoot = $PSScriptRoot
$assetExtRoot = (Resolve-Path (Join-Path $scriptRoot "..\..\..")).Path
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Error "Node.js not found. Run the pipeline from run-asset-extraction.ps1 (which checks/installs dependencies), or install: winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements"
    exit 1
}
. (Join-Path $scriptRoot "_get-parallelism.ps1")
. (Join-Path $scriptRoot "_get-msw-cache-dir.ps1")
$outputDirBase = if ($env:ASSET_EXTRACTION_OUTPUT_DIR) { $env:ASSET_EXTRACTION_OUTPUT_DIR } else { Join-Path $assetExtRoot "output" }
$metadataDb = if ([string]::IsNullOrEmpty($DbPath)) { Join-Path $outputDirBase "metadata.db" } else { $DbPath }
$useCsvStream = -not [string]::IsNullOrEmpty($ExtractListCsv)
if (-not $useCsvStream -and -not (Test-Path $metadataDb)) { Write-Error "metadata.db not found: $metadataDb (run steps 1 and 2 first, or pass -ExtractListCsv)."; exit 1 }
if ([string]::IsNullOrEmpty($CacheDir)) { $CacheDir = $MSWCacheDir }
if ([string]::IsNullOrEmpty($OutDir)) {
    $OutDir = Join-Path $outputDirBase "images"
}
if ([string]::IsNullOrEmpty($AudioOutDir)) {
    $AudioOutDir = Join-Path $outputDirBase "audio"
}
$OutDir = [System.IO.Path]::GetFullPath($OutDir)
$AudioOutDir = [System.IO.Path]::GetFullPath($AudioOutDir)
$tempDir = if (-not [string]::IsNullOrEmpty($TempDir)) { $TempDir } elseif ($env:ASSET_EXTRACTION_TEMP_DIR) { $env:ASSET_EXTRACTION_TEMP_DIR } else { Join-Path $assetExtRoot "temp" }
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
if (-not (Test-Path $AudioOutDir)) { New-Item -ItemType Directory -Path $AudioOutDir -Force | Out-Null }
if (-not (Test-Path $tempDir)) { New-Item -ItemType Directory -Path $tempDir -Force | Out-Null }
$helperDir = Join-Path $assetExtRoot "scripts\Asset Extraction Pipeline\helper-scripts"
$streamCatalogScript = Join-Path $helperDir "stream-catalog-enriched-for-extract.js"
$updateOffsetsScript = Join-Path $helperDir "update-catalog-offsets.js"
$cacheRoot = if (Test-Path $CacheDir) { (Resolve-Path $CacheDir).Path.TrimEnd('\', '/') } else { $null }
if (-not $cacheRoot) { Write-Error "Cache dir not found: $CacheDir"; exit 1 }
$BatchSize = if ($BatchSize -gt 0) { $BatchSize } else { [Math]::Max(50, [Math]::Min(200, 25 * [Math]::Max(1, [Environment]::ProcessorCount))) }
Write-Host "DB: $metadataDb. Cache root: $cacheRoot"
$batchScript = Join-Path $helperDir "dds-to-png-batch.js"
$otherImageBatchScript = Join-Path $helperDir "extract-image-batch.js"
$appDir = $assetExtRoot
if (-not (Test-Path $batchScript)) { Write-Error "Batch script not found: $batchScript (need dds-to-png-batch.js)"; exit 1 }
if (-not (Test-Path $otherImageBatchScript)) { Write-Error "extract-image-batch.js not found: $otherImageBatchScript"; exit 1 }
# streamCatalogScript and updateOffsetsScript are only required when using DB stream / flush (not for DuckDB CSV + StagingOnly); checked where used.

# Resolve which asset types to extract. Image types: sprite, damageskin, avataritem, atlas. Audio: audioclip.
$validTypes = @("sprite", "audioclip", "damageskin", "avataritem", "atlas")
if ($AudioOnly) {
    $extractTypes = @("audioclip")
    Write-Host "Asset types: audioclip only (-AudioOnly)."
} elseif ($AssetTypes -and $AssetTypes.Count -gt 0) {
    $extractTypes = @($AssetTypes | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ -in $validTypes } | Select-Object -Unique)
    if ($extractTypes.Count -eq 0) {
        Write-Error "AssetTypes must be one or more of: $($validTypes -join ', '). Got: $($AssetTypes -join ', ')."
        exit 1
    }
    Write-Host "Asset types: $($extractTypes -join ', ')."
} else {
    $extractTypes = @("sprite", "audioclip", "damageskin", "avataritem", "atlas")
}
$doSprites = $extractTypes -contains "sprite"
$doAudio = $extractTypes -contains "audioclip"
$doDamageskin = $extractTypes -contains "damageskin"
$doAvataritem = $extractTypes -contains "avataritem"
$doAtlas = $extractTypes -contains "atlas"

$throttle = if ($Workers -gt 0) { $Workers } elseif ($ThrottleLimit -gt 0) { $ThrottleLimit } else { $AssetExtractionParallelism }

# --- Dynamic work-list chunk size from available RAM ---
function Get-WorkListChunkSize {
    param([int] $BatchSize, [int] $Throttle)
    $bytesPerItem = 500   # estimate: two path strings + object overhead
    $reserveSystemBytes = 2 * 1024 * 1024 * 1024   # 2 GB for OS and general use
    $reserveNodePerWorkerBytes = 512 * 1024 * 1024   # 512 MB per concurrent Node process
    $reserveBytes = $reserveSystemBytes + ($Throttle * $reserveNodePerWorkerBytes)
    try {
        $os = Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop
        $freeKb = [long]$os.FreePhysicalMemory
        $totalKb = [long]$os.TotalVisibleMemorySize
        if ($freeKb -le 0 -or $totalKb -le 0) { return [Math]::Max($BatchSize * 2, 50000) }
        $freeBytes = $freeKb * 1024
        $available = [Math]::Max(0, $freeBytes - $reserveBytes)
        $chunk = [int]($available / $bytesPerItem)
        $chunk = [Math]::Max($BatchSize, [Math]::Min(500000, $chunk))
        return $chunk
    } catch {
        # Fallback when CIM unavailable: scale by CPU count so single-core gets smaller chunk
        $cpu = [Math]::Max(1, [Environment]::ProcessorCount)
        return [Math]::Max($BatchSize * 2, [Math]::Min(50000, 5000 * $cpu))
    }
}
$workListChunkSize = Get-WorkListChunkSize -BatchSize $BatchSize -Throttle $throttle
Write-Host "Extract: Work list chunk size: $workListChunkSize (from available RAM; process in chunks to limit memory)."
[Console]::Out.Flush()

# --- Stream CSV: parse one line (handles quoted fields with commas) ---
function Parse-CsvLine {
    param([string] $Line)
    $fields = [System.Collections.Generic.List[string]]::new()
    $i = 0
    $len = $Line.Length
    while ($i -lt $len) {
        if ($Line[$i] -eq '"') {
            $i++
            $sb = [System.Text.StringBuilder]::new()
            while ($i -lt $len) {
                $c = $Line[$i]
                if ($c -eq '"') {
                    $i++
                    if ($i -lt $len -and $Line[$i] -eq '"') { [void]$sb.Append('"'); $i++ }
                    else { break }
                } else {
                    [void]$sb.Append($c)
                    $i++
                }
            }
            $fields.Add($sb.ToString())
        } else {
            $start = $i
            while ($i -lt $len -and $Line[$i] -ne ',') { $i++ }
            $fields.Add($Line.Substring($start, $i - $start).Replace('""', '"'))
            if ($i -lt $len) { $i++ }
        }
    }
    return @($fields)
}

# --- Build column name -> index from header line (case-insensitive) ---
function Get-CsvColumnIndices {
    param([string] $HeaderLine)
    $arr = Parse-CsvLine -Line $HeaderLine
    $h = @{}
    for ($j = 0; $j -lt $arr.Length; $j++) {
        $name = ($arr[$j] -replace '^\s+|\s+$', '').ToLowerInvariant()
        if (-not $h.ContainsKey($name)) { $h[$name] = $j }
    }
    return $h
}

# Master CSV (msw-assets-export) has OutputSubdir on every row; no separate catalog stream.

# Helper: get output PNG path for a RUID. Uses OutputSubdir from row (master CSV).
function Get-PngPathForRuid {
    param([string]$ruid, [string]$outputSubdirFromRow = $null)
    $subDirRel = if (-not [string]::IsNullOrWhiteSpace($outputSubdirFromRow)) { $outputSubdirFromRow } else { "Unknown" }
    $subDirRel = $subDirRel -replace '/', [System.IO.Path]::DirectorySeparatorChar
    $outSubDir = Join-Path $OutDir $subDirRel
    return Join-Path $outSubDir "$ruid.png"
}

# Parse Node stdout: lines are {"pngPath":"...","ok":true,"offsetX":n,"offsetY":n} or {"summary":true,"ok":N,"fail":M}. Returns { ok, fail, offsets }.
function Parse-NodeOffsetOutput {
    param([string[]] $lines)
    $ok = 0
    $fail = 0
    $offsets = [System.Collections.Generic.List[object]]::new()
    foreach ($line in $lines) {
        $line = $line -replace '^\s*|\s*$', ''
        if ([string]::IsNullOrEmpty($line)) { continue }
        try {
            $obj = $line | ConvertFrom-Json
            if ($obj.summary -eq $true) {
                $ok = [int]$obj.ok
                $fail = [int]$obj.fail
            } elseif ($obj.ok -eq $true -and $null -ne $obj.offsetX -and $null -ne $obj.offsetY) {
                $offsets.Add([PSCustomObject]@{ pngPath = $obj.pngPath; offsetX = [double]$obj.offsetX; offsetY = [double]$obj.offsetY })
            }
        } catch { }
    }
    return [PSCustomObject]@{ ok = $ok; fail = $fail; offsets = @($offsets) }
}

function Get-RuidAndSubdirFromPngPath {
    param([string] $pngPath, [string] $outDir)
    $outDirNorm = $outDir.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
    if (-not $pngPath.StartsWith($outDirNorm, [StringComparison]::OrdinalIgnoreCase)) {
        $ruid = [System.IO.Path]::GetFileNameWithoutExtension($pngPath)
        return [PSCustomObject]@{ ruid = $ruid; output_subdir = "Unknown" }
    }
    $rel = $pngPath.Substring($outDirNorm.Length).Replace([System.IO.Path]::DirectorySeparatorChar, '/')
    $parts = $rel -split '/'
    if ($parts.Count -lt 2) {
        $ruid = [System.IO.Path]::GetFileNameWithoutExtension($pngPath)
        return [PSCustomObject]@{ ruid = $ruid; output_subdir = "Unknown" }
    }
    $ruid = [System.IO.Path]::GetFileNameWithoutExtension($parts[-1])
    $output_subdir = ($parts[0..($parts.Count - 2)] -join '/')
    return [PSCustomObject]@{ ruid = $ruid; output_subdir = $output_subdir }
}

function Process-Batch {
    param([object[]] $batchItems, [string] $batchJsonPath)
    $batchItems | ConvertTo-Json -Compress | Set-Content $batchJsonPath -Encoding UTF8 -NoNewline
    Push-Location $appDir
    try {
        $out = & node $batchScript $batchJsonPath 2>&1
        $parsed = Parse-NodeOffsetOutput -lines $out
        return [PSCustomObject]@{ ok = $parsed.ok; fail = $parsed.fail; offsets = $parsed.offsets }
    } finally {
        Pop-Location
        if (Test-Path $batchJsonPath) { Remove-Item $batchJsonPath -Force -ErrorAction SilentlyContinue }
    }
}

# Collect offset rows for DB update (catalog_enriched.offset_x, offset_y). Call from main thread only.
function Add-OffsetRowsToDbList {
    param([System.Collections.Generic.List[object]] $list, [object[]] $offsetRows, [string] $outDir)
    foreach ($row in $offsetRows) {
        $info = Get-RuidAndSubdirFromPngPath -pngPath $row.pngPath -outDir $outDir
        $list.Add([PSCustomObject]@{ ruid = $info.ruid; output_subdir = $info.output_subdir; offset_x = $row.offsetX; offset_y = $row.offsetY })
    }
}

# Ensure directories exist for a list of items (png or ogg paths).
function Ensure-DirsForItems {
    param([object[]] $items, [string] $pathProperty)
    $dirs = $items | ForEach-Object { [System.IO.Path]::GetDirectoryName($_.$pathProperty) } | Sort-Object -Unique
    foreach ($d in $dirs) {
        if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
    }
}

# Process one chunk of sprite work (list of items). Batches computed on the fly; offsets added to $offsetRowsForDb.
function Process-SpriteChunk {
    param(
        [System.Collections.Generic.List[object]] $chunkList,
        [System.Collections.Generic.List[object]] $offsetRowsForDb,
        [ref] $totalOkRef,
        [ref] $totalFailRef
    )
    $count = $chunkList.Count
    if ($count -eq 0) { return }
    Ensure-DirsForItems -items $chunkList -pathProperty 'pngPath'
    $numBatches = [int][Math]::Ceiling($count / $BatchSize)
    $progressInterval = [Math]::Max(1, [int]($numBatches / 20))
    $heartbeatSeconds = 30
    $lastHeartbeat = Get-Date
    if ($throttle -eq 1 -or $PSVersionTable.PSVersion.Major -lt 7) {
        for ($i = 0; $i -lt $numBatches; $i++) {
            $start = $i * $BatchSize
            $take = [Math]::Min($BatchSize, $count - $start)
            $batchItems = @($chunkList.GetRange($start, $take) | ForEach-Object { [PSCustomObject]@{ modPath = $_.modPath; pngPath = $_.pngPath } })
            $tempJson = [System.IO.Path]::GetTempFileName() + ".json"
            $result = Process-Batch -batchItems $batchItems -batchJsonPath $tempJson
            $totalOkRef.Value += $result.ok
            $totalFailRef.Value += $result.fail
            Add-OffsetRowsToDbList -list $offsetRowsForDb -offsetRows $result.offsets -outDir $OutDir
            $done = $totalOkRef.Value + $totalFailRef.Value
            if ((Get-Date) - $lastHeartbeat -ge [TimeSpan]::FromSeconds($heartbeatSeconds)) {
                Write-Host "  progress: sprites batch $($i+1)/$numBatches, $done items so far"
                [Console]::Out.Flush()
                $lastHeartbeat = Get-Date
            }
            if (($i + 1) % $progressInterval -eq 0 -or ($i + 1) -eq $numBatches) {
                Write-Host "  sprites: $done so far (batch $($i+1)/$numBatches)"
            }
        }
    } else {
        $waveSize = [Math]::Max(1, [int]($numBatches / 15))
        for ($w = 0; $w -lt $numBatches; $w += $waveSize) {
            if ((Get-Date) - $lastHeartbeat -ge [TimeSpan]::FromSeconds($heartbeatSeconds)) {
                $done = $totalOkRef.Value + $totalFailRef.Value
                Write-Host "  progress: sprites wave at batch $w/$numBatches, $done items so far"
                [Console]::Out.Flush()
                $lastHeartbeat = Get-Date
            }
            $waveEnd = [Math]::Min($w + $waveSize, $numBatches)
            $waveBatches = [System.Collections.Generic.List[object]]::new()
            for ($bi = $w; $bi -lt $waveEnd; $bi++) {
                $start = $bi * $BatchSize
                $take = [Math]::Min($BatchSize, $count - $start)
                $batchItems = @($chunkList.GetRange($start, $take) | ForEach-Object { [PSCustomObject]@{ modPath = $_.modPath; pngPath = $_.pngPath } })
                $waveBatches.Add($batchItems)
            }
            $results = $waveBatches | ForEach-Object -ThrottleLimit $throttle -Parallel {
                $batch = $_
                $appDir = $using:appDir
                $batchScript = $using:batchScript
                $tempJson = [System.IO.Path]::GetTempFileName() + ".json"
                [System.IO.File]::WriteAllText($tempJson, ($batch | ConvertTo-Json -Compress), [System.Text.UTF8Encoding]::new($false))
                Push-Location $appDir
                try {
                    $out = & node $batchScript $tempJson 2>&1
                    $ok = 0
                    $fail = 0
                    $offsets = [System.Collections.Generic.List[object]]::new()
                    foreach ($line in $out) {
                        $line = ($line -replace '^\s*|\s*$', '')
                        if ([string]::IsNullOrEmpty($line)) { continue }
                        try {
                            $obj = $line | ConvertFrom-Json
                            if ($obj.summary -eq $true) {
                                $ok = [int]$obj.ok
                                $fail = [int]$obj.fail
                            } elseif ($obj.ok -eq $true -and $null -ne $obj.offsetX -and $null -ne $obj.offsetY) {
                                $offsets.Add([PSCustomObject]@{ pngPath = $obj.pngPath; offsetX = [double]$obj.offsetX; offsetY = [double]$obj.offsetY })
                            }
                        } catch { }
                    }
                    [PSCustomObject]@{ ok = $ok; fail = $fail; offsets = @($offsets) }
                } finally {
                    Pop-Location
                    if (Test-Path $tempJson) { Remove-Item $tempJson -Force -ErrorAction SilentlyContinue }
                }
            }
            foreach ($r in $results) {
                $totalOkRef.Value += $r.ok
                $totalFailRef.Value += $r.fail
                Add-OffsetRowsToDbList -list $offsetRowsForDb -offsetRows $r.offsets -outDir $OutDir
            }
            $done = $totalOkRef.Value + $totalFailRef.Value
            Write-Host "  sprites: $done so far (wave complete)"
        }
    }
}

# Process one chunk of other images (damageskin/avataritem/atlas). Sequential batches; add offsets to list.
function Process-OtherImageChunk {
    param(
        [System.Collections.Generic.List[object]] $chunkList,
        [System.Collections.Generic.List[object]] $offsetRowsForDb,
        [ref] $otherOkRef,
        [ref] $otherFailRef
    )
    $count = $chunkList.Count
    if ($count -eq 0) { return }
    Ensure-DirsForItems -items $chunkList -pathProperty 'pngPath'
    $numBatches = [int][Math]::Ceiling($count / $BatchSize)
    $otherProgressInterval = [Math]::Max(1, [int]($numBatches / 15))
    $lastHeartbeat = Get-Date
    for ($i = 0; $i -lt $count; $i += $BatchSize) {
        $batchNum = [int][Math]::Floor($i / $BatchSize) + 1
        $take = [Math]::Min($BatchSize, $count - $i)
        $batch = $chunkList.GetRange($i, $take) | ForEach-Object { [PSCustomObject]@{ modPath = $_.modPath; pngPath = $_.pngPath; imageType = $_.imageType } }
        $tempJson = [System.IO.Path]::GetTempFileName() + ".json"
        $batch | ConvertTo-Json -Compress | Set-Content $tempJson -Encoding UTF8 -NoNewline
        Push-Location $appDir
        try {
            $out = & node $otherImageBatchScript $tempJson 2>&1
            $parsed = Parse-NodeOffsetOutput -lines $out
            $otherOkRef.Value += $parsed.ok
            $otherFailRef.Value += $parsed.fail
            Add-OffsetRowsToDbList -list $offsetRowsForDb -offsetRows $parsed.offsets -outDir $OutDir
        } finally {
            Pop-Location
            if (Test-Path $tempJson) { Remove-Item $tempJson -Force -ErrorAction SilentlyContinue }
        }
        $done = $otherOkRef.Value + $otherFailRef.Value
        if ((Get-Date) - $lastHeartbeat -ge [TimeSpan]::FromSeconds(30)) {
            Write-Host "  progress: other images batch $batchNum/$numBatches, $done so far"
            [Console]::Out.Flush()
            $lastHeartbeat = Get-Date
        }
        if ($batchNum % $otherProgressInterval -eq 0 -or $batchNum -eq $numBatches) {
            Write-Host "  other images: $done so far (batch $batchNum/$numBatches)"
            [Console]::Out.Flush()
        }
    }
}

# --- Stream catalog_enriched from DB (Node) and process in chunks ---
$offsetRowsForDb = [System.Collections.Generic.List[object]]::new()
$spriteChunk = [System.Collections.Generic.List[object]]::new()
$otherChunk = [System.Collections.Generic.List[object]]::new()
$audioChunk = [System.Collections.Generic.List[object]]::new()
$totalOk = 0
$totalFail = 0
$otherOk = 0
$otherFail = 0
$audioOk = 0
$audioFail = 0
$audioSkippedExisting = 0
$audioSkippedNotFound = 0
$firstNotFoundPath = $null
$totalSpritesProcessed = 0
$totalOtherProcessed = 0
$totalAudioProcessed = 0
$testLimit = if ($Test) { 50 } else { [int]::MaxValue }
$spriteChunkNum = 0
$otherChunkNum = 0
$audioChunkNum = 0
$assetTypesArg = @($extractTypes) -join ','
if ($useCsvStream) {
    $streamFromCsvScript = Join-Path $helperDir "stream-from-csv.js"
    if (-not (Test-Path $streamFromCsvScript)) { Write-Error "stream-from-csv.js not found: $streamFromCsvScript"; exit 1 }
    $streamArgs = @($streamFromCsvScript, "--input-csv", $ExtractListCsv, "--cache-dir", $cacheRoot)
    Write-Host "Streaming extract list from CSV (chunk size $workListChunkSize)..."
} else {
    if (-not (Test-Path $streamCatalogScript)) { Write-Error "stream-catalog-enriched-for-extract.js not found: $streamCatalogScript (use -ExtractListCsv for DuckDB pipeline)"; exit 1 }
    $streamArgs = @($streamCatalogScript, "--db", $metadataDb, "--cache-dir", $cacheRoot, "--asset-types", $assetTypesArg)
    if ($Test) { $streamArgs += "--test" }
    Write-Host "Streaming catalog_enriched from DB (chunk size $workListChunkSize)..."
}
[Console]::Out.Flush()
$lineNum = 0
& node $streamArgs 2>&1 | ForEach-Object {
    $line = [string]$_ -replace '^\s+|\s*$', ''
    if ([string]::IsNullOrEmpty($line)) { return }
    $lineNum++
    try {
        $obj = $line | ConvertFrom-Json
    } catch { return }
    $assetType = ($obj.asset_type -replace '^\s+|\s+$', '').ToLowerInvariant()
    $ruid = ($obj.ruid -replace '^\s+|\s+$', '')
    $outputSubdirRow = if ($obj.output_subdir) { $obj.output_subdir } else { 'Unknown' }
    $modPath = $obj.modPath

    if ($assetType -eq 'sprite') {
        $pngPath = Get-PngPathForRuid -ruid $ruid -outputSubdirFromRow $outputSubdirRow
        if ($SkipExisting -and (Test-Path $pngPath)) { return }
        if ($spriteChunk.Count -ge $testLimit) { return }
        $spriteChunk.Add([PSCustomObject]@{ modPath = $modPath; pngPath = $pngPath; imageType = 'sprite' })
        if ($spriteChunk.Count -ge $workListChunkSize) {
            $spriteChunkNum++
            Write-Host "Sprites: processing chunk $spriteChunkNum ($($spriteChunk.Count) items)..."
            Process-SpriteChunk -chunkList $spriteChunk -offsetRowsForDb $offsetRowsForDb -totalOkRef ([ref]$totalOk) -totalFailRef ([ref]$totalFail)
            $totalSpritesProcessed += $spriteChunk.Count
            $spriteChunk.Clear()
        }
    } elseif ($assetType -in @('damageskin','avataritem','atlas')) {
        $pngPath = Get-PngPathForRuid -ruid $ruid -outputSubdirFromRow $outputSubdirRow
        if ($SkipExisting -and (Test-Path $pngPath)) { return }
        if ($otherChunk.Count -ge $testLimit) { return }
        $otherChunk.Add([PSCustomObject]@{ modPath = $modPath; pngPath = $pngPath; imageType = $assetType })
        if ($otherChunk.Count -ge $workListChunkSize) {
            $otherChunkNum++
            Write-Host "Other images: processing chunk $otherChunkNum ($($otherChunk.Count) items)..."
            Process-OtherImageChunk -chunkList $otherChunk -offsetRowsForDb $offsetRowsForDb -otherOkRef ([ref]$otherOk) -otherFailRef ([ref]$otherFail)
            $totalOtherProcessed += $otherChunk.Count
            $otherChunk.Clear()
        }
    } elseif ($assetType -eq 'audioclip') {
        if ($audioChunk.Count -ge $testLimit) { return }
        $subDirRel = $outputSubdirRow -replace '/', [System.IO.Path]::DirectorySeparatorChar
        $outSubDir = Join-Path $AudioOutDir $subDirRel
        $oggPath = Join-Path $outSubDir "$ruid.ogg"
        if ($SkipExisting -and (Test-Path $oggPath)) { $audioSkippedExisting++; return }
        if (-not (Test-Path $modPath)) { $audioSkippedNotFound++; if (-not $firstNotFoundPath) { $firstNotFoundPath = $modPath }; return }
        $audioChunk.Add([PSCustomObject]@{ cachePath = $modPath; oggPath = $oggPath; Kind = 'msw' })
        if ($audioChunk.Count -ge $workListChunkSize) {
            $audioChunkNum++
            Ensure-DirsForItems -items $audioChunk -pathProperty 'oggPath'
            $OggS = [byte[]]@(0x4F, 0x67, 0x67, 0x53)
            if ($throttle -eq 1 -or $PSVersionTable.PSVersion.Major -lt 7) {
                foreach ($item in $audioChunk) {
                    try {
                        $bytes = [System.IO.File]::ReadAllBytes($item.cachePath)
                        $offset = 0
                        for ($i = 0; $i -le $bytes.Length - 4; $i++) {
                            if ($bytes[$i] -eq $OggS[0] -and $bytes[$i+1] -eq $OggS[1] -and $bytes[$i+2] -eq $OggS[2] -and $bytes[$i+3] -eq $OggS[3]) { $offset = $i; break }
                        }
                        if ($offset -eq 0 -and $bytes.Length -gt 162) { $offset = 162 }
                        $payloadLen = $bytes.Length - $offset
                        if ($payloadLen -gt 0) {
                            $fs = [System.IO.File]::Create($item.oggPath)
                            try { $fs.Write($bytes, $offset, $payloadLen); $audioOk++ } finally { $fs.Close() }
                        } else { $audioFail++ }
                    } catch { $audioFail++ }
                }
            } else {
                $waveItems = @($audioChunk)
                $waveResults = $waveItems | ForEach-Object -ThrottleLimit $throttle -Parallel {
                    $item = $_
                    $OggS = $using:OggS
                    $ok = 0
                    $fail = 0
                    try {
                        $bytes = [System.IO.File]::ReadAllBytes($item.cachePath)
                        $offset = 0
                        for ($i = 0; $i -le $bytes.Length - 4; $i++) {
                            if ($bytes[$i] -eq $OggS[0] -and $bytes[$i+1] -eq $OggS[1] -and $bytes[$i+2] -eq $OggS[2] -and $bytes[$i+3] -eq $OggS[3]) { $offset = $i; break }
                        }
                        if ($offset -eq 0 -and $bytes.Length -gt 162) { $offset = 162 }
                        $payloadLen = $bytes.Length - $offset
                        if ($payloadLen -gt 0) {
                            $fs = [System.IO.File]::Create($item.oggPath)
                            try { $fs.Write($bytes, $offset, $payloadLen); $ok = 1 } finally { $fs.Close() }
                        } else { $fail = 1 }
                    } catch { $fail = 1 }
                    [PSCustomObject]@{ Ok = $ok; Fail = $fail }
                }
                foreach ($r in $waveResults) { $audioOk += $r.Ok; $audioFail += $r.Fail }
            }
            $totalAudioProcessed += $audioChunk.Count
            Write-Host "  audio: $($audioOk + $audioFail) so far (chunk $audioChunkNum)"
            $audioChunk.Clear()
        }
    }
}

# Process remainder chunks
if ($spriteChunk.Count -gt 0) {
    $spriteChunkNum++
    Write-Host "Sprites: processing chunk $spriteChunkNum (final; $($spriteChunk.Count) items)..."
    Process-SpriteChunk -chunkList $spriteChunk -offsetRowsForDb $offsetRowsForDb -totalOkRef ([ref]$totalOk) -totalFailRef ([ref]$totalFail)
    $totalSpritesProcessed += $spriteChunk.Count
}
if ($otherChunk.Count -gt 0) {
    $otherChunkNum++
    Write-Host "Other images: processing chunk $otherChunkNum (final; $($otherChunk.Count) items)..."
    Process-OtherImageChunk -chunkList $otherChunk -offsetRowsForDb $offsetRowsForDb -otherOkRef ([ref]$otherOk) -otherFailRef ([ref]$otherFail)
    $totalOtherProcessed += $otherChunk.Count
}
# Flush offsets to DB (or write staging file when -StagingOnly for DuckDB pipeline)
$stagingPath = Join-Path $tempDir "offsets-staging.jsonl"
if ($StagingOnly) {
    if (-not (Test-Path $tempDir)) { New-Item -ItemType Directory -Path $tempDir -Force | Out-Null }
    if ($offsetRowsForDb.Count -gt 0) {
        $offsetRowsForDb | ForEach-Object { @{ ruid = $_.ruid; output_subdir = $_.output_subdir; offset_x = $_.offset_x; offset_y = $_.offset_y } | ConvertTo-Json -Compress } | Set-Content $stagingPath -Encoding UTF8
    } else {
        Set-Content -Path $stagingPath -Value $null -Encoding UTF8
    }
    Write-Host "Extract (StagingOnly): Wrote $($offsetRowsForDb.Count) offset row(s) to $stagingPath"
} elseif ($offsetRowsForDb.Count -gt 0) {
        if (-not (Test-Path $updateOffsetsScript)) { Write-Error "update-catalog-offsets.js not found: $updateOffsetsScript (use -StagingOnly for DuckDB pipeline)"; exit 1 }
        Write-Host "Extract: Flushing offsets to DB ($($offsetRowsForDb.Count) rows)..."
        [Console]::Out.Flush()
        $offsetsTemp = [System.IO.Path]::GetTempFileName()
        try {
            $offsetRowsForDb | ForEach-Object { @{ ruid = $_.ruid; output_subdir = $_.output_subdir; offset_x = $_.offset_x; offset_y = $_.offset_y } | ConvertTo-Json -Compress } | Set-Content $offsetsTemp -Encoding UTF8
            & node $updateOffsetsScript --db $metadataDb --input $offsetsTemp
            if ($LASTEXITCODE -ne 0) { Write-Warning "update-catalog-offsets.js exited with $LASTEXITCODE" }
            else { Write-Host "Updated catalog_enriched with $($offsetRowsForDb.Count) offset row(s)." }
        } finally {
            if (Test-Path $offsetsTemp) { Remove-Item $offsetsTemp -Force -ErrorAction SilentlyContinue }
        }
}

if ($audioChunk.Count -gt 0) {
    $audioChunkNum++
    Ensure-DirsForItems -items $audioChunk -pathProperty 'oggPath'
    $OggS = [byte[]]@(0x4F, 0x67, 0x67, 0x53)
    if ($throttle -eq 1 -or $PSVersionTable.PSVersion.Major -lt 7) {
        foreach ($item in $audioChunk) {
            try {
                if ($item.Kind -eq "raw") {
                    Copy-Item -Path $item.cachePath -Destination $item.oggPath -Force
                    $audioOk++
                } else {
                    $bytes = [System.IO.File]::ReadAllBytes($item.cachePath)
                    $offset = 0
                    for ($i = 0; $i -le $bytes.Length - 4; $i++) {
                        if ($bytes[$i] -eq $OggS[0] -and $bytes[$i+1] -eq $OggS[1] -and $bytes[$i+2] -eq $OggS[2] -and $bytes[$i+3] -eq $OggS[3]) { $offset = $i; break }
                    }
                    if ($offset -eq 0 -and $bytes.Length -gt 162) { $offset = 162 }
                    $payloadLen = $bytes.Length - $offset
                    if ($payloadLen -gt 0) {
                        $fs = [System.IO.File]::Create($item.oggPath)
                        try { $fs.Write($bytes, $offset, $payloadLen); $audioOk++ } finally { $fs.Close() }
                    } else { $audioFail++ }
                }
            } catch { $audioFail++ }
        }
    } else {
        $waveItems = @($audioChunk)
        $waveResults = $waveItems | ForEach-Object -ThrottleLimit $throttle -Parallel {
            $item = $_
            $OggS = $using:OggS
            $ok = 0
            $fail = 0
            try {
                if ($item.Kind -eq "raw") {
                    Copy-Item -Path $item.cachePath -Destination $item.oggPath -Force
                    $ok = 1
                } else {
                    $bytes = [System.IO.File]::ReadAllBytes($item.cachePath)
                    $offset = 0
                    for ($i = 0; $i -le $bytes.Length - 4; $i++) {
                        if ($bytes[$i] -eq $OggS[0] -and $bytes[$i+1] -eq $OggS[1] -and $bytes[$i+2] -eq $OggS[2] -and $bytes[$i+3] -eq $OggS[3]) { $offset = $i; break }
                    }
                    if ($offset -eq 0 -and $bytes.Length -gt 162) { $offset = 162 }
                    $payloadLen = $bytes.Length - $offset
                    if ($payloadLen -gt 0) {
                        $fs = [System.IO.File]::Create($item.oggPath)
                        try { $fs.Write($bytes, $offset, $payloadLen); $ok = 1 } finally { $fs.Close() }
                    } else { $fail = 1 }
                }
            } catch { $fail = 1 }
            [PSCustomObject]@{ Ok = $ok; Fail = $fail }
        }
        foreach ($r in $waveResults) { $audioOk += $r.Ok; $audioFail += $r.Fail }
    }
    $totalAudioProcessed += $audioChunk.Count
}

if ($audioSkippedNotFound -gt 0) {
    Write-Host "  Audio: $audioSkippedNotFound skipped (source file not in cache). Sample path: $firstNotFoundPath"
}
if ($audioSkippedExisting -gt 0) {
    Write-Host "  Audio: $audioSkippedExisting skipped (output already exists, -SkipExisting)"
}

Write-Host "Sprites: $totalOk PNG(s) written, $totalFail failed (total processed: $totalSpritesProcessed)"
Write-Host "Other images: $otherOk PNG(s) written, $otherFail failed (total processed: $totalOtherProcessed)"
Write-Host "Audio: $audioOk OGG(s) written, $audioFail failed (total processed: $totalAudioProcessed)"
Write-Host "Done: images -> $OutDir" + $(if ($totalAudioProcessed -gt 0) { ", audio -> $AudioOutDir" } else { "" }) + " (sprites: $totalOk, other images: $otherOk, audio: $audioOk)"
