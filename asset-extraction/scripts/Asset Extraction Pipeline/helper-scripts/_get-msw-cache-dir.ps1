# Default MSW resource cache path for the **current user** (uses %LOCALAPPDATA%; not hardcoded to any username).
# Dot-source from pipeline scripts; then use $MSWCacheDir when -CacheDir is not provided.
$script:MSWCacheDir = if ($env:LOCALAPPDATA) {
    [System.IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA "..\LocalLow\nexon\MapleStory Worlds\resource_cache"))
} else {
    ""
}
