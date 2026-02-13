# Shared helper: default parallelism for asset-extraction pipeline (half of logical cores).
# Dot-source from pipeline scripts: . "$PSScriptRoot\_get-parallelism.ps1"
# Override per-step via -Workers or -ThrottleLimit or -Concurrency (0 = use this default).
# Uses 1/2 of logical processors so the machine stays responsive; minimum 1.
$script:AssetExtractionParallelism = [Math]::Max(1, [int][Math]::Floor([Environment]::ProcessorCount / 2))
