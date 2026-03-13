$ErrorActionPreference = "Stop"

$Outdir = "D:\reference_smoke_fix"
$ProgressPath = Join-Path $Outdir "_run\download_ohlcv_minute_v1.progress.json"
$MonthsRoot = Join-Path $Outdir "_run\flatfiles_months"
$FilesRoot = Join-Path $Outdir "_run\flatfiles_files"

if (-not (Test-Path $ProgressPath)) {
    Write-Host "progress.json aun no existe:"
    Write-Host $ProgressPath
    exit 0
}

Write-Host "=== progress.json ==="
Get-Content $ProgressPath
Write-Host ""

$monthCount = 0
$fileCount = 0

if (Test-Path $MonthsRoot) {
    $monthCount = (Get-ChildItem $MonthsRoot -Recurse -File -Filter *.json | Measure-Object).Count
}
if (Test-Path $FilesRoot) {
    $fileCount = (Get-ChildItem $FilesRoot -Recurse -File -Filter *.json | Measure-Object).Count
}

Write-Host "=== manifest counts ==="
Write-Host "month_manifests: $monthCount"
Write-Host "file_manifests:  $fileCount"
