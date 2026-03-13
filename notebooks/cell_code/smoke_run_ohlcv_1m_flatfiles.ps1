$ErrorActionPreference = "Stop"

$Python = "C:\TSIS_Data\v1\backtest_SmallCaps\backtest\Scripts\python.exe"
$Script = "C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\download_ohlcv_minute_v1.py"
$Input = "C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_missing_in_ohlcv_1m_vs_daily_smoke.parquet"
$Outdir = "D:\reference_smoke_fix"
$EnvFile = "C:\TSIS_Data\v1\backtest_SmallCaps\.env"

if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }
        $idx = $line.IndexOf("=")
        if ($idx -lt 1) {
            return
        }
        $name = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim().Trim("'`"")
        if ($name) {
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

$flatOpts = @{
    key = $env:AWS_ACCESS_KEY_ID
    secret = $env:AWS_SECRET_ACCESS_KEY
    client_kwargs = @{
        endpoint_url = $(if ($env:S3_Endpoint) { $env:S3_Endpoint } else { "https://files.massive.com" })
        region_name = $(if ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { "auto" })
    }
} | ConvertTo-Json -Compress -Depth 5

$FlatOptsPath = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\debug\smoke_flatfiles_storage_options.json"
New-Item -ItemType Directory -Force -Path (Split-Path $FlatOptsPath) | Out-Null
[System.IO.File]::WriteAllText($FlatOptsPath, $flatOpts, [System.Text.UTF8Encoding]::new($false))

$argsList = @(
    $Script,
    "--input", $Input,
    "--outdir", $Outdir,
    "--start", "2005-01-01",
    "--end", "2026-12-31",
    "--source", "flatfiles",
    "--flatfiles-root", "s3://flatfiles/us_stocks_sip/minute_aggs_v1",
    "--flatfiles-storage-options", "@$FlatOptsPath",
    "--resume",
    "--resume-validate",
    "--prune-obsolete-months",
    "--progress-every", "25",
    "--progress-seconds", "20"
)

Write-Host "Running smoke flatfiles download..."
Write-Host "Input:  $Input"
Write-Host "Outdir: $Outdir"
Write-Host "Env file: $EnvFile"
Write-Host "Storage options file: $FlatOptsPath"
Write-Host ""

& $Python @argsList

if ($LASTEXITCODE -ne 0) {
    throw "Smoke download failed with exit code $LASTEXITCODE"
}
