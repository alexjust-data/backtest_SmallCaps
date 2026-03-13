param(
    [string]$DataRoot = "D:\ohlcv_1m",
    [string]$CalendarCsv = "C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\market_calendar_official_XNYS_20050101_20251231.csv",
    [string]$AuditDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\debug\audit_ohlcv_1m_hours",
    [int]$Workers = [Math]::Max(4, [Environment]::ProcessorCount - 2),
    [int]$ProgressEvery = 200,
    [Nullable[int]]$MaxTickers = $null
)

$ErrorActionPreference = "Stop"

$env:PYTHONUTF8 = "1"
$env:PYTHONUNBUFFERED = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:NUMEXPR_NUM_THREADS = "1"

New-Item -ItemType Directory -Force -Path $AuditDir | Out-Null
$LogPath = Join-Path $AuditDir "terminal_run.log"
$RunnerPath = Join-Path $AuditDir "_run_audit_ohlcv_1m_hours_runner.py"

$PyCode = @'
from pathlib import Path
from datetime import datetime
import os

g = {}
g["SCRIPT"] = Path(os.environ["AUDIT_SCRIPT"])
g["DATA_ROOT"] = Path(os.environ["AUDIT_DATA_ROOT"])
g["CALENDAR_CSV"] = Path(os.environ["AUDIT_CALENDAR_CSV"])
g["AUDIT_DIR"] = Path(os.environ["AUDIT_DIR"])
g["WORKERS"] = int(os.environ["AUDIT_WORKERS"])
g["PROGRESS_EVERY"] = int(os.environ["AUDIT_PROGRESS_EVERY"])
if os.environ.get("AUDIT_MAX_TICKERS"):
    g["MAX_TICKERS"] = int(os.environ["AUDIT_MAX_TICKERS"])

print(f"[{datetime.now()}] START audit_ohlcv_1m_hours")
print("CONFIG:")
print(f"- DATA_ROOT: {g['DATA_ROOT']}")
print(f"- CALENDAR_CSV: {g['CALENDAR_CSV']}")
print(f"- AUDIT_DIR: {g['AUDIT_DIR']}")
print(f"- WORKERS: {g['WORKERS']}")
print(f"- PROGRESS_EVERY: {g['PROGRESS_EVERY']}")
print(f"- MAX_TICKERS: {g.get('MAX_TICKERS')}")
print()

exec(g["SCRIPT"].read_text(encoding="utf-8"), g, g)

print()
print(f"[{datetime.now()}] END audit_ohlcv_1m_hours")
'@

$PyCode | Set-Content -LiteralPath $RunnerPath -Encoding UTF8

$env:AUDIT_SCRIPT = "C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\audit_ohlcv_1m_hours.py"
$env:AUDIT_DATA_ROOT = $DataRoot
$env:AUDIT_CALENDAR_CSV = $CalendarCsv
$env:AUDIT_DIR = $AuditDir
$env:AUDIT_WORKERS = [string]$Workers
$env:AUDIT_PROGRESS_EVERY = [string]$ProgressEvery
if ($null -ne $MaxTickers) {
    $env:AUDIT_MAX_TICKERS = [string]$MaxTickers
} else {
    Remove-Item Env:\AUDIT_MAX_TICKERS -ErrorAction SilentlyContinue
}

python -u $RunnerPath 2>&1 | Tee-Object -FilePath $LogPath

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
