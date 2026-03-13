param(
  [string]$RunId = "20260305_quotes_session_D01",
  [string]$QuotesRoot = "D:\quotes",
  [string]$PingMasterParquet = "C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\ping_range_master.parquet",
  [string]$DownloaderScript = "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py",
  [string]$DateFrom = "2005-01-01",
  [string]$DateTo = "2025-12-31",
  [int]$MaxConcurrent = 80,
  [int]$SleepSec = 30,
  [int]$NoProgressCyclesForRecovery = 4,
  [switch]$OneShot
)

$ErrorActionPreference = "Stop"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Write-Host "[agent01] RunId=$RunId" -ForegroundColor Cyan
Write-Host "[agent01] QuotesRoot=$QuotesRoot" -ForegroundColor Cyan
Write-Host "[agent01] RunDir=$runDir" -ForegroundColor Cyan

$lastProcessed = $null
$noProgressCount = 0
$didRecovery = $false

while ($true) {
  $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

  $pyPath = Join-Path $env:TEMP ("agent01_loop_" + $RunId + ".py")
  $py = @"
from pathlib import Path
import json
import re
import subprocess
import sys
import pandas as pd

RUN_ID = r'$RunId'
RUN_DIR = Path(r'$runDir')
RUN_DIR.mkdir(parents=True, exist_ok=True)

QUOTES_ROOT = Path(r'$QuotesRoot')
QUOTES_ROOT.mkdir(parents=True, exist_ok=True)

PING_MASTER_PARQUET = Path(r'$PingMasterParquet')
DOWNLOADER_SCRIPT = Path(r'$DownloaderScript')
DATE_FROM = r'$DateFrom'
DATE_TO = r'$DateTo'
MAX_CONCURRENT = int($MaxConcurrent)

TASKS_CSV = RUN_DIR / 'quotes_tasks_2005_2025_from_ping_master.csv'
TASKS_RETRY_CSV = RUN_DIR / 'quotes_tasks_retry_from_agent02.csv'
TASKS_SUMMARY_BY_TICKER_CSV = RUN_DIR / 'quotes_tasks_summary_by_ticker.csv'
RUN_CONTEXT_JSON = RUN_DIR / 'agent01_run_context.json'
RETRY_CURRENT_CSV = RUN_DIR / 'retry_queue_quotes_strict_current.csv'
LIVE_STATUS_JSON = RUN_DIR / 'live_status_quotes_strict.json'

MODE = r'__MODE__'
RESUME = bool(__RESUME__)

if not DOWNLOADER_SCRIPT.exists():
    raise FileNotFoundError(f'DOWNLOADER_SCRIPT no existe: {DOWNLOADER_SCRIPT}')


def build_base_tasks():
    if not PING_MASTER_PARQUET.exists():
        raise FileNotFoundError(f'No existe ping master: {PING_MASTER_PARQUET}')

    pm = pd.read_parquet(PING_MASTER_PARQUET)
    for c in ['ticker', 'has_data', 'first_day', 'last_day']:
        if c not in pm.columns:
            raise ValueError(f'ping master sin columna requerida: {c}')

    pm = pm.copy()
    pm['ticker'] = pm['ticker'].astype(str).str.strip()
    pm['has_data'] = pm['has_data'].fillna(False).astype(bool)
    pm['first_day'] = pd.to_datetime(pm['first_day'], errors='coerce')
    pm['last_day'] = pd.to_datetime(pm['last_day'], errors='coerce')

    pm = (
        pm.groupby('ticker', as_index=False)
          .agg(
              has_data=('has_data', 'max'),
              first_day=('first_day', 'min'),
              last_day=('last_day', 'max')
          )
    )

    start_bound = pd.Timestamp(DATE_FROM)
    end_bound = pd.Timestamp(DATE_TO)

    rows = []
    for _, r in pm.iterrows():
        if not bool(r['has_data']):
            continue
        if pd.isna(r['first_day']) or pd.isna(r['last_day']):
            continue

        lo = max(r['first_day'], start_bound)
        hi = min(r['last_day'], end_bound)
        if lo > hi:
            continue

        dts = pd.bdate_range(lo, hi)
        if len(dts) == 0:
            continue

        rows.append(pd.DataFrame({'ticker': r['ticker'], 'date': dts.strftime('%Y-%m-%d')}))

    if not rows:
        raise RuntimeError('No se generaron tareas desde ping master')

    tasks = (
        pd.concat(rows, ignore_index=True)
          .drop_duplicates(subset=['ticker', 'date'])
          .sort_values(['ticker', 'date'])
    )
    tasks.to_csv(TASKS_CSV, index=False)

    summary = tasks.groupby('ticker', as_index=False).agg(files_expected=('date', 'count'))
    summary.to_csv(TASKS_SUMMARY_BY_TICKER_CSV, index=False)

    ctx = {
        'run_id': RUN_ID,
        'quotes_root': str(QUOTES_ROOT),
        'date_from': DATE_FROM,
        'date_to': DATE_TO,
        'ping_master_parquet': str(PING_MASTER_PARQUET),
        'tasks_csv': str(TASKS_CSV),
        'tasks_rows': int(len(tasks)),
        'tickers_in_tasks': int(tasks['ticker'].nunique()),
        'max_concurrent': MAX_CONCURRENT,
    }
    RUN_CONTEXT_JSON.write_text(json.dumps(ctx, indent=2), encoding='utf-8')
    return int(len(tasks)), int(tasks['ticker'].nunique())


def build_retry_tasks():
    if not RETRY_CURRENT_CSV.exists():
        return 0

    q = pd.read_csv(RETRY_CURRENT_CSV)
    if q.empty or 'file' not in q.columns:
        return 0

    rows = []
    pat_dd = re.compile(
        r"[\\/](?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]day=(?P<day>\d{2})[\\/]quotes\.parquet$"
    )
    pat_iso = re.compile(
        r"[\\/](?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]day=(?P<iso_day>\d{4}-\d{2}-\d{2})[\\/]quotes\.parquet$"
    )

    for f in q['file'].astype(str):
        m_iso = pat_iso.search(f)
        if m_iso:
            rows.append({'ticker': m_iso.group('ticker'), 'date': m_iso.group('iso_day')})
            continue

        m_dd = pat_dd.search(f)
        if not m_dd:
            continue

        rows.append({'ticker': m_dd.group('ticker'), 'date': f"{m_dd.group('year')}-{m_dd.group('month')}-{m_dd.group('day')}"})

    if not rows:
        return 0

    df_retry = pd.DataFrame(rows).drop_duplicates().sort_values(['ticker', 'date'])
    df_retry.to_csv(TASKS_RETRY_CSV, index=False)
    return int(len(df_retry))


def run_downloader(csv_path: Path, resume: bool):
    cmd = [
        sys.executable,
        str(DOWNLOADER_SCRIPT),
        '--csv', str(csv_path),
        '--output', str(QUOTES_ROOT),
        '--concurrent', str(MAX_CONCURRENT),
    ]
    if resume:
        cmd.append('--resume')

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last = ''
    for line in p.stdout:
        last = line.rstrip()[-400:]
    code = p.wait()
    return code, last


base_tasks_rows = None
base_tickers = None
if (not TASKS_CSV.exists()) and MODE in {'NORMAL', 'RECOVERY'}:
    base_tasks_rows, base_tickers = build_base_tasks()

retry_rows = 0
selected_csv = TASKS_CSV
if MODE == 'RETRY':
    retry_rows = build_retry_tasks()
    if retry_rows > 0:
        selected_csv = TASKS_RETRY_CSV
    else:
        MODE = 'NORMAL'

if MODE in {'NORMAL', 'RECOVERY'} and not TASKS_CSV.exists():
    base_tasks_rows, base_tickers = build_base_tasks()

exit_code, last_line = run_downloader(selected_csv, RESUME)

live = {}
if LIVE_STATUS_JSON.exists():
    try:
        live = json.loads(LIVE_STATUS_JSON.read_text(encoding='utf-8'))
    except Exception:
        live = {}

out = {
    'mode': MODE,
    'resume': RESUME,
    'tasks_file': str(selected_csv),
    'retry_rows': retry_rows,
    'base_tasks_rows': base_tasks_rows,
    'base_tickers': base_tickers,
    'downloader_exit': exit_code,
    'downloader_last': last_line,
    'processed_total_state': live.get('files_processed_total_state'),
    'retry_pending_current': live.get('retry_pending_files_current'),
    'files_pending': live.get('files_pending')
}
print(json.dumps(out, ensure_ascii=False))
"@

  # Decide mode from Agent02 status
  $liveJson = Join-Path $runDir "live_status_quotes_strict.json"
  $mode = "NORMAL"
  $resume = $true

  if (Test-Path $liveJson) {
    try {
      $live = Get-Content -Raw -Path $liveJson | ConvertFrom-Json
      $processed = [int]($live.files_processed_total_state)
      $retryPending = [int]($live.retry_pending_files_current)

      if ($null -ne $lastProcessed -and $processed -le $lastProcessed) {
        $noProgressCount += 1
      } else {
        $noProgressCount = 0
      }
      $lastProcessed = $processed

      if ($noProgressCount -ge $NoProgressCyclesForRecovery -and -not $didRecovery) {
        $mode = "RECOVERY"
        $resume = $false
        $didRecovery = $true
      } elseif ($retryPending -gt 0) {
        $mode = "RETRY"
      } else {
        $mode = "NORMAL"
        $didRecovery = $false
      }
    } catch {
      $mode = "NORMAL"
    }
  }

  $py = $py.Replace('__MODE__', $mode)
  $py = $py.Replace('__RESUME__', ($resume.ToString().ToLower()))

  Set-Content -Path $pyPath -Value $py -Encoding UTF8

  $line = python $pyPath
  Write-Host "[$stamp] $line"

  if ($OneShot.IsPresent) { break }
  Start-Sleep -Seconds $SleepSec
}
