param(
  [string]$RunId = "SET_RUN_ID_HERE",
  [string]$QuotesRoot = "D:\quotes",
  [int]$MaxFiles = 5000,
  [int]$SleepSec = 30,
  [string]$RetryPolicy = "hard_and_soft",
  [double]$MaxCrossedRatioPct = 0.8,
  [int]$MaxRetryAttempts = 3,
  [double]$HardFailCrossedPct = 5.0,
  [double]$HardFailAskIntegerPct = 95.0,
  [double]$HardFailAskIntCrossedPct = 20.0,
  [switch]$RescanAll,
  [switch]$ResetState,
  [switch]$OneShot,
  [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Write-Host "[agent02] RunId=$RunId" -ForegroundColor Cyan
Write-Host "[agent02] QuotesRoot=$QuotesRoot" -ForegroundColor Cyan
Write-Host "[agent02] RunDir=$runDir" -ForegroundColor Cyan
Write-Host "[agent02] Policy=ACCEPT_ALL_RAW_DIAGNOSE_LATER" -ForegroundColor DarkCyan

while ($true) {
  $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

  $pyPath = Join-Path $env:TEMP ("agent02_loop_" + $RunId + ".py")
  $py = @"
from pathlib import Path
import json
import io
import contextlib
import pandas as pd

RUN_ID = r'$RunId'
RUN_DIR = Path(r'$runDir')
RUN_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED_QUOTES_ROOT = Path(r'$QuotesRoot')
PROBE_ROOT = EXPECTED_QUOTES_ROOT
MAX_FILES = int($MaxFiles)
RESET_STATE = bool($($ResetState.IsPresent))
RESCAN_ALL = bool($($RescanAll.IsPresent))

STATE_FILE = RUN_DIR / 'quotes_agent_strict_state.json'
EVENTS_HISTORY_CSV = RUN_DIR / 'quotes_agent_strict_events_history.csv'
EVENTS_CURRENT_CSV = RUN_DIR / 'quotes_agent_strict_events_current.csv'
EVENTS_CSV = EVENTS_HISTORY_CSV

RETRY_QUEUE_CSV = RUN_DIR / 'retry_queue_quotes_strict.csv'
RETRY_QUEUE_PARQUET = RUN_DIR / 'retry_queue_quotes_strict.parquet'
RETRY_CURRENT_CSV = RUN_DIR / 'retry_queue_quotes_strict_current.csv'
RETRY_ATTEMPTS_CSV = RUN_DIR / 'retry_attempts_quotes_strict.csv'
RETRY_FROZEN_CSV = RUN_DIR / 'retry_frozen_quotes_strict.csv'
BATCH_MANIFEST_CSV = RUN_DIR / 'batch_manifest_quotes_strict.csv'
RUN_CONFIG_JSON = RUN_DIR / 'run_config_quotes_strict.json'
GRANULAR_DIR = RUN_DIR / 'granular_strict'
LIVE_STATUS_JSON = RUN_DIR / 'live_status_quotes_strict.json'

RETRY_POLICY = r'$RetryPolicy'
MAX_CROSSED_RATIO_PCT = float($MaxCrossedRatioPct)
MAX_RETRY_ATTEMPTS = int($MaxRetryAttempts)
HARD_FAIL_CROSSED_PCT = float($HardFailCrossedPct)
HARD_FAIL_ASK_INTEGER_PCT = float($HardFailAskIntegerPct)
HARD_FAIL_ASK_INT_CROSSED_PCT = float($HardFailAskIntCrossedPct)

SCRIPT = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\032_probe_quotes_agent_behavior_strict.py')

if bool($($VerboseOutput.IsPresent)):
    exec(SCRIPT.read_text(encoding='utf-8'), globals())
else:
    with contextlib.redirect_stdout(io.StringIO()):
        exec(SCRIPT.read_text(encoding='utf-8'), globals())

live = {}
if LIVE_STATUS_JSON.exists():
    try:
        live = json.loads(LIVE_STATUS_JSON.read_text(encoding='utf-8'))
    except Exception:
        live = {}

processed = live.get('files_processed_total_state')
pending = live.get('files_pending')
retry = live.get('retry_pending_files_current')
sev = live.get('severity_counts_current') or {}
hard = sev.get('HARD_FAIL', 0)
soft = sev.get('SOFT_FAIL', 0)
passn = sev.get('PASS', 0)

gate = 'N/A'
rs = RUN_DIR / 'agent03_outputs' / 'run_summary.json'
if rs.exists():
    try:
        gate = json.loads(rs.read_text(encoding='utf-8')).get('gate_status', 'N/A')
    except Exception:
        pass

print(json.dumps({
    'processed_total': processed,
    'pending': pending,
    'pass': passn,
    'soft': soft,
    'hard': hard,
    'retry': retry,
    'gate': gate,
    'run_dir': str(RUN_DIR)
}, ensure_ascii=False))
"@

  Set-Content -Path $pyPath -Value $py -Encoding UTF8
  $line = python $pyPath
  Write-Host "[$stamp] $line"

  if ($OneShot.IsPresent) { break }
  Start-Sleep -Seconds $SleepSec
}
