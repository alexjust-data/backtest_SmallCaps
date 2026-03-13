param(
  [string]$RunId = "SET_RUN_ID_HERE",
  [int]$SleepSec = 60
)

$ErrorActionPreference = "Stop"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"

Write-Host "[agent03-compact] RunId=$RunId" -ForegroundColor Cyan
Write-Host "[agent03-compact] RunDir=$runDir" -ForegroundColor Cyan

while ($true) {
  $pyPath = Join-Path $env:TEMP ("agent03_compact_" + $RunId + ".py")
  $py = @"
from pathlib import Path
import json
import io
import contextlib

RUN_DIR = Path(r'$runDir')
EVENTS_CSV = RUN_DIR / 'quotes_agent_strict_events_current.csv'
RETRY_QUEUE_CSV = RUN_DIR / 'retry_queue_quotes_strict_current.csv'
RETRY_FROZEN_CSV = RUN_DIR / 'retry_frozen_quotes_strict.csv'
BATCH_MANIFEST_CSV = RUN_DIR / 'batch_manifest_quotes_strict.csv'
RUN_CONFIG_JSON = RUN_DIR / 'run_config_quotes_strict.json'
OFFICIAL_LIFECYCLE_CSV = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.csv')
OUT_DIR = RUN_DIR / 'agent03_outputs'
MIN_COVERAGE_TO_PASS = 0.95
COVERAGE_OK_STATUSES = ['PASS', 'SOFT_FAIL']
ACCEPT_ALL_RAW = True
DIAG_HEAD_N = 3
COV_HEAD_N = 3
TOP_N = 5
FIGSIZE = (8, 3)

if EVENTS_CSV.exists():
    s036 = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\036_agent3_quotes_coverage_and_causes.py')
    # silenciar salida verbosa de 036
    with contextlib.redirect_stdout(io.StringIO()):
        exec(s036.read_text(encoding='utf-8'), globals())

summary_path = OUT_DIR / 'run_summary.json'
live_path = RUN_DIR / 'live_status_quotes_strict.json'

processed = None
retry = None
hard = None
updated = None
if live_path.exists():
    try:
        live = json.loads(live_path.read_text(encoding='utf-8'))
        processed = live.get('files_processed_total_state')
        retry = live.get('retry_pending_files_current')
        updated = live.get('updated_utc')
        hard = (live.get('severity_counts_current') or {}).get('HARD_FAIL', 0)
    except Exception:
        pass

gate = 'N/A'
mean_cov = None
if summary_path.exists():
    try:
        rs = json.loads(summary_path.read_text(encoding='utf-8'))
        gate = rs.get('gate_status', 'N/A')
        mean_cov = rs.get('mean_coverage_ok', None)
    except Exception:
        pass

print(json.dumps({
    'updated_utc': updated,
    'processed': processed,
    'retry_pending': retry,
    'hard_fail': hard,
    'gate': gate,
    'raw_dataset_status': (rs.get('raw_dataset_status') if summary_path.exists() and 'rs' in globals() else None),
    'acceptance_policy': (rs.get('acceptance_policy') if summary_path.exists() and 'rs' in globals() else None),
    'mean_coverage_ok': mean_cov,
}, ensure_ascii=False))
"@

  Set-Content -Path $pyPath -Value $py -Encoding UTF8
  $out = python $pyPath
  $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Write-Host "[$now] $out"
  Start-Sleep -Seconds $SleepSec
}
