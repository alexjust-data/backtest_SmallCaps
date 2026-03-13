param(
  [string]$RunId = "20260305_quotes_session_01",
  [int]$SleepSec = 120,
  [int]$TopN = 10,
  [switch]$Run043,
  [switch]$Run038,
  [int]$DiagHeadN = 10,
  [int]$CovHeadN = 10
)

$ErrorActionPreference = "Stop"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"

Write-Host "[agent03] RunId=$RunId" -ForegroundColor Cyan
Write-Host "[agent03] RunDir=$runDir" -ForegroundColor Cyan

while ($true) {
  $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Write-Host "`n[$stamp] [agent03] ciclo start" -ForegroundColor Yellow

  $pyPath = Join-Path $env:TEMP ("agent03_loop_" + $RunId + ".py")
  $py = @"
from pathlib import Path

RUN_DIR = Path(r'$runDir')

QUOTES_ROOT = Path(r'C:\TSIS_Data\data\quotes_p95')
EVENTS_CSV = RUN_DIR / 'quotes_agent_strict_events_current.csv'
RETRY_QUEUE_CSV = RUN_DIR / 'retry_queue_quotes_strict_current.csv'
RETRY_FROZEN_CSV = RUN_DIR / 'retry_frozen_quotes_strict.csv'
BATCH_MANIFEST_CSV = RUN_DIR / 'batch_manifest_quotes_strict.csv'
RUN_CONFIG_JSON = RUN_DIR / 'run_config_quotes_strict.json'
OFFICIAL_LIFECYCLE_CSV = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.csv')
OUT_DIR = RUN_DIR / 'agent03_outputs'

MIN_COVERAGE_TO_PASS = 0.95
COVERAGE_OK_STATUSES = ['PASS', 'SOFT_FAIL']
DIAG_HEAD_N = int($DiagHeadN)
COV_HEAD_N = int($CovHeadN)
TOP_N = int($TopN)
FIGSIZE = (12, 5)

print('RUN_DIR:', RUN_DIR)
print('EVENTS_CSV exists:', EVENTS_CSV.exists())
print('RETRY_QUEUE_CSV exists:', RETRY_QUEUE_CSV.exists())
print('BATCH_MANIFEST_CSV exists:', BATCH_MANIFEST_CSV.exists())

if EVENTS_CSV.exists():
    s036 = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\036_agent3_quotes_coverage_and_causes.py')
    exec(s036.read_text(encoding='utf-8'), globals())

    s037 = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\037_agent3_diagnostics_tables_hist.py')
    exec(s037.read_text(encoding='utf-8'), globals())

    if bool($($Run043.IsPresent)):
        MAX_FILES_ANALYZE = 300
        TOP_N_WORST = 20
        FIGSIZE = (12, 4)
        s043 = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\043_dtype_rounding_plots.py')
        exec(s043.read_text(encoding='utf-8'), globals())

    if bool($($Run038.IsPresent)):
        TOP_N = 20
        ZERO_ZOOM_MAX_PCT = 0.02
        FIGSIZE = (12, 5)
        s038 = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\038_bid_ask_cross_deviation_metrics.py')
        exec(s038.read_text(encoding='utf-8'), globals())
else:
    print('EVENTS_CSV aun no existe. Agent02 todavia no escribio snapshot current.')
"@

  Set-Content -Path $pyPath -Value $py -Encoding UTF8
  python $pyPath
  Start-Sleep -Seconds $SleepSec
}
