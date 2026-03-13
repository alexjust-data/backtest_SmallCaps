param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [Parameter(Mandatory=$true)][string]$CsvPath,
  [string]$QuotesRoot = "D:\quotes",
  [int]$Concurrent = 24,
  [int]$TaskBatchSize = 500,
  [string]$SessionStart = "04:00:00",
  [string]$SessionEnd = "20:00:00"
)

$ErrorActionPreference = "Stop"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
if (!(Test-Path $CsvPath)) { throw "CSV no existe: $CsvPath" }

python "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py" `
  --csv "$CsvPath" `
  --output "$QuotesRoot" `
  --concurrent $Concurrent `
  --run-id "$RunId" `
  --run-dir "$runDir" `
  --resume `
  --task-batch-size $TaskBatchSize `
  --session-start $SessionStart `
  --session-end $SessionEnd
