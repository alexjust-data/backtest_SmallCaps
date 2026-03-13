param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [int]$SleepSec = 30
)

$ErrorActionPreference = "Stop"
& "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent03_monitor_compact.ps1" `
  -RunId "$RunId" `
  -SleepSec $SleepSec
