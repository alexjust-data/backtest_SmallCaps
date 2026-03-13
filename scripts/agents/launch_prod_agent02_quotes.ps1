param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [string]$QuotesRoot = "D:\quotes",
  [int]$MaxFiles = 50000,
  [int]$SleepSec = 15,
  [switch]$ResetState
)

$ErrorActionPreference = "Stop"
& "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent02_quotes_strict_loop.ps1" `
  -RunId "$RunId" `
  -QuotesRoot "$QuotesRoot" `
  -MaxFiles $MaxFiles `
  -SleepSec $SleepSec `
  -ResetState:$ResetState
