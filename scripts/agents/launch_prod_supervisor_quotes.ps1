param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [int]$IntervalSec = 10,
  [int]$StallSec = 180,
  [int]$HardFailWarnThreshold = 100,
  [switch]$BeepOnAlert
)

$ErrorActionPreference = "Stop"
& "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent_supervisor.ps1" `
  -RunId "$RunId" `
  -IntervalSec $IntervalSec `
  -StallSec $StallSec `
  -HardFailWarnThreshold $HardFailWarnThreshold `
  -BeepOnAlert:$BeepOnAlert
