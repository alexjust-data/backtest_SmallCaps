param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [int]$IntervalSec = 5
)

$ErrorActionPreference = "SilentlyContinue"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"
$statusJson = Join-Path $runDir "live_status_quotes_strict.json"

while ($true) {
  Clear-Host
  $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Write-Host "agent02-live-status" -ForegroundColor Cyan
  Write-Host "time: $now"
  Write-Host "run : $RunId"
  Write-Host "json: $statusJson"
  Write-Host ("-" * 100)

  if (!(Test-Path $statusJson)) {
    Write-Host "live_status_quotes_strict.json no existe aun." -ForegroundColor Yellow
    Start-Sleep -Seconds $IntervalSec
    continue
  }

  try {
    $obj = Get-Content -Raw -Path $statusJson | ConvertFrom-Json

    Write-Host ("updated_utc                : " + $obj.updated_utc)
    Write-Host ("probe_root                 : " + $obj.probe_root)
    Write-Host ("max_files                  : " + $obj.max_files)
    Write-Host ("files_discovered_total     : " + $obj.files_discovered_total)
    Write-Host ("files_pending              : " + $obj.files_pending)
    Write-Host ("files_processed_total_state: " + $obj.files_processed_total_state)
    Write-Host ("files_current_snapshot     : " + $obj.files_current_snapshot)
    Write-Host ("retry_pending_files_current: " + $obj.retry_pending_files_current)

    Write-Host "`nseverity_counts_current:" -ForegroundColor Cyan
    if ($obj.severity_counts_current) {
      $obj.severity_counts_current.PSObject.Properties | ForEach-Object {
        Write-Host ("  - {0}: {1}" -f $_.Name, $_.Value)
      }
    } else {
      Write-Host "  (vacio)"
    }

    Write-Host "`ntop_causes_current:" -ForegroundColor Cyan
    if ($obj.top_causes_current) {
      $obj.top_causes_current | Select-Object -First 10 | ForEach-Object {
        Write-Host ("  - {0}: {1}" -f $_.cause, $_.count)
      }
    } else {
      Write-Host "  (vacio)"
    }
  } catch {
    Write-Host "No se pudo leer/parsear live_status_quotes_strict.json." -ForegroundColor Red
  }

  Start-Sleep -Seconds $IntervalSec
}
