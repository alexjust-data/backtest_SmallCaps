param(
  [string]$RunId = "SET_RUN_ID_HERE",
  [int]$IntervalSec = 10,
  [int]$StallSec = 180,
  [int]$HardFailSpike = 50,
  [int]$RetrySpike = 200,
  [int]$DownloadNoProgressSec = 300,
  [int]$HardFailWarnThreshold = 100,
  [switch]$BeepOnAlert
)

$ErrorActionPreference = "SilentlyContinue"
$runDir = "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\$RunId"

$downloadJson = Join-Path $runDir "download_live_status.json"
$strictJson = Join-Path $runDir "live_status_quotes_strict.json"
$runSummaryJson = Join-Path $runDir "agent03_outputs\run_summary.json"
$eventsCsv = Join-Path $runDir "quotes_agent_strict_events_current.csv"
$retryCsv = Join-Path $runDir "retry_queue_quotes_strict_current.csv"
$downloadEventsCsv = Join-Path $runDir "download_events_current.csv"
$supervisorEventsCsv = Join-Path $runDir "supervisor_events_history.csv"
$supervisorCurrentJson = Join-Path $runDir "supervisor_current_status.json"

$prevDoneOk = $null
$prevDoneBad = $null
$prevHard = $null
$prevRetry = $null
$prevAlertFingerprint = ""
$prevStateFingerprint = ""
$prevWarningFingerprint = ""

function Read-JsonFile([string]$Path) {
  if (!(Test-Path $Path)) { return $null }
  try {
    return Get-Content -Raw -Path $Path | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Get-JsonAgeSec($obj) {
  if ($null -eq $obj -or [string]::IsNullOrWhiteSpace([string]$obj.updated_utc)) { return $null }
  try {
    $u = [datetime]::Parse([string]$obj.updated_utc)
    return [int](($script:now.ToUniversalTime() - $u.ToUniversalTime()).TotalSeconds)
  } catch {
    return $null
  }
}

function Add-Alert([System.Collections.Generic.List[string]]$alerts, [string]$message) {
  if (![string]::IsNullOrWhiteSpace($message)) {
    $alerts.Add($message) | Out-Null
  }
}

function Add-Warning([System.Collections.Generic.List[string]]$warnings, [string]$message) {
  if (![string]::IsNullOrWhiteSpace($message)) {
    $warnings.Add($message) | Out-Null
  }
}

function Add-RunState([System.Collections.Generic.List[string]]$states, [string]$message) {
  if (![string]::IsNullOrWhiteSpace($message)) {
    $states.Add($message) | Out-Null
  }
}

function Write-StatusEvents(
  [string]$RunId,
  [string]$RunDir,
  [string]$CsvPath,
  [datetime]$Now,
  [System.Collections.Generic.List[string]]$RunStates,
  [System.Collections.Generic.List[string]]$Warnings,
  [System.Collections.Generic.List[string]]$Alerts
) {
  $rows = New-Object 'System.Collections.Generic.List[object]'

  foreach ($m in $RunStates) {
    $rows.Add([pscustomobject]@{
      observed_at_local = $Now.ToString("yyyy-MM-dd HH:mm:ss")
      observed_at_utc = $Now.ToUniversalTime().ToString("o")
      run_id = $RunId
      run_dir = $RunDir
      level = "RUN_STATE"
      message = $m
    }) | Out-Null
  }
  foreach ($m in $Warnings) {
    $rows.Add([pscustomobject]@{
      observed_at_local = $Now.ToString("yyyy-MM-dd HH:mm:ss")
      observed_at_utc = $Now.ToUniversalTime().ToString("o")
      run_id = $RunId
      run_dir = $RunDir
      level = "WARNING"
      message = $m
    }) | Out-Null
  }
  foreach ($m in $Alerts) {
    $rows.Add([pscustomobject]@{
      observed_at_local = $Now.ToString("yyyy-MM-dd HH:mm:ss")
      observed_at_utc = $Now.ToUniversalTime().ToString("o")
      run_id = $RunId
      run_dir = $RunDir
      level = "ALERT"
      message = $m
    }) | Out-Null
  }

  if ($rows.Count -eq 0) { return }

  $csvExists = Test-Path $CsvPath
  $rows | Export-Csv -Path $CsvPath -Append -NoTypeInformation -Encoding UTF8:$false
  if (-not $csvExists) {
    $tmp = Import-Csv $CsvPath
    $tmp | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
  }
}

function Write-CurrentStatusJson(
  [string]$Path,
  [datetime]$Now,
  [string]$RunId,
  [string]$RunDir,
  [object]$Download,
  [object]$Strict,
  [object]$Summary,
  [System.Collections.Generic.List[string]]$RunStates,
  [System.Collections.Generic.List[string]]$Warnings,
  [System.Collections.Generic.List[string]]$Alerts
) {
  $payload = [ordered]@{
    updated_local = $Now.ToString("yyyy-MM-dd HH:mm:ss")
    updated_utc = $Now.ToUniversalTime().ToString("o")
    run_id = $RunId
    run_dir = $RunDir
    run_state = @($RunStates)
    warnings = @($Warnings)
    alerts = @($Alerts)
    download_json = if ($null -ne $Download) { [ordered]@{
      updated_utc = $Download.updated_utc
      tasks_total = $Download.tasks_total
      done_ok = $Download.done_ok
      done_bad = $Download.done_bad
      pending = $Download.pending
      concurrent = $Download.concurrent
    }} else { $null }
    strict_json = if ($null -ne $Strict) { [ordered]@{
      updated_utc = $Strict.updated_utc
      files_current_snapshot = $Strict.files_current_snapshot
      files_pending = $Strict.files_pending
      files_processed_total_state = $Strict.files_processed_total_state
      retry_pending_files_current = $Strict.retry_pending_files_current
      severity_counts_current = $Strict.severity_counts_current
      top_causes_current = $Strict.top_causes_current
    }} else { $null }
    run_summary = if ($null -ne $Summary) { [ordered]@{
      gate_status = $Summary.gate_status
      acceptance_policy = $Summary.acceptance_policy
      raw_dataset_status = $Summary.raw_dataset_status
      mean_coverage_ok = $Summary.mean_coverage_ok
      retry_pending_files = $Summary.retry_pending_files
      hard_fail_files = $Summary.hard_fail_files
    }} else { $null }
  }
  ([pscustomobject]$payload | ConvertTo-Json -Depth 8) | Set-Content -Path $Path -Encoding UTF8
}

while ($true) {
  Clear-Host
  $now = Get-Date
  $alerts = New-Object 'System.Collections.Generic.List[string]'
  $warnings = New-Object 'System.Collections.Generic.List[string]'
  $runStates = New-Object 'System.Collections.Generic.List[string]'

  Write-Host "agent-supervisor" -ForegroundColor Cyan
  Write-Host ("time : " + $now.ToString("yyyy-MM-dd HH:mm:ss"))
  Write-Host ("run  : " + $RunId)
  Write-Host ("dir  : " + $runDir)
  Write-Host ("-" * 100)

  $download = Read-JsonFile $downloadJson
  $strict = Read-JsonFile $strictJson
  $summary = Read-JsonFile $runSummaryJson

  Write-Host "AGENT01 download" -ForegroundColor Cyan
  if ($null -eq $download) {
    Write-Host "  download_live_status.json no existe aun." -ForegroundColor Yellow
    Add-Warning $warnings "AGENT01_MISSING: sin download_live_status.json"
  } else {
    $doneOk = [int]$download.done_ok
    $doneBad = [int]$download.done_bad
    $tasksTotal = [int]$download.tasks_total
    $pending = [int]$download.pending
    $downloadAge = Get-JsonAgeSec $download

    Write-Host ("  updated_utc      : " + $download.updated_utc)
    Write-Host ("  tasks_total      : " + $tasksTotal)
    Write-Host ("  done_ok          : " + $doneOk)
    Write-Host ("  done_bad         : " + $doneBad)
    Write-Host ("  pending          : " + $pending)
    Write-Host ("  concurrent       : " + $download.concurrent)
    Write-Host ("  session          : {0}-{1}" -f $download.session_start_local, $download.session_end_local)
    if ($downloadAge -ne $null) {
      Write-Host ("  age_sec          : " + $downloadAge)
      if ($downloadAge -gt $StallSec) {
        Add-Alert $alerts "AGENT01_STALL: download_live_status sin actualizar > $StallSec s"
      }
    }

    if ($prevDoneOk -ne $null -and $prevDoneBad -ne $null) {
      $deltaDone = ($doneOk + $doneBad) - ($prevDoneOk + $prevDoneBad)
      if ($deltaDone -le 0 -and $pending -gt 0 -and $downloadAge -ne $null -and $downloadAge -gt $DownloadNoProgressSec) {
        Add-Alert $alerts "AGENT01_NO_PROGRESS: done_ok+done_bad no aumenta con pending=$pending"
      }
    }

    if ($pending -gt 0) {
      Add-RunState $runStates "AGENT01_RUNNING: descarga aun en progreso (pending=$pending)"
    } else {
      Add-RunState $runStates "AGENT01_DONE: descarga sin pendientes"
    }
  }

  Write-Host ""
  Write-Host "AGENT02 strict validation" -ForegroundColor Cyan
  if ($null -eq $strict) {
    Write-Host "  live_status_quotes_strict.json no existe aun." -ForegroundColor Yellow
    Add-Warning $warnings "AGENT02_MISSING: sin live_status_quotes_strict.json"
  } else {
    $processed = [int]$strict.files_processed_total_state
    $retry = [int]$strict.retry_pending_files_current
    $filesPending = [int]$strict.files_pending
    $hard = 0
    if ($strict.severity_counts_current) {
      $h = $strict.severity_counts_current.PSObject.Properties | Where-Object { $_.Name -eq 'HARD_FAIL' }
      if ($h) { $hard = [int]$h.Value }
    }
    $strictAge = Get-JsonAgeSec $strict

    Write-Host ("  updated_utc      : " + $strict.updated_utc)
    Write-Host ("  files_snapshot   : " + $strict.files_current_snapshot)
    Write-Host ("  files_pending    : " + $filesPending)
    Write-Host ("  processed_state  : " + $processed)
    Write-Host ("  retry_pending    : " + $retry)
    Write-Host ("  hard_fail        : " + $hard)
    if ($strictAge -ne $null) {
      Write-Host ("  age_sec          : " + $strictAge)
      if ($strictAge -gt $StallSec) {
        Add-Alert $alerts "AGENT02_STALL: live_status_quotes_strict sin actualizar > $StallSec s"
      }
    }

    if ($prevHard -ne $null -and ($hard - $prevHard) -ge $HardFailSpike) {
      Add-Warning $warnings "AGENT02_HARD_FAIL_SPIKE: +$($hard - $prevHard)"
    }
    if ($prevRetry -ne $null -and ($retry - $prevRetry) -ge $RetrySpike) {
      Add-Warning $warnings "AGENT02_RETRY_SPIKE: +$($retry - $prevRetry)"
    }

    Write-Host "  top_causes_current:" -ForegroundColor DarkCyan
    if ($strict.top_causes_current) {
      $strict.top_causes_current | Select-Object -First 8 | ForEach-Object {
        Write-Host ("    - {0}: {1}" -f $_.cause, $_.count)
        if ($_.cause -eq "parquet_unreadable" -and [int]$_.count -gt 0) {
          Add-Alert $alerts "AGENT02_STRUCTURAL: parquet_unreadable presente"
        }
      }
    } else {
      Write-Host "    (vacio)"
    }

    if ($retry -gt 0) {
      Add-RunState $runStates "AGENT02_RETRY_OPEN: retry_pending_files_current=$retry"
    }
    if ($hard -ge $HardFailWarnThreshold) {
      Add-Warning $warnings "AGENT02_HARD_PRESENT: hard_fail_current=$hard"
    }
  }

  Write-Host ""
  Write-Host "AGENT03 coverage summary" -ForegroundColor Cyan
  if ($null -eq $summary) {
    Write-Host "  run_summary.json no existe aun." -ForegroundColor Yellow
    Add-Warning $warnings "AGENT03_MISSING: sin agent03_outputs/run_summary.json"
  } else {
    $gate = [string]$summary.gate_status
    $acceptancePolicy = [string]$summary.acceptance_policy
    $rawDatasetStatus = [string]$summary.raw_dataset_status
    $meanCov = $summary.mean_coverage_ok
    $retryPending = $summary.retry_pending_files
    $hardFailFiles = $summary.hard_fail_files
    $summaryAge = [int]((Get-Date) - (Get-Item $runSummaryJson).LastWriteTime).TotalSeconds

    Write-Host ("  gate_status      : " + $gate)
    Write-Host ("  acceptance_policy: " + $acceptancePolicy)
    Write-Host ("  raw_dataset      : " + $rawDatasetStatus)
    Write-Host ("  mean_coverage_ok : " + $meanCov)
    Write-Host ("  retry_pending    : " + $retryPending)
    Write-Host ("  hard_fail_files  : " + $hardFailFiles)
    Write-Host ("  file_age_sec     : " + $summaryAge)

    if ($summaryAge -gt $StallSec) {
      Add-Alert $alerts "AGENT03_STALL: run_summary.json antiguo > $StallSec s"
    }
    if ($acceptancePolicy -eq "ACCEPT_ALL_RAW_REVIEW_LATER") {
      if ($rawDatasetStatus -eq "RAW_ACCEPTED_REVIEW_PENDING") {
        Add-RunState $runStates "AGENT03_RAW_ACCEPTED_REVIEW_PENDING"
      } elseif ($rawDatasetStatus -eq "RAW_ACCEPTED_REVIEW_COMPLETE") {
        Add-RunState $runStates "AGENT03_RAW_ACCEPTED_REVIEW_COMPLETE"
      }
      if ($retryPending -gt 0) {
        Add-RunState $runStates "AGENT03_REVIEW_PENDING_RETRY: retry_pending_files=$retryPending"
      }
      if ($hardFailFiles -gt 0) {
        Add-Warning $warnings "AGENT03_REDLIST_PENDING: hard_fail_files=$hardFailFiles"
      }
    } elseif ($gate -eq "NO_CLOSE_RETRY_PENDING") {
      Add-RunState $runStates "AGENT03_GATE_OPEN: NO_CLOSE_RETRY_PENDING"
    } elseif ($gate -eq "NO_CLOSE_HARD_FAIL_PRESENT" -or $gate -eq "NO_CLOSE_HARD_FAIL") {
      Add-Warning $warnings "AGENT03_GATE_BLOCKED: $gate"
    } elseif ($gate -eq "CLOSED_OK" -or $gate -eq "CLOSE_OK") {
      Add-RunState $runStates "AGENT03_CLOSE_OK"
    } elseif (![string]::IsNullOrWhiteSpace($gate)) {
      Add-Warning $warnings "AGENT03_GATE_OTHER: $gate"
    }
  }

  Write-Host ""
  Write-Host "files" -ForegroundColor Cyan
  Write-Host ("  download_events_current exists : " + (Test-Path $downloadEventsCsv))
  Write-Host ("  events_current exists          : " + (Test-Path $eventsCsv))
  Write-Host ("  retry_current exists           : " + (Test-Path $retryCsv))
  Write-Host ("  supervisor_events exists       : " + (Test-Path $supervisorEventsCsv))
  Write-Host ("  supervisor_current exists      : " + (Test-Path $supervisorCurrentJson))

  Write-Host ""
  if ($runStates.Count -gt 0) {
    Write-Host "RUN STATE:" -ForegroundColor Cyan
    $runStates | ForEach-Object { Write-Host ("  - " + $_) -ForegroundColor Cyan }
  } else {
    Write-Host "RUN STATE: none" -ForegroundColor DarkGray
  }

  Write-Host ""
  if ($warnings.Count -gt 0) {
    Write-Host "WARNINGS:" -ForegroundColor Yellow
    $warnings | ForEach-Object { Write-Host ("  - " + $_) -ForegroundColor Yellow }
  } else {
    Write-Host "WARNINGS: none" -ForegroundColor Green
  }

  Write-Host ""
  $stateFingerprint = ($runStates -join "|")
  $warningFingerprint = ($warnings -join "|")
  $alertFingerprint = ($alerts -join "|")

  if ($alerts.Count -gt 0) {
    Write-Host "ALERTS:" -ForegroundColor Red
    $alerts | ForEach-Object { Write-Host ("  - " + $_) -ForegroundColor Red }
    if ($BeepOnAlert -and $alertFingerprint -ne $prevAlertFingerprint) {
      try { [console]::Beep(1200, 300); [console]::Beep(900, 300) } catch {}
    }
  } else {
    Write-Host "ALERTS: none" -ForegroundColor Green
  }
  if ($stateFingerprint -ne $prevStateFingerprint -or $warningFingerprint -ne $prevWarningFingerprint -or $alertFingerprint -ne $prevAlertFingerprint) {
    Write-StatusEvents -RunId $RunId -RunDir $runDir -CsvPath $supervisorEventsCsv -Now $now -RunStates $runStates -Warnings $warnings -Alerts $alerts
  }
  Write-CurrentStatusJson -Path $supervisorCurrentJson -Now $now -RunId $RunId -RunDir $runDir -Download $download -Strict $strict -Summary $summary -RunStates $runStates -Warnings $warnings -Alerts $alerts

  $prevStateFingerprint = $stateFingerprint
  $prevWarningFingerprint = $warningFingerprint
  $prevAlertFingerprint = $alertFingerprint

  if ($null -ne $download) {
    $prevDoneOk = [int]$download.done_ok
    $prevDoneBad = [int]$download.done_bad
  }
  if ($null -ne $strict) {
    $prevHard = 0
    if ($strict.severity_counts_current) {
      $h = $strict.severity_counts_current.PSObject.Properties | Where-Object { $_.Name -eq 'HARD_FAIL' }
      if ($h) { $prevHard = [int]$h.Value }
    }
    $prevRetry = [int]$strict.retry_pending_files_current
  }

  Start-Sleep -Seconds $IntervalSec
}
