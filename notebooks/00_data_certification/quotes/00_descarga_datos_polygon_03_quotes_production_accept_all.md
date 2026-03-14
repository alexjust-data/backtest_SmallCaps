## politica operativa de produccion quotes polygon accept-all

Este documento formaliza la corrida de produccion de `quotes` con politica `accept all raw, diagnose all, red-list later`.

## objetivo operativo

El objetivo no es cerrar la corrida con `retry_pending = 0` ni con `hard_fail = 0`.

El objetivo es:

- descargar el universo completo de `quotes`
- materializar un dataset bruto trazable por `RUN_ID`
- clasificar en tiempo real calidad y causas
- consolidar cobertura y patrones por ticker
- emitir una lista roja posterior para `ML` y `backtest`

## regla de aceptacion inicial

Se acepta el dataset bruto inicial siempre que:

- los parquets sean legibles
- la corrida quede trazada por `RUN_ID`
- Agent01, Agent02, Agent03 y supervisor dejen artefactos consistentes

`SOFT_FAIL`, `HARD_FAIL`, `retry_pending` y `retry_frozen_exhausted` no bloquean la aceptacion inicial del bruto.

Se interpretan como senales de revision posterior.

## rol de cada agente

### Agent01

- descarga `ticker,date`
- publica solo parquets completos
- registra estados de descarga
- no decide elegibilidad para backtest/ML

### Agent02

- clasifica cada parquet
- emite severidad, causas y colas de retry
- su `retry_queue` se interpreta como priorizacion diagnostica, no como gate de aceptacion inicial

### Agent03

- consolida cobertura y causas
- emite `gate_status` legacy por compatibilidad
- emite tambien:
  - `acceptance_policy = ACCEPT_ALL_RAW_REVIEW_LATER`
  - `raw_dataset_status = RAW_ACCEPTED_REVIEW_PENDING | RAW_ACCEPTED_REVIEW_COMPLETE`
- prepara la base de la lista roja

### Supervisor

- vigila salud operativa de Agent01/02/03
- diferencia `RUN STATE`, `WARNINGS`, `ALERTS`
- en modo accept-all no trata `NO_CLOSE_*` como bloqueo de aceptacion del bruto

## artefactos clave

- `download_live_status.json`
- `quotes_agent_strict_events_current.csv`
- `retry_queue_quotes_strict_current.csv`
- `retry_frozen_quotes_strict.csv`
- `agent03_outputs\run_summary.json`
- `agent03_outputs\causal_hypotheses\*`
- `supervisor_events_history.csv`
- `supervisor_current_status.json`

## interpretacion operativa

- `RAW_ACCEPTED_REVIEW_PENDING`
  - el bruto ya es aceptable como descarga completa en curso o terminada
  - aun hay revision pendiente para backtest/ML

- `RAW_ACCEPTED_REVIEW_COMPLETE`
  - el bruto esta aceptado y sin pendientes relevantes de revision en esa foto

- `HARD_FAIL`
  - no implica borrar ni rechazar la corrida
  - implica posible inclusion posterior en lista roja

## criterio final de uso

La elegibilidad final para `ML` o `backtest` no se decide en Agent01.

Se decide despues del analisis causal, por ticker o ticker-dia.

Categorias objetivo:

- `usable_raw`
- `review_required`
- `red_list_backtest`
- `red_list_ml`
- `vendor_pathology`
- `split_window_sensitive`
- `illiquid_low_information`

## lanzaderas de produccion

### Agent01

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py `
  --csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\<RUN_ID>\inputs\tasks_quotes_prod.csv `
  --output D:\quotes `
  --concurrent 24 `
  --run-id <RUN_ID> `
  --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\<RUN_ID> `
  --task-batch-size 500 `
  --session-start 04:00:00 `
  --session-end 20:00:00
```

### Agent02

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent02_quotes_strict_loop.ps1" `
  -RunId "<RUN_ID>" `
  -QuotesRoot "D:\quotes" `
  -MaxFiles 50000 `
  -SleepSec 15 `
  -ResetState
```

### Agent03 compact

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent03_monitor_compact.ps1" `
  -RunId "<RUN_ID>" `
  -SleepSec 30
```

### Supervisor

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent_supervisor.ps1" `
  -RunId "<RUN_ID>" `
  -IntervalSec 10 `
  -StallSec 180 `
  -HardFailWarnThreshold 100 `
  -BeepOnAlert
```
