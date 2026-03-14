# Runbook Operativo de `quotes` en Produccion

## Scope

Este documento resume la arquitectura real que est? corriendo ahora para `quotes`, los artefactos actuales del run, y el procedimiento correcto para recuperar la corrida si se va la luz o se interrumpe un agente.

Run activo documentado:

- `RUN_ID = 20260313_quotes_prod_full_12133_clean`
- `RUN_DIR = C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260313_quotes_prod_full_12133_clean`
- `QUOTES_ROOT = D:\quotes`

## Estado operativo actual

Snapshot operativo observado durante esta fase:

- Agent01:
  - `processed=812500/3066263`
  - `batch_done=812500/3066263`
- Agent03 compact:
  - `processed = 189286`
  - `retry_pending = 252514`
  - `hard_fail = 28392`
  - `gate = NO_CLOSE_RETRY_PENDING`
  - `raw_dataset_status = RAW_ACCEPTED_REVIEW_PENDING`
  - `acceptance_policy = ACCEPT_ALL_RAW_REVIEW_LATER`
  - `mean_coverage_ok = 0.3999364619031087`
- Agent02 strict:
  - `processed_total = 189286`
  - `pending = 17137`
  - `pass = 344324`
  - `soft = 232840`
  - `hard = 28392`
  - `retry = 252514`
  - `gate = NO_CLOSE_RETRY_PENDING`

Interpretaci?n correcta:

- la descarga sigue avanzando
- el bruto se est? aceptando con pol?tica `ACCEPT_ALL_RAW_REVIEW_LATER`
- existe una cola grande de revisi?n posterior
- no se est? esperando `retry_pending = 0` para aceptar el bruto

## Fuente de verdad del run

La fuente de verdad de tareas del run es:

- `tasks_quotes_prod.csv`

Path real:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260313_quotes_prod_full_12133_clean\inputs\tasks_quotes_prod.csv`

Artefactos complementarios del input:

- `tickers_quotes_prod.csv`
- `tasks_quotes_prod_meta.json`

Esto define el conjunto esperado de tareas `ticker,date`.

## Arquitectura actual

### Agent01 download

Entry point real:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py`

Launcher real:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent01_quotes.ps1`

Sem?ntica:

- descarga `quotes` de Polygon
- escribe parquets at?micos
- tiene `resume`
- usa estado por tarea y manifiesto de descarga

Artefactos del run:

- `download_events_current.csv`
- `download_events_history.csv`
- `download_live_status.json`
- `download_retry_queue_current.csv`
- `download_state.json`
- `batch_manifest_quotes_strict.csv`
- `quotes_discovered_index.parquet`
- `quotes_discovery_state.json`
- `quotes_pending_queue.parquet`
- `quotes_reconciliation_status.json`

### Agent02 strict validation

Script real que corre en loop:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent02_quotes_strict_loop.ps1`

Launcher real:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent02_quotes.ps1`

Sem?ntica:

- valida parquets de quotes
- clasifica severidad y causas
- mantiene colas `retry_queue` y `retry_frozen`
- publica snapshot vivo de validaci?n

Artefactos del run:

- `live_status_quotes_strict.json`
- `quotes_agent_strict_events_current.csv`
- `quotes_agent_strict_events_history.csv`
- `quotes_agent_strict_state.json`
- `retry_attempts_quotes_strict.csv`
- `retry_queue_quotes_strict.csv`
- `retry_queue_quotes_strict.parquet`
- `retry_queue_quotes_strict_current.csv`
- `retry_frozen_quotes_strict.csv`
- `run_config_quotes_strict.json`

### Agent03 coverage summary

Modo operativo real actual:

- compact monitor basado en `036`

Launchers y scripts relevantes:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent03_quotes.ps1`
- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent03_monitor_compact.ps1`
- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\036_agent3_quotes_coverage_and_causes.py`

Scripts anal?ticos relevantes del notebook de Agent03:

- `036_agent3_quotes_coverage_and_causes.py`
- `037_agent3_diagnostics_tables_hist.py`
- `041_agent3_examples_independent.py`
- `044_agent3_go_nogo_review.py`
- `045_agent3_causal_hypotheses.py`
- `046_agent3_halt_contrast.py`

Notebook principal actual de producci?n:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\quotes\agent_03_monitor_coverage_and_stats_realtime_v2_production.ipynb`

Artefactos del run:

- `agent03_outputs\run_summary.json`
- `agent03_outputs\coverage_by_ticker.csv`
- `agent03_outputs\causes_by_ticker.csv`
- subcarpetas de an?lisis como `causal_hypotheses`

Sem?ntica operativa actual:

- `acceptance_policy = ACCEPT_ALL_RAW_REVIEW_LATER`
- `raw_dataset_status = RAW_ACCEPTED_REVIEW_PENDING | RAW_ACCEPTED_REVIEW_COMPLETE`

### Supervisor

Supervisor real actual:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\run_agent_supervisor.ps1`

Launcher real:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_supervisor_quotes.ps1`

Artefactos:

- `supervisor_current_status.json`
- `supervisor_events_history.csv`

Sem?ntica:

- observa Agent01, Agent02, Agent03
- distingue `RUN STATE`, `WARNINGS`, `ALERTS`
- dispara alertas de `STALL` por edad de snapshot

## Pol?tica operativa actual

La pol?tica vigente de la corrida es:

- `ACCEPT_ALL_RAW_REVIEW_LATER`

Eso significa:

- el bruto se descarga completo y se acepta como materia prima trazable
- `SOFT_FAIL`, `HARD_FAIL`, `retry_pending` y `retry_frozen` no bloquean la aceptaci?n inicial del bruto
- la decisi?n final para `backtest` o `ML` se toma despu?s, no en Agent01

Lectura correcta:

- `retry_pending` es una cola diagn?stica / operativa
- `hard_fail` es una se?al fuerte de revisi?n posterior
- `NO_CLOSE_RETRY_PENDING` no significa ?tirar la corrida?
- significa ?el bruto sigue aceptado pero la revisi?n no ha terminado?

## Qu? artefactos importan si se va la luz

### N?cleo duro del estado

Estos son los artefactos que permiten recuperar el punto real del run:

- `inputs\tasks_quotes_prod.csv`
- `download_events_current.csv`
- `download_live_status.json`
- `download_state.json`
- `live_status_quotes_strict.json`
- `quotes_agent_strict_events_current.csv`
- `retry_queue_quotes_strict_current.csv`
- `retry_frozen_quotes_strict.csv`
- `agent03_outputs\run_summary.json`
- `supervisor_current_status.json`

### Qu? significan

- `tasks_quotes_prod.csv`
  - contrato esperado del run
- `download_events_current.csv`
  - snapshot actual por tarea de descarga
- `download_live_status.json`
  - progreso agregado de Agent01
- `live_status_quotes_strict.json`
  - progreso agregado de Agent02
- `quotes_agent_strict_events_current.csv`
  - snapshot actual validado de quotes
- `retry_queue_quotes_strict_current.csv`
  - cola viva de revisi?n posterior
- `agent03_outputs\run_summary.json`
  - visi?n consolidada de cobertura y sem?ntica accept-all

## C?mo reiniciar tras corte de luz

### Principio general

No cambiar de `RUN_ID`. No cambiar de `RUN_DIR`. No lanzar nada con `ResetState` salvo que se quiera recomputar conscientemente.

### Paso 1: verificar el run

Debe seguir existiendo:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260313_quotes_prod_full_12133_clean`

Y dentro:

- `inputs\tasks_quotes_prod.csv`
- `download_events_current.csv`
- `download_live_status.json`

### Paso 2: relanzar Agent01 con resume

Launcher real:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent01_quotes.ps1" -RunId "20260313_quotes_prod_full_12133_clean" -CsvPath "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260313_quotes_prod_full_12133_clean\inputs\tasks_quotes_prod.csv"
```

Notas:

- el launcher ya mete `--resume`
- Agent01 debe leer `download_events_current.csv`
- no debe recomenzar desde cero

Qu? se espera ver si el restart fue sano:

- `tasks_already_ok > 0`
- `tasks_to_process < tasks_total`
- la descarga sigue desde donde iba

### Paso 3: relanzar Agent02

Si quieres continuar la visi?n operativa tal como estaba:

- **no usar `-ResetState`**

Comando recomendado:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent02_quotes.ps1" -RunId "20260313_quotes_prod_full_12133_clean" -MaxFiles 50000
```

Solo usar `-ResetState` si quieres reconstruir el snapshot de Agent02 desde cero.

### Paso 4: relanzar Agent03 compact

Comando real:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_agent03_quotes.ps1" -RunId "20260313_quotes_prod_full_12133_clean"
```

Qu? debe hacer:

- volver a leer `live_status_quotes_strict.json`
- regenerar `agent03_outputs\run_summary.json`
- recuperar `processed`, `retry_pending`, `hard_fail`, `mean_coverage_ok`

### Paso 5: relanzar Supervisor

Comando real:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agents\launch_prod_supervisor_quotes.ps1" -RunId "20260313_quotes_prod_full_12133_clean"
```

Qu? debe hacer:

- reconstruir `RUN STATE`
- recuperar `WARNINGS` y `ALERTS`
- volver a calcular `age_sec`
- volver a alertar `STALL` si aplica

## Qu? esperamos que pase tras relanzar

### Agent01

Esperamos que:

- lea el estado previo
- contin?e el progreso de `done_ok`
- no reinicie a `0`
- mantenga `pending = tasks_total - done_ok`

### Agent02

Esperamos que:

- reconstruya o contin?e el snapshot de validaci?n
- mantenga la sem?ntica de `retry_pending` y `hard_fail`
- si el proceso vuelve a quedarse vivo pero sin refrescar `live_status_quotes_strict.json`, el supervisor lo marcar? como `STALL`

### Agent03

Esperamos que:

- publique de nuevo `raw_dataset_status = RAW_ACCEPTED_REVIEW_PENDING`
- siga mostrando `acceptance_policy = ACCEPT_ALL_RAW_REVIEW_LATER`
- recupere la cobertura media y el estado del retry backlog

### Supervisor

Esperamos que:

- diferencie claramente:
  - descarga viva
  - validaci?n viva o estancada
  - resumen consolidado
- marque `AGENT02_STALL` si el `updated_utc` de Agent02 envejece demasiado

## Interpretaci?n correcta del estado actual

Con los n?meros actuales:

- Agent01 sigue descargando
- Agent02 sigue validando, aunque puede entrar en `STALL`
- Agent03 mantiene la tesis accept-all
- el problema operativo dominante sigue siendo la estabilidad del loop de Agent02, no Agent01

Eso implica:

- el bruto puede seguir creciendo y acept?ndose
- pero la observabilidad y la clasificaci?n estricta dependen de relanzar Agent02 si se queda congelado

## Notebooks relevantes

### Agent01 notebook

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\quotes\agent_01_download_quotes_realtime.ipynb`

### Agent02 notebook

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\quotes\agent_02_validate_quotes_trades_realtime.ipynb`

### Agent03 notebook principal actual

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\quotes\agent_03_monitor_coverage_and_stats_realtime_v2_production.ipynb`

### Documentos previos en la carpeta

- `00_descarga_datos_polygon_02_quotes_realtime.md`
- `00_descarga_datos_polygon_03_quotes_production_accept_all.md`

## Resumen ejecutivo

Si se va la luz y quieres seguir igual:

1. no cambies el `RUN_ID`
2. verifica `tasks_quotes_prod.csv`
3. relanza Agent01 con el launcher de producci?n
4. relanza Agent02 sin `ResetState` si quieres continuidad operativa
5. relanza Agent03 compact
6. relanza supervisor

La continuidad real del run de `quotes` se apoya en:

- `tasks_quotes_prod.csv`
- `download_events_current.csv`
- `download_live_status.json`
- `live_status_quotes_strict.json`
- `quotes_agent_strict_events_current.csv`
- `agent03_outputs\run_summary.json`

Ese es el n?cleo que permite continuar donde se qued? la corrida.
