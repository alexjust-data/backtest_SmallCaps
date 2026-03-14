# Descarga y Certificacion de `trades_ticks`

## Objetivo

Este documento describe el estado actual del pipeline de `trades_ticks`, los artefactos reales que deja cada agente, y el procedimiento operativo para continuar exactamente desde el punto donde se qued? una corrida si se va la luz o se interrumpe el proceso.

El contrato operativo del dataset es:

- unidad de tarea: `ticker,date,session`
- session actual de trabajo:
  - `market`
- file esperado por tarea:
  - `{ticker}/year={YYYY}/month={MM}/day={YYYY-MM-DD}/market.parquet`

Ejemplo de file esperado:

- `D:\trades_ticks_prod_2005_2026\AABA\year=2017\month=06\day=2017-06-16\market.parquet`

## Estado actual de la arquitectura

Se trabaja con 4 agentes y un generador de tareas:

- `205_generate_trades_ticks_tasks.py`
- `201_agent1_download_trades_ticks_realtime.py`
- `202_agent2_validate_trades_ticks_strict.py`
- `203_agent3_monitor_trades_ticks_coverage.py`
- `204_agent4_supervisor_trades_ticks.py`

Notebooks asociados:

- `agent_01_download_trades_ticks_realtime.ipynb`
- `agent_02_validate_trades_ticks_realtime.ipynb`
- `agent_03_monitor_trades_ticks_realtime.ipynb`
- `agent_04_supervisor_trades_ticks_realtime.ipynb`

Path de notebooks:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\trades_tiks`

Path de scripts:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification`

## Fuente de verdad del run

La fuente de verdad del run no es el disco ni el output parcial del supervisor. La fuente de verdad es:

- `tasks_trades_ticks.csv`

Path real por run:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\<RUN_ID>\inputs\tasks_trades_ticks.csv`

Sem?ntica:

- define expl?citamente el conjunto esperado de tareas `ticker,date`
- todo lo dem?s se contrasta contra ese CSV

Artefactos complementarios generados por `205`:

- `tickers_trades_ticks.csv`
- `tasks_trades_ticks_meta.json`

## Generador de tareas (`205`)

Script:

- `205_generate_trades_ticks_tasks.py`

Qu? hace:

- modo `smoke`
  - crea unas pocas tareas manuales
- modo `lifecycle`
  - usa universo + lifecycle oficial
  - expande a business days
  - genera `tasks_trades_ticks.csv`

Fuentes de base:

- universo PTI
- `official_lifecycle_compiled.csv`

Comando real para producci?n:

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\205_generate_trades_ticks_tasks.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --mode lifecycle --cutoff 2026-03-14
```

Resultado esperado:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv`

## Agente 01: descarga (`201`)

Script:

- `201_agent1_download_trades_ticks_realtime.py`

### Qu? hace

- consume `tasks_trades_ticks.csv`
- llama a Polygon `v3/trades/{ticker}`
- filtra por ventana horaria de sesi?n
- pagina por `next_url`
- normaliza a parquet at?mico
- escribe un estado persistente por tarea

### Esquema normalizado actual

- `ticker`
- `date`
- `timestamp`
- `price`
- `size`
- `exchange`
- `conditions`
- `year`
- `month`
- `day`

### Estados posibles de descarga

- `DOWNLOADED_OK`
- `DOWNLOADED_EMPTY`
- `DOWNLOAD_FAIL`

### Qu? qued? refactorizado

Agent01 ya tiene:

- `resume` por `task_key`
- retries con backoff exponencial + jitter
- m?tricas por batch
- pol?tica expl?cita de concurrencia recomendada

Argumentos nuevos relevantes:

- `--max-retries`
- `--retry-base-sleep-sec`
- `--retry-max-sleep-sec`

Pol?tica de concurrencia recomendada:

- `<= 8`
  - `stable_default`
- `9-12`
  - `fast_balanced`
- `> 12`
  - `aggressive_monitor_429`

### Artefactos del run

En el `RUN_DIR`:

- `download_events_trades_ticks_current.csv`
- `download_events_trades_ticks_history.csv`
- `live_status_trades_ticks_download.json`
- `run_config_trades_ticks_download.json`
- `expected_manifest_trades_ticks.csv`

En disco de datos:

- `D:\trades_ticks_prod_2005_2026\...\market.parquet`

### Qu? significan estos artefactos si se va la luz

Esto es cr?tico:

- `download_events_trades_ticks_current.csv`
  - snapshot actual por tarea
- `download_events_trades_ticks_history.csv`
  - hist?rico acumulado de eventos de descarga
- `expected_manifest_trades_ticks.csv`
  - manifiesto esperado de `task_key -> expected_file`

Con `--resume`, Agent01:

1. lee `download_events_trades_ticks_current.csv`
2. considera ya resueltas las tareas con:
   - `DOWNLOADED_OK`
   - `DOWNLOADED_EMPTY`
3. adem?s escanea disco y si ve un parquet ya existente y legible lo marca como:
   - `resume_existing_file`

### Conclusi?n operativa

Si se va la luz:

- **no reinicia desde cero**
- reanuda por `task_key = ticker|date|session`
- respeta tareas ya aceptadas
- vuelve a intentar solo lo que no est? resuelto o lo que fall?

### Comando de producci?n actual

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\201_agent1_download_trades_ticks_realtime.py --csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv --output D:\trades_ticks_prod_2005_2026 --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --session market --concurrent 8 --task-batch-size 200 --max-retries 4 --retry-base-sleep-sec 0.75 --retry-max-sleep-sec 12.0 --resume
```

## Agente 02: validaci?n estricta (`202`)

Script:

- `202_agent2_validate_trades_ticks_strict.py`

### Qu? hace

- escanea los `market.parquet` reales en disco
- valida estructura m?nima y calidad por file
- clasifica cada file en:
  - `PASS`
  - `SOFT_FAIL`
  - `HARD_FAIL`
- genera una cola de revisi?n
- contrasta esperado vs encontrado usando la sem?ntica real de Agent01

### Pol?tica global

- `ACCEPT_ALL_RAW_DIAGNOSE_LATER`

Eso significa:

- el bruto se acepta primero
- la clasificaci?n de calidad se hace despu?s
- la exclusi?n para research/backtest/ML no la decide Agent01 ni Agent02 por s? solos

### Qu? valida exactamente

Campos requeridos:

- `ticker`
- `date`
- `timestamp`
- `price`
- `size`
- `exchange`
- `conditions`

Checks actuales:

- `parquet_unreadable`
- `missing_required_cols`
- `empty_file`
- `null_timestamp_rows`
- `timestamp_not_monotonic`
- `nonpositive_price_rows`
- `negative_size_rows`
- duplicados exactos de trades con clave:
  - `timestamp`
  - `price`
  - `size`
  - `exchange`
  - `conditions_repr`

### Pol?tica refinada de duplicados

M?tricas actuales:

- `duplicate_group_rows`
- `duplicate_group_ratio_pct`
- `duplicate_excess_rows`
- `duplicate_excess_ratio_pct`
- `adjacent_exact_repeats`

Decision actual:

- `HARD_FAIL`
  - si `duplicate_excess_ratio_pct > 10.0`
- `SOFT_FAIL`
  - si `duplicate_excess_ratio_pct > 3.0`
- `SOFT_FAIL`
  - si hay duplicados exactos por debajo del umbral

### Artefactos del Agente 02

- `trades_ticks_agent_events_current.csv`
- `trades_ticks_agent_events_history.csv`
- `review_queue_trades_ticks_current.csv`
- `expected_vs_found_trades_ticks.csv`
- `live_status_trades_ticks_strict.json`
- `run_config_trades_ticks_strict.json`

### Qu? qued? refactorizado

Agent02 ya no trata todo lo no encontrado como `missing` sin matiz.

Ahora separa expl?citamente:

- `FOUND_FILE`
- `DOWNLOADED_EMPTY`
- `DOWNLOAD_FAIL`
- `EXPECTED_MISSING`

Y usa la salida de Agent01 (`download_events_trades_ticks_current.csv`) como parte del contrato esperado/encontrado.

### Si se va la luz

Agent02 no es incremental fino como Agent01. Su sem?ntica actual es:

- relee el estado actual del disco
- revalida los files visibles
- reconstruye el snapshot actual

Importante:

- si lo relanzas **sin** `--reset-state`
  - no borra el estado previo
  - pero reescanea el conjunto visible
- si lo relanzas **con** `--reset-state`
  - borra snapshot y reconstruye desde cero

### Recomendaci?n tras corte de luz

Si quieres continuar sin borrar la visi?n actual:

- **no usar `--reset-state`**

Comando recomendado:

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\202_agent2_validate_trades_ticks_strict.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --probe-root D:\trades_ticks_prod_2005_2026 --max-files 50000 --sleep-sec 15 --expected-csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv
```

Para una sola foto puntual:

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\202_agent2_validate_trades_ticks_strict.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --probe-root D:\trades_ticks_prod_2005_2026 --max-files 50000 --sleep-sec 15 --one-shot --expected-csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv
```

## Agente 03: cobertura, sem?ntica y review granular (`203` + `206`)

Scripts:

- `203_agent3_monitor_trades_ticks_coverage.py`
- `206_agent3_trades_ticks_granular_review.py`

### Qu? hace `203`

- monitoriza de forma continua el estado del run
- resume expected/found/empty/fail/missing
- consolida cobertura por ticker
- separa tickers esperados de tickers realmente observados

### Qu? qued? refactorizado en `203`

1. ahora puede correr en:
- modo continuo
- modo `--one-shot`

2. el resumen ya calcula directamente desde Agent01:
- `expected_found = DOWNLOADED_OK`
- `expected_empty = DOWNLOADED_EMPTY`
- `download_fail_tasks = DOWNLOAD_FAIL`
- `expected_missing = total - found - empty - fail`

3. se corrigi? la sem?ntica por ticker:
- un ticker con `files = 0` ya no sale como aceptado
- ahora sale como:
  - `NOT_OBSERVED_YET`

4. se a?adi? un output nuevo:
- `observed_status_by_ticker.csv`

### Artefactos de `203`

Dentro de:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\<RUN_ID>\agent03_trades_ticks_outputs`

Quedan:

- `run_summary.json`
- `coverage_by_ticker.csv`
- `observed_status_by_ticker.csv`
- `causes_by_ticker.csv`
- `expected_vs_found_trades_ticks.csv` (copia si existe)

### Sem?ntica correcta de estados por ticker

- `NOT_OBSERVED_YET`
  - ticker esperado pero a?n no tocado por el pipeline
- `REVIEW_QUEUE_PENDING`
  - ticker observado con files pendientes de revisi?n
- `OBSERVED_ACCEPTED`
  - ticker observado, sin review queue ni hard fail

### Uso correcto de outputs

- `coverage_by_ticker.csv`
  - universo esperado completo del run
- `observed_status_by_ticker.csv`
  - solo tickers ya tocados por el pipeline

### Si se va la luz

`203` no necesita ?reanudar? como Agent01. Solo necesita volver a leer los artefactos actuales de Agent01 y Agent02.

Comando continuo recomendado:

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\203_agent3_monitor_trades_ticks_coverage.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --expected-csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv --interval-sec 15
```

Comando puntual:

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\203_agent3_monitor_trades_ticks_coverage.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --expected-csv C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv --one-shot
```

### Qu? hace `206`

`206` es la capa de an?lisis granular visual.

Hace:

- conteo de tareas por estado final
- top tickers por hallazgos
- mapa `ticker x fecha`
- tabla `task_status_detail.csv`
- explorer visual por `ticker/date/status`

L?gica de exploraci?n importante:

- si `PASS / SOFT_FAIL / HARD_FAIL`
  - intenta dibujar el `market.parquet` intrad?a
- si `DOWNLOADED_EMPTY`
  - mira primero `D:\ohlcv_1m`
  - si no existe `1m` local ese d?a exacto, cae a `D:\ohlcv_daily`

Esto es clave para interpretar correctamente un vac?o.

## Agente 04: supervisor (`204`)

Script:

- `204_agent4_supervisor_trades_ticks.py`

### Qu? hace

- consolida Agent01, Agent02 y Agent03
- imprime un estado operativo homog?neo al de `quotes`
- calcula `age_sec`
- detecta `STALL`
- persiste snapshot actual e hist?rico del supervisor

### Artefactos

- `trades_ticks_supervisor_current_status.json`
- `trades_ticks_supervisor_events_history.csv`

### Comando recomendado

```powershell
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\204_agent4_supervisor_trades_ticks.py --run-id trades_ticks_prod_2005_2026 --run-dir C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026 --interval-sec 15 --stall-sec 180
```

## Qu? hacer si se va la luz

### Principio operativo

No improvisar. Mirar primero los artefactos persistidos del `RUN_DIR`.

### Paso 1: verificar que el `RUN_ID` correcto sigue siendo el mismo

Para producci?n actual:

- `RUN_ID = trades_ticks_prod_2005_2026`

### Paso 2: comprobar que sigue existiendo la fuente de verdad

Debe existir:

- `C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\trades_ticks_prod_2005_2026\inputs\tasks_trades_ticks.csv`

Si no existe, no relanzar agentes.

### Paso 3: revisar el estado persistido de Agent01

Mirar:

- `download_events_trades_ticks_current.csv`
- `live_status_trades_ticks_download.json`

Qu? esperar:

- tareas ya resueltas en `DOWNLOADED_OK` o `DOWNLOADED_EMPTY`
- snapshot con `done_ok`, `done_bad`, `pending`

### Paso 4: relanzar Agent01 con `--resume`

No quitar `--resume`.

Eso har? que:

- respete tareas ya cerradas
- no recomience desde cero
- escanee disco y reinyecte `resume_existing_file` si hace falta

### Paso 5: relanzar Agent02 sin `--reset-state`

Si quieres continuar sin destruir el snapshot operativo:

- no usar `--reset-state`

### Paso 6: relanzar Agent03 continuo

Para que reconstruya la visi?n consolidada desde los artefactos actuales.

### Paso 7: relanzar Agent04 supervisor

Para recuperar visi?n operativa homog?nea del run.

## Qu? esperamos que suceda tras relanzar

### Agent01

Esperamos:

- que lea `download_events_trades_ticks_current.csv`
- que compute `tasks_already_ok > 0`
- que reanude desde el punto donde iba
- que `tasks_to_process` sea menor que `tasks_total`

Si vuelve a salir:

- `tasks_already_ok = 0`

entonces algo est? mal con el `RUN_DIR`, el CSV actual o la ra?z de output.

### Agent02

Esperamos:

- que vea los files ya descargados
- que no borre el snapshot si no se usa `--reset-state`
- que siga incrementando la visi?n actual del conjunto visible

### Agent03

Esperamos:

- que reconstruya `expected_found`, `expected_empty`, `expected_missing` de forma coherente
- que `expected_missing = total - found - empty - fail`
- que los tickers no observados aparezcan como `NOT_OBSERVED_YET`

### Agent04

Esperamos:

- mismo estilo visual del supervisor de `quotes`
- bloques:
  - `AGENT01`
  - `AGENT02`
  - `AGENT03`
  - `files`
  - `RUN STATE`
  - `WARNINGS`
  - `ALERTS`
- y alertas de `STALL` si un agente deja de actualizar snapshot

## Celdas ?tiles de notebooks

### Notebook Agent01

Path:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\trades_tiks\agent_01_download_trades_ticks_realtime.ipynb`

Qu? debe permitir:

- configurar `RUN_ID`, `RUN_DIR`, `OUTPUT_ROOT`
- lanzar con `resume`
- ver `live_status` y m?tricas de batch

### Notebook Agent02

Path:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\trades_tiks\agent_02_validate_trades_ticks_realtime.ipynb`

Qu? debe permitir:

- lanzar continuo o puntual
- inspeccionar `live_status_trades_ticks_strict.json`
- ver `review_queue` y `expected_vs_found`

### Notebook Agent03

Path:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\trades_tiks\agent_03_monitor_trades_ticks_realtime.ipynb`

Qu? debe permitir:

- lanzar `203` en modo continuo o `one-shot`
- ejecutar `206` para review granular visual
- inspeccionar:
  - `coverage_by_ticker.csv`
  - `observed_status_by_ticker.csv`
  - `expected_vs_found_trades_ticks.csv`

### Notebook Agent04

Path:

- `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\trades_tiks\agent_04_supervisor_trades_ticks_realtime.ipynb`

Qu? debe permitir:

- lanzar supervisor continuo
- leer `trades_ticks_supervisor_current_status.json`

## Resumen operativo final

Si se va la luz y quieres seguir igual:

1. no cambies `RUN_ID`
2. no cambies `OUTPUT_ROOT`
3. verifica `tasks_trades_ticks.csv`
4. relanza Agent01 con `--resume`
5. relanza Agent02 sin `--reset-state`
6. relanza Agent03 continuo
7. relanza Agent04 supervisor

La continuidad real del run est? soportada por:

- `tasks_trades_ticks.csv`
- `download_events_trades_ticks_current.csv`
- `expected_manifest_trades_ticks.csv`
- `live_status_trades_ticks_download.json`

Ese es el n?cleo que permite continuar donde se qued? la descarga.
