## proceso operativo completo para descarga de quotes polygon en tiempo real

Este documento deja cerrado el flujo completo que se va a ejecutar para descargar `quotes` desde Polygon
sin dejar huecos entre descarga, validacion, monitorizacion y reintentos.

La idea no es solo “bajar parquet”.

La idea es:

- descargar `quotes`
- publicarlos solo cuando el dia este completo
- validar en tiempo real lo que entra en disco
- medir cobertura en la misma corrida
- reintentar automaticamente lo que falle
- y no cerrar la corrida hasta que no queden puntos operativos abiertos


## principio rector del flujo

No vamos a considerar correcta una descarga solo porque exista un archivo.

Para que el proceso sea aceptable, cada dia `ticker/date` tiene que pasar por estas capas:

- descarga desde Polygon
- escritura segura en disco
- validacion tecnica del parquet
- inclusion en metrica de cobertura
- exclusion o inclusion explicita en retry
- cierre de corrida con trazabilidad

Si una sola de esas capas falla, el dia no se da por cerrado.


## rutas operativas de esta corrida

- root de quotes observado y descargado:
  - `D:\quotes`

- proyecto de orquestacion:
  - `C:\TSIS_Data\v1\backtest_SmallCaps`

- notebook base de descarga:
  - `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\agent_01_download_quotes_realtime.ipynb`

- notebook de validacion realtime:
  - `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\agent_02_validate_quotes_trades_realtime.ipynb`

- notebook de monitorizacion/cobertura:
  - `C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\00_data_certification\agent_03_monitor_coverage_and_stats_realtime_v2_clean.ipynb`

- downloader operativo:
  - `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py`


## que problemas teniamos antes y como quedan resueltos

El flujo anterior tenia cuatro riesgos importantes:

- se podian publicar dias parciales en disco
- se usaba una ventana horaria fija `-05:00` que rompe DST
- habia mezcla de rutas `C:\TSIS_Data\...` vs `D:\quotes`
- se usaba existencia de archivo como proxy de exito

El downloader operativo ya deja corregido esto:

- usa timezone de mercado `America/New_York`
- no limita paginas por defecto
- trabaja por `task_key = ticker|date`
- y solo publica `quotes.parquet` en `D:\quotes` cuando el dia esta completo

Esto es importante:

- si una descarga se queda `DOWNLOAD_PARTIAL`, no aparece un parquet “falso bueno” en `D:\quotes`
- si Polygon devuelve vacio provisional, tampoco se publica un parquet vacio en la raiz vigilada

Eso evita que Agent 02 y Agent 03 certifiquen sin querer un dia incompleto.


## arquitectura del flujo 01 -> 02 -> 03

**Agente 01: descarga**

- consume un CSV `ticker,date`
- descarga desde `v3/quotes/{ticker}`
- pagina hasta terminar
- si el dia queda completo, escribe:
  - `D:\quotes\<ticker>\year=YYYY\month=MM\day=DD\quotes.parquet`
- si el dia queda parcial o fallido:
  - no publica parquet final en `D:\quotes`
  - registra el estado del dia en artefactos de corrida

**Agente 02: validacion strict realtime**

- escanea lo nuevo que aparece en `D:\quotes`
- valida:
  - parquet legible
  - path valido
  - columnas requeridas
  - filas > 0
  - tipos correctos
  - anomalias bid/ask
- genera:
  - `quotes_agent_strict_events_current.csv`
  - `retry_queue_quotes_strict_current.csv`
  - `retry_attempts_quotes_strict.csv`
  - `retry_frozen_quotes_strict.csv`

**Agente 03: cobertura + causas**

- consume la salida actual del Agente 02
- calcula cobertura por ticker contra ventana esperada
- clasifica por causa operativa y causa tecnica
- deja claro por que un ticker no puede cerrarse:
  - `RETRY_PENDING`
  - `HARD_FAIL`
  - `LOW_COVERAGE`


## orden exacto de ejecucion

El orden correcto es este y no se debe invertir:

1. preparar `RUN_ID` estable
2. generar o cargar lote `ticker,date`
3. lanzar Agente 01 contra `D:\quotes`
4. lanzar Agente 02 con el mismo `RUN_ID` y la misma raiz `D:\quotes`
5. lanzar Agente 03 con ese mismo `RUN_ID`
6. dejar correr los tres en paralelo
7. cuando Agent 02 produzca retry queue, relanzar Agente 01 sobre esa cola
8. repetir hasta que no queden pendientes reales
9. cerrar corrida solo cuando no haya `retry_pending`, ni `hard_fail`, ni `low_coverage` por causa operativa no explicada


## paso 1: fijar un RUN_ID estable

Toda la sesion debe usar el mismo `RUN_ID`.

No hay que cambiarlo a mitad.

Ejemplo:

```python
RUN_ID = "20260312_quotes_session_01"
```

Ese `RUN_ID` va a vivir durante:

- descarga base
- validacion realtime
- monitorizacion
- rondas de retry

Si cambias el `RUN_ID` en mitad del flujo:

- cortas la trazabilidad
- separas `events_current` de su cobertura
- y mezclas retries de una corrida con otra


## paso 2: preparar el lote ticker-date

El lote que consume el downloader debe tener:

- columna `ticker`
- columna `date`

Formato esperado:

```csv
ticker,date
AABA,2019-01-02
AABA,2019-01-03
AAI,2004-05-28
```

Ese lote puede venir de:

- ping master
- universo filtrado
- CSV P95 ya generado
- o retry convertido desde `file -> ticker,date`

No se debe pasar al downloader un CSV con otras columnas pensando que “las ignorara sin problema”.

Lo seguro es dejarlo exactamente en `ticker,date`.


## paso 3: lanzar Agent 01 downloader

Notebook:

- `agent_01_download_quotes_realtime.ipynb`

Script real que ejecuta:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py`

CLI compatible:

```bash
python "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py" ^
  --csv "C:\ruta\al\tasks_quotes.csv" ^
  --output "D:\quotes" ^
  --concurrent 80 ^
  --run-id "20260312_quotes_session_01" ^
  --run-dir "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260312_quotes_session_01" ^
  --resume
```

Puntos importantes:

- `--output` debe ser `D:\quotes`
- `--run-id` debe coincidir con 02 y 03
- `--run-dir` debe ser unico para esa corrida
- `--resume` sirve para saltar `task_key` ya cerradas en OK


## paso 4: lanzar Agent 02 strict realtime

Agent 02 tiene que mirar `D:\quotes`.

Ese punto es obligatorio.

Si Agent 02 mira `C:\TSIS_Data\data\quotes_p95` mientras Agent 01 descarga en `D:\quotes`, el pipeline queda roto:

- la descarga ira por un lado
- la validacion por otro
- y Agent 03 monitorizara una realidad que no es la corrida actual

Se puede lanzar:

- via notebook
- o via PowerShell wrapper

Si se usa el wrapper, hay que pasar explicitamente:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent02_quotes_strict_loop.ps1" -RunId "20260312_quotes_session_01" -QuotesRoot "D:\quotes" -MaxFiles 5000 -SleepSec 15
```

Si se usa el notebook directamente, la celda inicial de configuracion debe quedar conceptualmente asi:

```python
EXPECTED_QUOTES_ROOT = Path(r"D:\quotes")
PROBE_ROOT = EXPECTED_QUOTES_ROOT
```


## paso 5: lanzar Agent 03 monitor de cobertura y causas

Agent 03 no necesita apuntar a la raiz de quotes si ya consume bien la salida de Agent 02.

Lo que necesita es:

- mismo `RUN_ID`
- mismo `RUN_DIR`
- leer:
  - `quotes_agent_strict_events_current.csv`
  - `retry_queue_quotes_strict_current.csv`
  - `batch_manifest_quotes_strict.csv`
  - `run_config_quotes_strict.json`

Lanzamiento tipico:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_live_fast.ps1" -RunId "20260312_quotes_session_01" -IntervalSec 5
```

Monitor compacto:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_monitor_compact.ps1" -RunId "20260312_quotes_session_01" -SleepSec 30
```


## paso 6: dejar correr en paralelo

Una vez lanzados 01, 02 y 03:

- 01 descarga y publica solo dias completos
- 02 valida lo que aparece en `D:\quotes`
- 03 resume cobertura y causas

Esto significa que la vista operativa correcta durante la corrida es:

- Terminal A:
  - downloader

- Terminal B:
  - validador strict loop

- Terminal C:
  - monitor cobertura/causas

No hay que esperar a “terminar la descarga” para validar.

Precisamente este flujo existe para no descubrir al final que:

- habia parciales
- habia errores de esquema
- o habia una cola grande de retry no atendida


## paso 7: como se cierra el loop de retry

El retry de 02 no sale directamente como `ticker,date`.

Sale como `file`.

Por eso el loop correcto es:

- Agent 02 detecta archivo malo o pendiente
- genera `retry_queue_quotes_strict_current.csv`
- se convierte `file -> ticker,date`
- Agent 01 se relanza solo con ese lote
- Agent 02 vuelve a validar

Eso no se mete dentro del validador.

Se deja desacoplado por archivos para mantener:

- trazabilidad
- reproducibilidad
- y menor acoplamiento operativo


## paso 8: que estados del downloader existen y que significan

Estados relevantes del downloader:

- `DOWNLOADED_OK`
  - descarga completa y parquet final publicado

- `DOWNLOADED_EMPTY`
  - Polygon devolvio vacio en esta ronda, pero todavia no se confirma como vacio persistente

- `EMPTY_CONFIRMED`
  - se repitio el vacio hasta el umbral de confirmacion
  - no se publica parquet final en `D:\quotes`
  - queda para revision operativa si aplica

- `DOWNLOAD_PARTIAL`
  - hubo datos, pero la descarga del dia no cerro limpia
  - no se considera cerrada
  - no se debe certificar por Agent 02 como dia valido

- `DOWNLOAD_FAIL`
  - no se pudo completar la tarea

Punto clave:

- `DOWNLOAD_PARTIAL` y `DOWNLOAD_FAIL` no deben traducirse en un parquet visible en `D:\quotes`


## paso 9: que valida Agent 02 exactamente

Agent 02 valida al menos:

- parquet legible
- particion valida:
  - `ticker/year=YYYY/month=MM/day=DD/quotes.parquet`
- columnas requeridas:
  - `timestamp`
  - `bid_price`
  - `ask_price`
  - `bid_size`
  - `ask_size`
- filas > 0
- precios no negativos
- ratio de `bid > ask`
- tipos de dato esperados

Esto sirve para detectar:

- parquet roto
- parquet vacio
- path mal construido
- escritura corrupta
- degradacion obvia de bid/ask


## paso 10: que no detecta Agent 02 por si solo

Agent 02 no es una bala de plata.

Por si solo no garantiza:

- que el dia tenga todos los quotes esperados por negocio
- que no falte una parte del dia si el parquet parcial parece “sano”
- que un dataset viejo heredado del flujo antiguo este perfecto

Por eso la solucion real no podia quedarse solo en Agent 02/03.

Habia que corregir tambien el downloader de origen.


## paso 11: por que el downloader corregido era obligatorio

Si el downloader publica un parquet parcial pero tecnicamente legible:

- Agent 02 puede verlo como `PASS` o `SOFT_FAIL`
- Agent 03 puede contarlo en cobertura
- y la corrida puede dar una sensacion falsa de completitud

La correccion importante es esta:

- solo publicar a `D:\quotes` cuando el dia esta completo

Eso mueve la garantia al punto correcto:

- prevencion en origen
- validacion en tiempo real despues


## paso 12: criterio de cierre de corrida

La corrida no se cierra solo porque Agent 01 termine el CSV base.

La corrida se cierra cuando se cumplen todos estos puntos:

- Agent 01 no tiene lote base pendiente
- `retry_queue_quotes_strict_current.csv` esta vacia
- no hay `HARD_FAIL` activos en `events_current`
- no hay `RETRY_PENDING`
- la cobertura de Agent 03 no tiene huecos operativos sin explicar

En lenguaje operativo:

- sin pendientes
- sin hard fails
- sin retries vivos
- sin falsa cobertura


## paso 13: que hacer con low coverage

`LOW_COVERAGE` no significa automaticamente corrupcion.

Puede significar:

- la corrida sigue en curso
- Agent 02 aun no ha visto ese rango
- faltan retries por ejecutar
- o realmente falta data

Por eso `LOW_COVERAGE` se interpreta junto con:

- `retry_pending`
- `hard_fail`
- causa dominante por ticker
- y fase actual de la corrida

No hay que cerrar por cobertura baja mientras aun haya retry pendiente.


## paso 14: que no se puede hacer

No se puede:

- cambiar `RUN_ID` a mitad
- descargar en `D:\quotes` y validar en otra raiz
- dar por bueno un parquet solo porque existe
- meter parciales en la raiz vigilada
- cerrar corrida sin vaciar retry
- mezclar artefactos de distintas corridas en el mismo `RUN_DIR`


## paso 15: secuencia recomendada de terminales

**Terminal A**

Descarga base:

```bash
python "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py" ^
  --csv "C:\ruta\al\tasks_quotes.csv" ^
  --output "D:\quotes" ^
  --concurrent 80 ^
  --run-id "20260312_quotes_session_01" ^
  --run-dir "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260312_quotes_session_01" ^
  --resume
```

**Terminal B**

Validacion strict loop:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent02_quotes_strict_loop.ps1" -RunId "20260312_quotes_session_01" -QuotesRoot "D:\quotes" -MaxFiles 5000 -SleepSec 15
```

**Terminal C**

Monitor de cobertura/causas:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_live_fast.ps1" -RunId "20260312_quotes_session_01" -IntervalSec 5
```


## paso 16: secuencia recomendada de retry

Cuando exista `retry_queue_quotes_strict_current.csv`:

- convertir `file -> ticker,date`
- guardar `retry_round_01.csv`
- relanzar downloader solo contra esa cola

Ejemplo:

```bash
python "C:\TSIS_Data\v1\backtest_SmallCaps\scripts\download_quotes.py" ^
  --csv "C:\ruta\al\retry_round_01.csv" ^
  --output "D:\quotes" ^
  --concurrent 50 ^
  --run-id "20260312_quotes_session_01" ^
  --run-dir "C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260312_quotes_session_01"
```

Y despues:

- Agent 02 revalida
- Agent 03 actualiza cobertura
- si sigue habiendo cola, nueva ronda


## paso 17: definicion de exito real

Exito real no es:

- “el script acabo”
- “hay parquet en disco”
- “el monitor imprime cosas”

Exito real es:

- corrida trazable por `RUN_ID`
- downloader sin publicar parciales
- validacion strict sobre la misma raiz real
- monitor con cobertura coherente
- retry vacio o congelado de forma explicada
- cierre sin estados abiertos ambiguos


## resumen ejecutivo

El flujo que se va a ejecutar es este:

- Agent 01 descarga en `D:\quotes`
- publica solo dias completos
- Agent 02 valida lo que entra en `D:\quotes`
- Agent 03 monitoriza cobertura y causas
- lo que falle vuelve a retry
- y no se cierra nada hasta vaciar pendientes reales

Ese es el proceso correcto de principio a fin.

No falta ninguna pieza si se respetan estos puntos:

- misma raiz real: `D:\quotes`
- mismo `RUN_ID`
- mismo `RUN_DIR`
- descarga, validacion y monitorizacion en paralelo
- retry hasta vaciar
- cierre solo con estados limpios



Veredicto

  Para vuestro caso de uso, el Agente 01 por REST es la estrategia correcta, pero con una precisión importante:

  - Si el objetivo es descargar ticker/date selectivo mientras validáis en paralelo, v3/quotes/{ticker} es la vía
    adecuada.
  - Si el objetivo fuese descargar historia masiva completa de quotes SIP, entonces no: lo correcto serían los flat
    files de Massive/Polygon, no REST.

  Con las correcciones que acabo de dejar en download_quotes.py, el script ya está cerca de una base buena de producción
  para vuestro flujo 01-02-03. Antes no lo estaba del todo.

  Qué he verificado

  - El endpoint oficial correcto es v3/quotes/{ticker} con filtros timestamp.gte, timestamp.lt, limit, sort, order y pa
    ginación por next_url. Fuente oficial: https://massive.com/docs/rest/stocks/trades-quotes/quotes
  - Massive/Polygon documenta los flat files us_stocks_sip/quotes_v1 como la vía de descarga histórica masiva diaria. F
    uente oficial: https://massive.com/docs/flat-files/stocks/quotes/2025/04
  - La guía REST oficial separa claramente REST y flat files como mecanismos distintos de acceso a históricos. Fuente o
    ficial: https://massive.com/docs/rest/quickstart

  Lo que sí hace bien el script actual

  - Ventana de mercado DST-aware con America/New_York en download_quotes.py:240.
  - Paginación completa por next_url y apiKey reinyectada en download_quotes.py:302.
  - Escritura atómica en download_quotes.py:92.
  - No publica parciales en D:\quotes, que era el fallo más peligroso, en download_quotes.py:328.
  - resume ya reutiliza archivos buenos existentes en disco, no solo estado previo, en download_quotes.py:277.
  - Ya persiste estado/retry de forma incremental durante la corrida en download_quotes.py:443.
  - Ya procesa tareas en batches para no crear todas las coroutines de golpe en download_quotes.py:474.

  Lo que sigue sin estar “óptimo máximo”

  - El script ya escribe incrementalmente por páginas a parquet temporal y publica al final. Eso endurece RAM y
    publicación, pero todavía falta validarlo en descarga real contra Polygon en una prueba comparativa controlada.
  - Agent01 y Agent02 ya pasan/consumen `RUN_ID`, `RUN_DIR` y `D:\quotes` de forma alineada.
  - Agent03 también debe trabajar sobre el mismo `RUN_ID` y `D:\quotes`; si se deja en auto sobre “última corrida” puede
    mezclar sesiones si hay más de una activa.

  Conclusión práctica

  - Para vuestro flujo realtime con Agent02 y Agent03: sí, REST + este Agent01 corregido es la opción correcta.
  - Para “mejor posible” en sentido absoluto: todavía no. El siguiente salto es validarlo con descarga real frente a
    Polygon y comparar outputs contra un caso controlado.

  He verificado sintaxis del script con py_compile. No he probado descarga real contra Polygon desde este entorno.

  Siguiente paso recomendado: ejecutar una descarga real pequeña, con el mismo `RUN_ID`, y dejar Agent02/03 mirando esa
  misma corrida para confirmar que el contrato operativo se cumple extremo a extremo.
