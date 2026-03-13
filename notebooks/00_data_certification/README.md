# 00_data_certification - Runtime Guide (Quotes)

## Scope

Flujo realtime para `quotes`:
- Agente 01: descarga (script final pendiente).
- Agente 02: validacion incremental strict.
- Agente 03: monitor/cobertura y diagnostico.

## Regla operativa

- Usar un unico `RUN_ID` durante toda la sesion.
- 02 y 03 deben usar el mismo `RUN_DIR`.
- No regenerar `RUN_ID` en cada vuelta.

## Scripts activos

- `032_probe_quotes_agent_behavior_strict.py` (validador quotes strict)
- `036_agent3_quotes_coverage_and_causes.py`
- `037_agent3_diagnostics_tables_hist.py`
- `043_dtype_rounding_plots.py`

## Terminal (.ps1)

Ubicacion:
- `C:\Users\AlexJ\Desktop\tsis_agents\run_agent02_quotes_strict_loop.ps1`
- `C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_monitor_loop.ps1`
- `C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_live_fast.ps1`

### 1) Agente 02 (loop incremental)

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent02_quotes_strict_loop.ps1" -RunId "20260305_quotes_session_01" -QuotesRoot "C:\TSIS_Data\data\quotes_p95" -MaxFiles 5000 -SleepSec 30
```

### 2) Agente 03 pesado (036+037, opcional 043/038)

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_monitor_loop.ps1" -RunId "20260305_quotes_session_01" -SleepSec 120 -TopN 10 -Run043
```

Opcional extra:
- `-Run043` anade plots de enterizacion/crossed.
- `-Run038` anade metricas globales de desviacion bid>ask.

### 3) Agente 03 rapido (instantaneo)

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\AlexJ\Desktop\tsis_agents\run_agent03_live_fast.ps1" -RunId "20260305_quotes_session_01" -IntervalSec 5
```

Este modo lee solo `live_status_quotes_strict.json` (sin recalcular notebooks).

## Artefactos por `RUN_DIR`

Base ejemplo:
`C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\20260305_quotes_session_01`

Genera 02:
- `quotes_agent_strict_events_history.csv`
- `quotes_agent_strict_events_current.csv`
- `quotes_agent_strict_state.json`
- `retry_queue_quotes_strict_current.csv`
- `batch_manifest_quotes_strict.csv`
- `run_config_quotes_strict.json`
- `live_status_quotes_strict.json`

Genera 03:
- `agent03_outputs\coverage_by_ticker.csv`
- `agent03_outputs\quality_summary_by_ticker.csv`
- `agent03_outputs\causes_by_ticker.csv`
- `agent03_outputs\ticker_diagnosis.csv`
- `agent03_outputs\run_summary.json`

## Umbrales strict vigentes

- `MAX_CROSSED_RATIO_PCT = 0.8`
- `HARD_FAIL_CROSSED_PCT = 5.0`
- `HARD_FAIL_ASK_INTEGER_PCT = 95.0`
- `HARD_FAIL_ASK_INT_CROSSED_PCT = 20.0`

Causas relevantes:
- `crossed_ratio_gt_threshold`
- `crossed_ratio_gt_hard_cap`
- `ask_integer_with_crossed_anomaly`




----

Sí, y ya vi cómo puede pasar. Hay varios fallos de diseño que explican exactamente “muchos folders vacíos o meses/años faltantes”.

  Causa principal (crítica): --resume marca ticker como completo con evidencia mínima

  1. En intraday wrapper, si existe cualquier minute.parquet, ese ticker se considera completado.

  - batch_intraday_wrapper.py:75
  - batch_intraday_wrapper.py:82:82

  2. En trades wrapper, igual: si existe cualquier premarket.parquet o market.parquet, se marca completado.

  - batch_trades_wrapper.py:64
  - batch_trades_wrapper.py:71:71

  Causa fuerte adicional: truncado de meses en intraday ultra-fast
  3. En intraday ultra-fast hay límite duro max_pages = 20 por mes.

  - ingest_intraday_ultra_fast.py:188
  - ingest_intraday_ultra_fast.py:191:191

  Si un mes necesita más páginas, queda incompleto. Luego resume del wrapper lo puede dar por “hecho” por tener un solo parquet.

  Causa de cobertura inconsistente
  4. Los ingestors cargan tickers desde parquet/csv sin filtrar has_data == true.
  Esto mete ruido y gasto en tickers sin datos, y aumenta probabilidad de ejecuciones parciales.

  Evidencia en tus datos (muestreo real)
  5. Comparando ping_range vs meses descargados (muestra de 1200 tickers):

  - Intraday 2019-2025: 59/1200 con déficit; 45 con déficit >= 3 meses.
  - Intraday 2004-2018: 113/1200 con déficit; 102 con déficit >= 3 meses.
  - Trades 2019-2025: 48/1200 con déficit; 43 con déficit >= 3 meses.
  - Trades 2004-2018: 105/1200 con déficit; 99 con déficit >= 3 meses.


  1. lógica de resume demasiado permisiva en wrappers,
  2. límites de paginación/errores que dejan descargas parciales,
  3. ausencia de criterio de “completo real” por ticker (meses/días esperados).

---

Hallazgos críticos (pueden reproducir el mismo fallo de “missing files/folders”)

  1. processed_files marca como “hecho” también los HARD_FAIL/SOFT_FAIL
     En el agente estricto, cualquier archivo procesado entra al estado global, aunque haya fallado validación. Luego ya no vuelve a
     pending normal.

  - 032_probe_quotes_agent_behavior_strict.py:377
  - 032_probe_quotes_agent_behavior_strict.py:394
    Riesgo: archivos malos quedan “cerrados” operativamente sin reparación real, igual que el problema de resume permisivo que te
    expliqué antes.

  2. No hay ciclo de reparación automática desde retry_queue a descarga
     retry_queue se genera, pero el loop de Agent02 vuelve a escanear disco, no ejecuta un downloader de retry acoplado.

  - run_agent02_quotes_strict_loop.ps1
  - 032_probe_quotes_agent_behavior_strict.py:525
    Riesgo: backlog eterno de retry y cobertura incompleta persistente.

  3. Bug en Agent03: se “resetea” manifest_files y se anula diagnóstico missing_in_events/missing_in_disk
     Primero carga manifest, pero más abajo vuelve a manifest_files = set(), dejando esas causas inactivas.

  - 036_agent3_quotes_coverage_and_causes.py:136
  - 036_agent3_quotes_coverage_and_causes.py:218
    Riesgo: te oculta justo los huecos operativos que querías detectar.

  4. Cálculo de cobertura temporal potencialmente sesgado (ventana esperada recortada por observado)
     exp_min=max(list_date, obs_min) y exp_max=min(delist_date, obs_max) puede reducir la ventana esperada al tramo ya observado.

  - 036_agent3_quotes_coverage_and_causes.py:117
    Riesgo: aparenta buena cobertura aunque falten meses históricos al inicio/fin.

  5. Cálculo de expected_days en días calendario, no trading days
     (exp_max-exp_min).days+1 sobreestima/mezcla fines de semana/feriados.

  - 036_agent3_quotes_coverage_and_causes.py:183
    Riesgo: falsos “LOW_COVERAGE” o métricas inconsistentes según ticker/época.

  6. Validadores dependen de partición day=DD rígida
     Regex fija day=\d{2}. Si cambias a day=YYYY-MM-DD (como otros pipelines históricos), rompe parsing y cobertura.

  - 032_probe_quotes_agent_behavior_strict.py:57
  - 036_agent3_quotes_coverage_and_causes.py:66
  - agent_05_validate_local_vs_ping.ipynb

  7. agent_04_refresh_ping_range_master no maneja robustamente 429/5xx/reintentos
     Hace ping secuencial simple; errores transitorios pueden degradar has_data/first_day/last_day.

  - agent_04_refresh_ping_range_master.ipynb
    Riesgo: fuente de verdad operativa incorrecta => cobertura mal evaluada.

  8. Universo base construido desde carpetas existentes de quotes_p95 (circularidad)
     El step de universo usa lo que ya existe en disco, no universo objetivo externo.

  - step00b_build_tickers_universe_from_quotes.py
    Riesgo: si faltó descargar tickers enteros, desaparecen del universo y el sistema “no ve” el missing.

  9. Desalineación funcional: nombre dice quotes+trades, runtime real valida solo quotes
     La propia notebook lo declara.

  - agent_02_validate_quotes_trades_realtime.ipynb
    Riesgo: creer que trades está certificado cuando no lo está en este loop.

  ———

  Evidencia operativa actual (run real)
  En 20260305_quotes_session_01:

  - files_discovered_total: 596,824
  - files_pending: 581,274
  - files_processed_total_state: 16,050
  - retry_pending_files_current: 12,211
    Fuente: live_status_quotes_strict.json

  Esto confirma que el diseño actual está en fase incremental de validación, pero con alto riesgo de cronificar faltantes si no se
  cierra el loop de reparación.





  ------



  • No, así como está no está bien respecto a lo que hallamos.
  Este notebook puede volver a producir missing importantes.

  Puntos críticos en agent_01_download_quotes_realtime.ipynb:

  1. Bug grave al unir ping_range
     Hace concat(df_a, df_b) y luego drop_duplicates(subset=["ticker"], keep="last").
     Si un ticker está en ambos ping (2004-2018 y 2019-2025), te quedas con una sola fila “última” y puedes perder ventana histórica
     (2005-2018).
     Esto explica directamente “años/meses no descargados”.
  2. Genera fechas con bdate_range entre first_day y last_day
     Eso no usa días reales de trading del ticker; mete días hábiles sin sesión del activo (feriados, ventanas sin cotización
     efectiva).
     Resultado: muchas tareas inútiles, retries y ruido de cobertura.
  3. No usa fuente de verdad por día (daily local) para construir tasks
     Con ping solo tienes min/max, no los días exactos. Para precisión operativa deberías generar tasks desde daily.parquet (como ya
     se hacía en generate_quotes_dates.py del proyecto anterior).
  4. QUOTES_ROOT default distinto al estándar de tu loop actual
     Default D:\quotes, mientras tu validación/monitoreo principal está en C:\TSIS_Data\data\quotes_p95.
     Si no lo alineas manualmente, Agent02/03 puede validar otra raíz distinta.
  5. No cierra el loop de retry
     Lanza downloader una vez, pero no integra automáticamente la cola de retry generada por Agent02.

  Conclusión:
  El diseño de este notebook sí puede derivar en el mismo fallo de cobertura incompleta.
  Si quieres, te propongo la versión corregida de lógica de tareas (merge correcto de ping + días reales desde daily) para dejarlo
  robusto.






  Sí. Este script puede generar missing y también pudo haber generado los errores que viste.

  Fallos críticos

  1. Truncado silencioso por límite de páginas (max_pages=10)
     Si un día necesita más páginas, corta la descarga y guarda parcial sin marcar error duro.
     download_quotes_ultra_fast.py:147
     download_quotes_ultra_fast.py:177
  2. completed sube aunque no haya archivo válido
     En el batch se incrementa completed para cualquier tuple, incluyendo respuestas None por 429/error/timeout. Eso contamina
     checkpoint y resume.
     download_quotes_ultra_fast.py:260
     download_quotes_ultra_fast.py:262
  3. resume por posición, no por identidad de archivo
     Hace tasks = tasks[skip:] según contador, no según archivos realmente escritos. Si hubo fallos, puedes saltarte tareas pendientes
     al reanudar.
     download_quotes_ultra_fast.py:284
     download_quotes_ultra_fast.py:285
  4. 429 inicial no cuenta como error y se pierde tarea en silencio
     Devuelve (ticker,date,None) tras sleep y sigue, sin reintento real ni archivo.
     download_quotes_ultra_fast.py:153
     download_quotes_ultra_fast.py:155
  5. Borra checkpoint al final aunque haya errores
     Pierdes trazabilidad/reanudación fina de pendientes reales.
     download_quotes_ultra_fast.py:381
     download_quotes_ultra_fast.py:383

  Riesgos adicionales
  6. Paginación interrumpida en 429 dentro del loop: corta el día y guarda parcial.
  download_quotes_ultra_fast.py:183
  download_quotes_ultra_fast.py:185

  7. Particionado rígido day=DD: si otro pipeline usa day=YYYY-MM-DD, se rompe interoperabilidad/verificación cruzada.
     download_quotes_ultra_fast.py:91

  Conclusión:
  Sí, este script es candidato directo a provocar faltantes y parciales “silenciosos”. Si quieres, te propongo una versión segura con
  resume por file key, paginación completa sin límite fijo, y estado separado done/failed/partial.



  Sí. Revisado a fondo: este script puede causar faltantes de archivos/cobertura.

  Problemas críticos que detecté:

  1. Resume incorrecto por índice de fila (checkpoint.completed)

  - Reanuda haciendo tasks = tasks[skip:].
  - Si cambió el CSV (orden, filas nuevas, deduplicaciones), puede saltarse tareas o repetir mal.

  2. Marca como “skip” solo porque existe quotes.parquet

  - should_skip() no valida integridad (0 bytes, corrupto, parcial).
  - Si hay archivo malo/parcial, no se reintenta.

  3. max_pages = 10 por día (hard cap)

  - Si un día necesita más páginas, se corta y guarda parcial como si fuera OK.
  - Esto explica cobertura baja en tickers líquidos.

  4. Manejo de 429 débil

  - Si recibe 429, devuelve None y ese día queda como error puntual sin estrategia robusta de reintento por tarea.

  5. Errores silenciosos en paginación

  - En cualquier error de página hace break y guarda lo que haya.
  - Resultado: parquet incompleto pero “exitoso”.

  6. Escribe parquet vacío para días sin datos y además cachea vacío

  - Si el “sin datos” fue por error temporal/API, se puede congelar como vacío y saltarlo después.

  7. Borra checkpoint al final siempre

  - close() elimina .checkpoint.json sin validar completitud real del lote.
  - Pierdes estado de recuperación consistente.

  8. No existe artefacto de “completitud por tarea” fuerte

  - No genera manifest con ticker,date,status,pages,rows,checksum.
  - Sin eso, difícil auditar qué quedó parcial.

  Conclusión:

  - Sí, este downloader puede producir exactamente el problema de “files/folders faltantes o incompletos”.

  Si quieres, te lo parcheo en modo robusto con:

  1. max_pages configurable o sin cap duro,
  2. reintentos por tarea con estado explícito,
  3. validación de parquet antes de skip,
  4. manifest por tarea (PASS/PARTIAL/FAIL),
  5. resume por clave ticker+date (no por índice).
  - Resume por clave (ticker,date) en events_current, no por índice de CSV.
  - No cachear “empty day” en primer fallo temporal.

  3. Acoplar 01↔02 con cola viva
  - 01 consume esa cola y redescarga automáticamente por rondas.
  - 02 revalida después de cada ronda.
  - Ciclo: 01 download -> 02 validate -> 01 retry -> 02 revalidate.

  4. Agente 02: separar “intentado” de “cerrado”

  - No meter HARD_FAIL/SOFT_FAIL en processed_ok.
  - Mantener: attempted_files, validated_ok_files, retry_pending.
  - Solo PASS (y si defines, SOFT_FAIL aceptable) cierra tarea.

  5. Agente 03: siempre sobre snapshot current

  - Lee events_current + retry_queue_current + run_config.
  - No histórico crudo para gate operativo.
  - Gate único:
  - NO_CLOSE_RETRY_PENDING
  - NO_CLOSE_HARD_FAIL_PRESENT
  - CLOSED_OK

  6. Cobertura correcta

  - Expected window: ping_master first_day..last_day (no recortar por observado).
  - Expected days: trading days reales (calendario de mercado), no calendario plano.

  7. Particionado/patrones flexibles

  - Parser soporta day=DD y day=YYYY-MM-DD para no romper pipelines mixtos.

  8. Artefactos obligatorios por corrida (RUN_ID)

  - run_config.json
  - download_events_history/current (01)
  - quotes_agent_events_history/current (02)
  - retry_queue_current, retry_attempts, retry_frozen
  - live_status.json (instantáneo)
  - agent03_outputs/* (analítico)

  9. Terminales en tiempo real (tu flujo 3 agentes)

  - T1: Agent01 loop (MaxFiles por ciclo, consume retry si existe).
  - T2: Agent02 loop (valida incremental).
  - T3: Agent03 live compact (lee solo snapshot, respuesta inmediata).

  10. Regla de oro

  - Un archivo no está “hecho” hasta pasar 02 y salir de retry.
  - “Existe en disco” nunca vuelve a ser criterio de cierre.


› Sí, si corriges eso bien, debería funcionar mucho mejor con tu flujo 01-02-03.

    Tu propuesta va en la dirección correcta. Yo solo reforzaría 4 puntos para que no reaparezca el problema:

    1. Agent01 debe escribir atómicamente
       Descarga a quotes.parquet.tmp y renombra a quotes.parquet solo al final. Evita que 02 valide archivos a medio escribir.
    2. Criterio de “partial” explícito
       Si cortas por max_pages (o error en páginas intermedias), marca DOWNLOAD_PARTIAL y no cierres tarea. Eso alimenta retry
       automático.
    3. Agent02 no debe cerrar SOFT_FAIL por defecto
       Si vas a usar SOFT_FAIL como cerrable, hazlo con lista blanca de causas. Si no, que entre a retry igual.
    4. Cobertura con calendario de mercado real
       Tu punto 6 es clave: sin calendario real vas a seguir viendo falsos gaps.

    Si implementas lo que has escrito + esos 4 refuerzos, sí: eliminas casi todas las causas estructurales de missing/folders
  vacíos que
    vimos. No hagas nada aún.


• Perfecto, totalmente alineado.

  Tus 4 refuerzos son correctos y los tomo como requisitos duros para la implementación:

  1. escritura atómica en 01 (.tmp -> rename),
  2. DOWNLOAD_PARTIAL explícito y reintento obligatorio,
  3. SOFT_FAIL no cerrable por defecto (solo whitelist),
  4. cobertura con calendario real de mercado.

  No hago cambios ahora. Cuando me digas, lo implementamos por fases.