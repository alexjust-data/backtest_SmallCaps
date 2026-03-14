## descarga de datos halts oficiales

Este documento resume de forma tecnica y operativa lo que se ha montado para descargar, normalizar y consolidar `halts` oficiales en `D:\Halts`.

## objetivo

El objetivo no es usar un vendor secundario para inferir halts.

El objetivo es construir un `master` de verdad-terreno oficial para contrastar:

- anomalias de `quotes`
- ventanas `pre-halt / halt / reopen`
- eventos `LULD`, `news pending`, `corporate action`, `regulatory halt`, `SEC suspension`
- y despues cruzarlo con Agent02 / Agent03 en el analisis causal de `bid > ask`

## principio de fuentes

Para `halts` se priorizan fuentes oficiales primarias:

- `Nasdaq Trader`
- `NYSE`
- `SEC`

Polygon / Massive se usa para:

- `quotes`
- `trades`
- `aggs`
- y contraste de microestructura

pero no como fuente primaria de verdad regulatoria del halt.

La razon es:

- el halt lo decide y publica el venue o el regulador
- necesitamos taxonomia oficial de evento
- necesitamos trazabilidad defendible
- y queremos evitar circularidad vendor -> vendor

## situacion inicial

Antes de esta fase ya existia un bloque oficial Nasdaq descargado en `D:\Halts`.

Artefactos relevantes heredados:

- `D:\Halts\raw\nasdaq_rss_by_date\*.xml`
- `D:\Halts\processed\halts_master_nasdaq_for_run_dates.csv`
- `D:\Halts\processed\halts_master_nasdaq_for_run_dates.parquet`
- `D:\Halts\processed\universe_vs_halts_coverage.csv`

Cobertura inicial del universo con solo Nasdaq:

- `5486 / 12132` tickers con datos de halt
- `45.22%` del universo
- `29287` eventos en el universo

## fuentes oficiales adicionales incorporadas

### NYSE

Fuente oficial usada:

- pagina: `https://www.nyse.com/trade/trading-halts`
- current CSV: `https://www.nyse.com/api/trade-halts/current/download`
- historical CSV publico: `https://www.nyse.com/api/trade-halts/historical/download`
- static data: `https://www.nyse.com/api/trade-halts/static-data`

Hallazgo importante:

- la propia web de NYSE usa el componente `trade-halts-historical`
- ese componente expone dos endpoints oficiales reutilizables:
  - `/api/trade-halts/historical/filter`
  - `/api/trade-halts/historical/download`
- la pagina publica indica que los datos historicos gratis disponibles son de `1 year`

Por tanto, la cobertura gratuita oficial de NYSE que se ha incorporado es:

- `current`
- `historical 1 year`

No se ha asumido historico profundo libre de NYSE mas alla de ese ano.

### SEC

Fuente oficial usada:

- pagina: `https://www.sec.gov/enforcement-litigation/trading-suspensions`
- legacy equivalente: `https://www.sec.gov/litigation/suspensions.htm`
- RSS: `https://www.sec.gov/enforcement-litigation/trading-suspensions/rss`

Hallazgo importante:

- la pagina oficial esta paginada
- se confirmo al menos paginacion publica hasta `?page=13`
- el HTML contiene filas oficiales con:
  - `publish date`
  - `respondents`
  - `release no`
  - links al PDF oficial
  - links a `Trading Suspension Release` o `Order`

Esto permite descarga historica oficial publica de suspensiones SEC.

## scripts creados

Se crearon tres scripts en:

- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\halts\download_nyse_halts_official.py`
- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\halts\download_sec_suspensions_official.py`
- `C:\TSIS_Data\v1\backtest_SmallCaps\scripts\halts\build_halts_master_multisource.py`

### 1. download_nyse_halts_official.py

Funcion:

- descarga `current` oficial NYSE
- descarga `historical` oficial NYSE del ultimo ano usando `haltDateFrom` / `haltDateTo`
- guarda raw CSV y static data
- normaliza a schema comun

Entrada principal:

- `--output-root D:\Halts`
- `--from-date`
- `--to-date`

Raw generados:

- `D:\Halts\raw\nyse\nyse_current_<date>.csv`
- `D:\Halts\raw\nyse\nyse_historical_<from>_to_<to>.csv`
- `D:\Halts\raw\nyse\nyse_static_data.json`

Procesados:

- `D:\Halts\processed\halts_master_nyse_1y.csv`
- `D:\Halts\processed\halts_master_nyse_1y.parquet`

Resultado actual:

- `29` rows current
- `13193` rows historical
- `13178` rows master tras deduplicacion segura

### 2. download_sec_suspensions_official.py

Funcion:

- recorre la paginacion oficial SEC
- guarda raw HTML por pagina
- parsea filas oficiales de la tabla de suspensiones
- recupera:
  - `publish date`
  - `respondent / issuer name`
  - `release no`
  - `item_link` del PDF principal
  - link `see also` cuando existe
- intenta inferir `ticker` por dos vias:
  - extraccion regex sobre texto del PDF oficial
  - reconciliacion exacta por nombre contra `D:\reference\overview`

Entrada principal:

- `--output-root D:\Halts`
- `--max-pages`
- `--overview-root D:\reference\overview`

Raw generados:

- `D:\Halts\raw\sec\sec_trading_suspensions_page_00.html`
- `...`
- `D:\Halts\raw\sec\sec_trading_suspensions_page_13.html`

Procesados:

- `D:\Halts\processed\halts_master_sec.csv`
- `D:\Halts\processed\halts_master_sec.parquet`

Resultado actual:

- `1346` rows SEC
- `250` rows con `ticker` inferido

Limitacion estructural:

- SEC publica muy bien el evento y el PDF
- pero el `ticker` no siempre aparece de forma trivial en la pagina
- por eso el bloque SEC se considera oficial y util, pero con `ticker recovery` parcial

### 3. build_halts_master_multisource.py

Funcion:

- carga el Nasdaq oficial existente
- carga el nuevo bloque NYSE
- carga el nuevo bloque SEC
- normaliza columnas a un schema comun
- repara la parte Nasdaq que tenia `ticker` vacio en el parquet viejo usando el raw existente:
  - `title` como fallback de `ticker`
  - `halt_date_text` como fallback de `halt_date`
- genera un `master` multisource consolidado
- exporta tambien resumen por fuente

Procesados generados:

- `D:\Halts\processed\halts_master_multisource.csv`
- `D:\Halts\processed\halts_master_multisource.parquet`
- `D:\Halts\processed\halts_master_multisource_summary.csv`

## schema comun del master multisource

Columnas comunes:

- `source`
- `source_priority`
- `ticker`
- `issuer_name`
- `listing_exchange`
- `halt_date`
- `halt_start_et`
- `resume_quote_et`
- `resume_trade_et`
- `halt_code`
- `halt_type`
- `raw_reason`
- `release_no`
- `item_link`
- `url_source`
- `is_sec_suspension`

Criterios:

- `source_priority = 1` para fuentes oficiales directas `NYSE` y `SEC`
- `source_priority = 2` para el bloque Nasdaq heredado ya descargado
- no se fuerza igualdad perfecta entre fuentes; se conserva el mayor detalle posible por source

## resultados actuales

Resumen del consolidado tras rehacer el bloque Nasdaq en el builder:

- `Nasdaq`: `119630` rows, `119619` con ticker no nulo
- `NYSE`: `13178` rows, `13178` con ticker no nulo
- `SEC`: `1346` rows, `250` con ticker no nulo
- `all`: `88683` rows en el master consolidado tras deduplicacion segura
- `87576` rows con ticker no nulo en el master consolidado

Nota tecnica:

- el numero `all` es menor que la suma simple por deduplicacion entre fuentes y por normalizacion de registros equivalentes
- no es una perdida accidental; es consolidacion de eventos repetidos entre sources o snapshots

## cobertura del universo tras multisource

Se genero tambien el cruce actualizado contra el universo actual:

- `D:\Halts\processed\universe_vs_halts_coverage_multisource.csv`
- `D:\Halts\processed\universe_tickers_with_halts_multisource.csv`
- `D:\Halts\processed\universe_tickers_without_halts_multisource.csv`

Resultado:

- universo unico: `12132` tickers
- con halt data multisource: `7955`
- sin halt data multisource: `4177`
- cobertura multisource: `65.57%`
- eventos totales en universo: `51474`

Mejora frente a solo Nasdaq:

- antes: `45.22%`
- ahora: `65.57%`

## uso previsto en analisis causal

El master multisource se usa para:

- enriquecer `Agent03`
- contrastar `same-day halt`
- contrastar `pre-halt / halt_window / reopen`
- separar:
  - `halt_microstructure_stress`
  - `halt_placeholder_pathology`
  - `nonhalt_low_rows_extreme`
  - `corporate_action_related`
  - `persistent_vendor_pathology`

Y se integra con:

- `quotes_agent_strict_events_current.csv`
- `045_agent3_causal_hypotheses.py`
- `046_agent3_halt_contrast.py`

## limites conocidos

### NYSE

- cobertura publica gratuita historica: `1 year`
- no se ha resuelto aun historico profundo libre de NYSE mas alla de ese horizonte

### SEC

- el `ticker` no siempre es recuperable automaticamente
- parte del valor de SEC esta en:
  - `issuer_name`
  - `release_no`
  - `PDF official link`
- el bloque SEC es valido como verdad-terreno de suspensiones, aunque la reconciliacion a ticker no sea total

### Nasdaq

- el raw heredado no venia limpio a nivel `ticker` / `halt_code`
- se ha dejado una reparacion pragmatica para coverage y master multisource
- si mas adelante se quiere precision completa intradia para Nasdaq, conviene rehacer ese parser sobre el raw RSS/CSV con mas detalle

## criterio tecnico final

Tras esta fase, el mayor hueco de referencia oficial para `halts` ha quedado fuertemente reducido.

Estado actual:

- `Nasdaq`: cubierto
- `NYSE current + 1y`: cubierto
- `SEC suspensions`: cubierto
- `master multisource`: construido
- `coverage against universe`: actualizado

Por tanto, el siguiente paso ya no es descargar mas `halts`, sino explotarlos:

- rerun de `046_agent3_halt_contrast.py` usando `halts_master_multisource.parquet`
- etiquetado de casos `same-day halt`
- separacion explicativa entre `stress de halt`, `placeholders`, `iliquidez extrema` y `pathology no-halt`
