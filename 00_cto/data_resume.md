# Data Resume

## Ruta analizada
- `C:\TSIS_Data\data`

## Estructura principal
- `quotes_p95` (6114 tickers)
- `trades_ticks_2019_2025` (6311 tickers)
- `trades_ticks_2004_2018` (3478 tickers)
- `ohlcv_intraday_1m` (particiones `2004_2018` y `2019_2025`)
- `short_data` (`short_interest`, `short_volume`)
- `fundamentals` (`balance_sheets`, `cash_flow_statements`, `income_statements`, `smallcap_ratios`; `financial_ratios` vacio)
- `additional` (`corporate_actions`, `economic`, `ipos`, `news`)
- `reference` (`condition_codes`, `exchanges`, etc.)
- `regime_indicators` (`indices`, `etfs`)

## Cobertura temporal detectada
- `quotes_p95`: 2004-2025
- `quotes_p95_2004_2018`: 2004-2018
- `quotes_p95_2019_2025`: 2019-2025
- `trades_ticks_2004_2018`: 2004-2018
- `trades_ticks_2019_2025`: 2004-2025 (inconsistente con el nombre)
- `ohlcv_intraday_1m` (union de particiones): 2004-2025

## Esquemas clave (resumen)
- `quotes`: bid/ask price-size, exchanges, condiciones, `timestamp`, `participant_timestamp`.
- `trades_ticks`: `price`, `size`, `exchange`, `conditions`, `timestamp`, separacion `market`/`premarket`.
- `ohlcv_intraday_1m`: `open/high/low/close`, `volume`, `vwap`, `transactions`, `timestamp`, `minute`.
- `short_interest`: `settlement_date`, `short_interest`, `avg_daily_volume`, `days_to_cover`.
- `short_volume`: volumen short total y por venue + ratios/zscore.
- `corporate_actions`: splits (`execution_date`, `split_from`, `split_to`, `ticker`).
- `news`: titulo, descripcion, fecha UTC, url, publisher, tickers mencionados.
- `economic`: CPI/PCE y variantes.
- `ipos`: metadata de IPO y pricing.
- `smallcap_ratios`: liquidez, leverage, margenes, runways y flags de riesgo.

## Calidad de datos (auditoria rapida por muestra)
Muestra: 10 archivos de `quotes`, 10 de `trades`, 10 de `ohlcv`.
- Nulos en columnas criticas (`timestamp`, precios, volumen): 0 en la muestra.
- Orden temporal (`timestamp` no decreciente): 100% de archivos de la muestra.
- En muestra de 200 tickers de `trades_ticks_2019_2025`:
  - 200/200 tienen `market`.
  - 196/200 tienen tambien `premarket`.

## Consistencia / riesgos a considerar
- `trades_ticks_2019_2025` contiene datos pre-2019 en 200 tickers (ej. `AABA`, `AAWW`, `ALTR`), por lo que conviene filtrar por fecha real y no por nombre de carpeta.
- `fundamentals/financial_ratios` esta vacio.
- Las particiones usan convenciones distintas:
  - `quotes`: `day=DD`
  - `trades_ticks`: `day=YYYY-MM-DD`

## Universo usable para backtest (intersecciones)
- `quotes_p95` intersect `trades_ticks_2019_2025` intersect `ohlcv_intraday_1m`: 4777 tickers.
- + `short_interest`: 4772 tickers.
- + `smallcap_ratios`: 4156 tickers.
- `core + short + smallcap`: 4156 tickers.

## Conclusion operativa
La base es suficiente para backtest serio de smallcaps tipo pump-and-dump (microestructura, premarket/market, 1m bars, short metrics, news y eventos corporativos). El principal cuidado es normalizar filtros temporales y particiones antes de modelar.
