
```bash
════════════════════════════════════════════════════════════════════════════════
                EJEMPLO PIPELINE DE CONSTRUCCIÓN DE UNIVERSO
                2005-2026 COMMON STOCKS NYSE AMEX/ARCA NASDAQ
                población target de Small Caps (market cap < $2B)
════════════════════════════════════════════════════════════════════════════════

Todo lo descargable por ticker (core público) desde Polygon.io

Stocks (acciones)

- Trades tick-by-tick (histórico + último trade).
- Quotes NBBO tick-by-tick (histórico + última quote).
- Barras OHLCV (custom range, open/close diario, previous day).
- Snapshots (single ticker, lista de tickers, full market).
- Referencia por ticker (ticker overview, tipo de ticker, etc.).
- Corporate actions por ticker (dividends, splits, IPOs cuando aplica).
- Fundamentals por ticker (Income Statement, Balance Sheet, Cash Flow, Ratios).
- News por ticker.
- Flat Files S3:
    - us_stocks_sip/trades_v1
    - us_stocks_sip/quotes_v1
    - us_stocks_sip/minute_aggs_v1
    - us_stocks_sip/day_aggs_v1   <- necesario para close_t

Descarga por S3 Flat Files (más robusto que API por paginación).
Endpoint y bucket docs: https://files.massive.com y flatfiles.
Fuente: https://massive.com/docs/flat-files/quickstart
```

```bash
════════════════════════════════
universo (activos + inactivos)
═══════════════════════════════
Objetivo: construir universo completo 2005-2026 (activos + inactivos) y
dejarlo guardado como snapshot point-in-time en formato Parquet particionado.

Regla operativa clave:  
- No usar snapshot único 2026 para inferir 2005-2026.
- Construir panel temporal con /v3/reference/tickers?date=... (ideal mensual, mínimo trimestral).
- Filtros por corte: market=stocks, type=CS, primary_exchange IN [XNAS, XNYS, XASE, ARCX], active=true.
- Clave de entidad: FIGI (composite_figi/share_class_figi). Ticker solo como etiqueta temporal.

# ejecuto comandos
python C:\TSIS_Data\v1\backtest_SmallCaps\scripts\build_universe_pti.py 
   --start 2005-01-01 --end 2025-12-31 
   --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti 
   --frequency daily 
   --active-filter all 
   --checkpoint-mode on --resume

# reultados
panel rows: 966,935
entities : 13,642
active   : 5,230
inactive : 8,412
cuts     : requested=7,670 successful=7,670 missing=0
outdir   : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti
all      : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet
active   : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_active.parquet
inactive : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_inactive.parquet
qa       : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\qa_coverage_by_cut.csv

snapshot 2026-03-09: rows=5,256 entities=5,256 | progress=7738/7738 (100.00%) elapsed=210s eta=0s
panel rows: 29,735,570
entities : 15,979
active   : 5,256
inactive : 10,723
cuts     : requested=7,738 successful=7,738 missing=0
outdir   : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti
all      : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet
active   : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_active.parquet
inactive : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_inactive.parquet
qa       : C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\qa_coverage_by_cut.csv

# verifico
python C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agent_universe_audit.py 
--outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti 
--stale-minutes 10 
--validate-latest-checkpoints 50 
--out-report-json C:\TSIS_Data\v1\backtest_SmallCaps\runs\data_quality\00_data_certification\universe_audit_final_after_cleanup.json

Verificado: quedó bien y cerrado.
   Resultado final:
      1. progress: completed, 7670/7670, snapshot_date=2025-12-31
      2. panel: 29,379,144 filas, 7670 snapshots diarios (2005-01-01 a 2025-12-31)
      3. meta y qa consistentes con panel
      4. Auditor final: overall: OK
   Reporte final guardado en:
      - universe_audit_final_after_cleanup.json
   También archivé el qa_coverage_by_cut.partial.csv residual para eliminar el último warning.
```

```bash
════════════════════════════════════════════════
Universo Filtrado listed - deslisted
════════════════════════════════════════════════
No descarga nada: toma el panel/snapshot ya generado y
filtra los tickers que estuvieron listados entre 2005 y 2026.

Regla temporal :
- incluir si intervalo [first_seen_date, last_seen_date] intersecta [2005-01-01, 2026-12-31].
- Es decir, hace barrido de ... e identifica la primera fecha donde aparece el tiker y la ultima

Es decir:
- recorre los snapshots del panel PTI para cada ticker/entidad,
   - C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_panel_pti
- identifica la primera fecha en la que aparece (`first_seen_date`),
- identifica la última fecha en la que aparece (`last_seen_date`).

# Verificacion : listed - deslisted : tail(1)
rows: 15979

ticker                                         ZZ
name                        SEALY CORPORATION COM
market                                     stocks
locale                                         us
primary_exchange                             XNYS
type                                           CS
active                                       True
currency_name                                 usd
cik                                    0000748015
composite_figi                                NaN
share_class_figi                              NaN
list_date                                    None
delisted_utc                                 None
last_updated_utc      2025-01-15T22:43:12.193858Z
snapshot_date                          2013-03-18
entity_id                                 ZZ|XNYS
exchange_priority                               1
has_composite_figi                              0
has_share_class_figi                            0
has_list_date                                   0
cut_frequency                               daily
#first_seen_date                        2006-04-07
#last_seen_date                         2013-03-18
latest_active                                True
status_confidence                            high
status                                   inactive
```


```bash
═════════════════════════════════════════════════════════════════════════════════════════
Universo Híbrido Enriquecido (Market Cap, Description, Employees, SIC Code, Delisted UTC, ...)
═════════════════════════════════════════════════════════════════════════════════════════
Enriquecer el universo filtrado con estrategia dual y prioridad temporal (as-of date).

Estrategia Dual de Enriquecimiento:

ACTIVOS (por corte):
- /v3/reference/tickers/{ticker}?date=...
- market_cap, weighted_shares_outstanding, description, employees, sic, etc.

INACTIVOS o faltantes:
- algunos inactivos pueden no resolver; usar backfill desde snapshots/eventos.
- delisted_utc, list_date, cik, figi.

Merge final:
- prioridad dato as-of-date > dato estático de snapshot final.

# launcher
python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_universe_hybrid_enriched.py 
   --input C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet 
   --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched 
   --batch-size 200 
   --resume

batch 1/80 rows=0-199 done=200/15979 (1.25%) eta=4733s
...
batch 80/80 rows=15800-15978 done=15979/15979 (100.00%) eta=0s
C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_universe_hybrid_enriched.py:288: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  asof = pd.concat(frames, ignore_index=True)
rows=15,979
saved=C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet
qa=C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.qa.csv
coverage:
                               field  non_null  rows  coverage_pct
                     final_round_lot     15978 15979         99.99
                   final_ticker_root     15978 15979         99.99
final_share_class_shares_outstanding     15245 15979         95.41
                           final_cik     14936 15979         93.47
                  final_phone_number     11643 15979         72.86
              final_address_address1     11641 15979         72.85
                  final_address_city     11641 15979         72.85
           final_address_postal_code     11572 15979         72.42
                 final_address_state     11556 15979         72.32
                      final_sic_code     11528 15979         72.14
                     final_list_date     10643 15979         66.61
   final_weighted_shares_outstanding     10495 15979         65.68
                    final_market_cap     10454 15979         65.42
                final_composite_figi      9128 15979         57.12
              final_share_class_figi      9121 15979         57.08
                   final_description      8896 15979         55.67
                  final_homepage_url      8489 15979         53.13
               final_total_employees      7598 15979         47.55
             final_branding_logo_url      5656 15979         35.40
             final_branding_icon_url      4173 15979         26.12
                 final_ticker_suffix       734 15979          4.59
                  final_delisted_utc         0 15979          0.00


# implementamos deslistados directamente desde polygon : final_delisted_utc 67.11%

1. Catálogo de inactivos directo desde Polygon:
   - /v3/reference/tickers con active=false, type=CS, exchanges XNAS/XNYS/XASE/ARCX.
   - Se guarda en: C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\inactive_catalog.parquet
2. Merge del catálogo en el híbrido por ticker + primary_exchange.
3. Prioridad de fuentes actualizada:
   - asof > inactive_catalog > events > fallback > missing
   - para final_delisted_utc, final_cik, final_composite_figi, final_share_class_figi, final_last_updated_utc.
4. Inferencia final para delisted:
   - si sigue nulo y status=inactive, usa last_seen_date como final_delisted_utc.
   - marca source_final_delisted_utc = inferred_last_seen.

Nuevos flags:
   - --disable-inactive-catalog
   - --inactive-catalog-refresh

   # ejecuto  (forzando refresco del catálogo):
   python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_universe_hybrid_enriched.py 
      --input C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet 
      --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched 
      --batch-size 200 
      --inactive-catalog-refresh 
      --resume

   C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_universe_hybrid_enriched.py:394: 
      FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, 
      this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, 
      exclude the relevant entries before the concat operation. asof = pd.concat(frames, ignore_index=True)
      rows=15,979

      saved=C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet
      qa=C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.qa.csv
      
      coverage:

                                 field  non_null  rows  coverage_pct
                        final_round_lot     15978 15979         99.99
                      final_ticker_root     15978 15979         99.99
                              final_cik     15385 15979         96.28
   final_share_class_shares_outstanding     15245 15979         95.41
                     final_phone_number     11643 15979         72.86
                 final_address_address1     11641 15979         72.85
                     final_address_city     11641 15979         72.85
              final_address_postal_code     11572 15979         72.42
                    final_address_state     11556 15979         72.32
                         final_sic_code     11528 15979         72.14
                     final_delisted_utc     10724 15979         67.11
                        final_list_date     10643 15979         66.61
      final_weighted_shares_outstanding     10495 15979         65.68
                       final_market_cap     10454 15979         65.42
                   final_composite_figi      9623 15979         60.22
                 final_share_class_figi      9616 15979         60.18
                      final_description      8896 15979         55.67
                     final_homepage_url      8489 15979         53.13
                  final_total_employees      7598 15979         47.55
                final_branding_logo_url      5656 15979         35.40
                final_branding_icon_url      4173 15979         26.12
                    final_ticker_suffix       734 15979          4.59


   python -c "
      import pandas as pd; 
      d=pd.read_parquet(r'C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet'); 
      print(d['source_final_delisted_utc'].value_counts(dropna=False).to_string()
      )"

      source_final_delisted_utc
      inactive_catalog      7366
      missing               5255
      inferred_last_seen    3358

      - source_final_delisted_utc = missing son 5,255 filas.
      - Esas missing están 100% en status=active:
         - active: 5,255
         - inactive: 0
      - Por exchange dentro de missing:
         - XNAS: 3,281
         - XNYS: 1,739
         - XASE: 235
```

```bash
═════════════════════════════════════════════════════════════════════════════════════════
Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bias
═════════════════════════════════════════════════════════════════════════════════════════
Definir small-cap en forma point-in-time (sin look-ahead bias).

Regla por fecha t:
- market_cap_t = close_t * shares_outstanding_t
- close_t viene de day_aggs_v1
- small_cap_t = market_cap_t < 2_000_000_000

Fallback obligatorio si falta shares_outstanding_t:
- usar último valor válido anterior (LOCF) con límite máximo de antigüedad (ejemplo: 180 días).
- si no hay valor dentro del límite: market_cap_t = null e is_small_cap_t = null.
- nunca imputar con datos futuros.

Notas críticas:
- No clasificar 2005-2026 con market_cap "actual".
- Mantener inactivos históricos que fueron small-cap en su ventana de vida.
- Guardar tabla final:
- population_target_pti(date, figi, ticker, is_small_cap, exchange, status).

# Problema real y solucion --> Necesitamos datos de los fundamentales.

Validé que:

- el script actual sí soporta --resume-validate y --progress-every;
- --help refleja exactamente esos flags;
- tu input tiene 12,473 tickers únicos, así que procesará aprox. 12,473 x 4 = 49,892 tareas dataset/ticker.

python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\download_fundamentals_v1.py 
   --input C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet 
   --outdir D:\financial --datasets income_statements,balance_sheets,cash_flow_statements,ratios 
   --batch-size 200 
   --workers 12 
   --limit 1000 
   --max-pages 50 
   --resume 
   --resume-validate 
   --progress-every 25

ticker 10/12473 (0.08%) last=ratios:AAGR rows=1 status=200 eta=2388s
ticker 20/12473 (0.16%) last=ratios:AAP rows=1 status=200 eta=1977s
...
...
ticker 12470/12473 (99.98%) last=ratios:ZYME rows=1 status=200 eta=0s
ticker 12473/12473 (100.00%) last=ratios:ZZ rows=0 status=200 eta=0s
tickers_processed=12,473
tickers_skipped_valid=0
outdir=D:\financial
progress=D:\financial\_run\download_fundamentals_v1.progress.json
errors=D:\financial\_run\download_fundamentals_v1.errors.csv

```

**Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bia**

1) Gate de datos (obligatorio antes de clasificar)

   1. Confirmar fuente de close_t (day_aggs_v1) disponible localmente.
   2. Confirmar fuente temporal de shares_outstanding_t (no está en tickers_panel_pti; hay que construirla).
   3. Si falta alguno, no calcular is_small_cap_t final.

   **close_t (day_aggs_v1)**  
     Es el precio de cierre diario del ticker en la fecha t.  
     Sirve para valorar la empresa en ese día: market_cap_t = close_t * shares_outstanding_t.  
     Sin close_t diario no puedes hacer clasificación small-cap PTI fiable.  
  **hares_outstanding_t** (serie temporal, no en tickers_panel_pti)  
     Es el número de acciones en circulación en la fecha t (no el actual).  
     Sirve para completar la fórmula de market cap diaria y evitar look-ahead.  
     Como no viene histórico en tickers_panel_pti, hay que construir una serie temporal (as-of + LOCF con límite) antes de clasificar  
     is_small_cap_t.

2) Spine PTI (ya lo tienes)

   1. Base diaria desde tickers_panel_pti (2005-2025/2026 parcial), con: date, ticker, entity_id, exchange, status.
   2. Clave de entidad para salida:
      - figi = coalesce(composite_figi, share_class_figi)
      - fallback técnico: entity_id si FIGI falta (con flag).

3)

   1. Leer day_aggs_v1 y extraer close por ticker,date.
   2. Join left contra spine diaria.

4) Serie de shares PTI (shares_outstanding_t)

   1. Prioridad 1: construir tabla histórica de shares por fecha (as-of).
   2. Prioridad 2: fundamentals históricos (si contienen shares útiles por periodo).
   3. Prioridad 3 (degradado): final_weighted_shares_outstanding estático de hybrid_enriched con source_shares='static_fallback'.
   4. Guardar source_shares y shares_observed_date.

5) LOCF estricto (anti-lookahead)

   1. Para cada entidad, forward-fill solo hacia adelante en el tiempo.
   2. Límite: 180 días desde último dato observado.
   3. Si excede 180 días: shares_outstanding_t = null.

6) Clasificación diaria

   1. market_cap_t = close_t * shares_outstanding_t si ambos no null.
   2. is_small_cap_t = market_cap_t < 2_000_000_000.
   3. Si falta alguno: market_cap_t = null, is_small_cap_t = null.

7) Salida final

   1. population_target_pti.parquet con:
      - date, figi, ticker, entity_id, primary_exchange, status
      - close_t, shares_outstanding_t, shares_age_days, source_shares
      - market_cap_t, is_small_cap_t
   2. Particionar por year=date.year y opcional month.

8) QA de cierre (bloqueante)

   1. %rows con close_t.
   2. %rows con shares_t y %rows con LOCF<=180.
   3. %rows clasificables.
   4. Distribución is_small_cap_t por año y exchange.
   5. Check anti-lookahead: 0 casos con shares_observed_date > date.

Punto crítico ahora: en tu entorno, ya validamos que tickers_panel_pti está bien, pero hay que localizar/normalizar la ruta real de
day_aggs_v1 y decidir fuente principal de shares_t.
Si quieres, en el siguiente paso te dejo el script build_population_target_pti.py ya con estos 8 bloques y checkpoints/resume.


```bash
═════════════════════════════════════════════════════════════════════════════════════════
Status: LISTO PARA FASE B si QA cobertura = OK (Ingesta quotes)
═════════════════════════════════════════════════════════════════════════════════════════
Checklist de salida antes de quotes_v1:
- panel point-in-time completo 2005-2026
- reconciliación de eventos (first_seen/last_seen/symbol changes)
- deduplicación por FIGI
- validación de cobertura por calendario
- población target small-cap point-in-time congelada

Nota temporal:
- Si ejecutas "hasta hoy", hoy es 2026-03-06. 2026 queda parcial y debe manejarse como incremental.
```