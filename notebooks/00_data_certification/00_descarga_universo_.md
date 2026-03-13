```BASH
  ## 1) Universo base (seed)

  Endpoint principal: GET /v3/reference/tickers
  Filtras por market=stocks, type=CS y exchange (normalmente exchange; si necesitas IN, haces 4 llamadas o filtras luego por
  primary_exchange en respuesta).

  Qué te aporta:

  - Identidad y clasificación básica del activo
  - Estado activo/inactivo
  - Campo de exchange/listing para tu universo

  Fuente: All Tickers (https://massive.com/docs/rest/stocks/tickers/all-tickers?auth=login)

  ## 2) Enriquecimiento “master data” por ticker (empresa/identidad)

  Endpoint: GET /v3/reference/tickers/{ticker} (Ticker Overview)

  Campos enriquecidos confirmados por docs/samples:

  - market_cap
  - cik
  - composite_figi
  - share_class_figi
  - description
  - primary_exchange
  - list_date
  - currency_name
  - branding assets (logo, icon)
    Y en sample público también aparecen datos de contacto/dirección.

  Fuentes:

  - Ticker Overview (docs) (https://massive.com/docs/rest/forex/tickers/ticker-overview?auth=signup) (mismo endpoint /v3/reference/tick
    ers/{ticker})
  - Sample Ticker Details (AAPL)

  (https://massive.com/stocks?gad_campaignid=22427171503&gad_source=1&hsa_ad=744876560076&hsa_cam=1330311037&hsa_grp=177610769693&hsa_kw=stock+data&hsa_mt=p&hsa_net=adwords&hsa_src=g&hsa_tgt=kwd-298368892789&hsa_ver=3)

  ## 3) Corporate actions (historia corporativa)

  Endpoints:

  - Dividends
  - Splits
  - Ticker Events (cambios de símbolo/rebranding, experimental)

  Qué añade:

  - Ajustes históricos para backtesting y continuidad de series
  - Trazabilidad de cambios de ticker (muy útil para entidades que cambian símbolo)

  Fuentes:

  - Dividends (https://massive.com/docs/rest/stocks/corporate-actions/dividends)
  - Splits (https://massive.com/docs/rest/stocks/corporate-actions/splits?auth=login)
  - Ticker Events (https://massive.com/docs/rest/stocks/corporate-actions/ticker-events?auth=login)

  ## 4) Fundamentals modernos (enriquecimiento financiero fuerte)

  Endpoints nuevos (2025):

  - Income Statements
  - Cash Flow Statements
  - Balance Sheets
  - Ratios
  - Float

  Qué añade:

  - Estados financieros normalizados (quarterly/annual/TTM)
  - Ratios de valoración, rentabilidad, liquidez, leverage (ej. P/E, EV/EBITDA, ROE, etc.)
  - Datos de float/share structure para analítica de liquidez y ownership pressure

  Fuentes:

  - Income Statements (https://massive.com/docs/rest/stocks/fundamentals/income-statements)
  - Cash Flow Statements
    (https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements?assetClass=stocks&license=commercial&name=stocks_business)
  - Ratios (https://massive.com/docs/rest/stocks/fundamentals/ratios?assetClass=stocks&license=personal&name=stocks_developer)
  - Float (https://massive.com/docs/rest/stocks/fundamentals/float)
  - Changelog (nuevos Financials & Ratios, deprecación endpoint viejo) (https://www.massive.com/changelog)
  - Announcement Financials
    (https://massive.com/blog/announcing-polygon-io-financials-balance-sheets-cash-flow-income-statements-and-ratios)

  ## 5) Enriquecimiento de mercado (dinámico por ticker)

  Endpoints:

  - Snapshots (single/full market/top movers)
  - Trades
  - Aggregates (OHLC intradía/día)

  Qué añade:

  ## 6) Enriquecimiento informacional (news/event-driven)

  Endpoint:

  - Stocks News

  Qué añade:

  - Capa de catalizadores y event-driven features por ticker
  - Features NLP/sentiment downstream

  Fuente: News (https://massive.com/docs/rest/stocks/news?assetClass=stocks&license=personal&name=stocks_basic)

  ## 7) Sobre tus campos ejemplo

  Lo que pediste (market_cap, weighted_shares_outstanding, description, employees, sic_code, cik, composite_figi, share_class_figi,
  list_date, delisted_utc) cae principalmente en:

  - Ticker Overview / Ticker Details para identidad/fundamentales estáticos
  - All Tickers para discovery + estado
  - Float/Ratios/Financials para capa cuantitativa adicional
    delisted_utc suele aplicar cuando el ticker está inactivo/histórico.
```

```bash
DESLISTED

• No. En deslistadas no está “todo completo”; hay cobertura buena en unas capas y huecos en otras.

  - Reference/Tickers sí mantiene deslistadas (active=false) y sirve para evitar survivorship bias.
    Fuente: https://polygon.io/docs/rest/stocks/tickers/all-tickers ,
    https://intercom.help/polygonio/en/articles/6590095-what-does-polygon-do-with-delisted-tickers
  - En Ticker Overview, muchos campos son opcionales (cik, address, branding, etc.), así que en deslistadas pueden venir null o faltar
    (market_cap, employees, sic_code, weighted_shares_outstanding, etc. no siempre están completos).
    Fuente: https://polygon.io/docs/rest/stocks/tickers/ticker-overview
  - date en Ticker Overview da vista point-in-time; útil para recuperar estado histórico cercano al periodo de vida del ticker.
    Fuente: https://polygon.io/docs/rest/stocks/tickers/ticker-overview
  - Ratios/fundamentals modernos no cubren “todo el universo”; Massive indica cobertura aproximada (p.ej. ratios en ~4,400 activos,
    financials ~6,700 compañías), por lo que deslistadas pueden quedar fuera o incompletas.
    Fuente: https://www.massive.com/changelog
  - Market data histórico (trades/quotes/aggs) suele existir hasta la fecha de baja; después del delist no esperes datos nuevos de
    mercado para ese símbolo.
  - Para continuidad cuando hubo cambios de símbolo/reuso, hay que enlazar por composite_figi + ticker events.
    Fuente: https://massive.com/docs/stocks/get_vx_reference_tickers__id__events
```


```bash
En deslistadas (active=false) no tendrás completitud 100% en todos los campos.
  Sí tendrás buena trazabilidad de identidad/histórico, pero en enriquecidos (fundamentales, algunos company profile, ratios) habrá null
  o ausencia según cobertura/época/plan.

  ## Diccionario oficial (Massive/Polygon) para tu universo

  Filtros base del universo:

  - GET /v3/reference/tickers con market=stocks, type=CS, exchange in {XNAS,XNYS,XASE,ARCX} y active=true/false según necesites.
  - Validación de type y exchanges por catálogos: /v3/reference/tickers/types, /v3/reference/exchanges.

  ### A) Identidad y estado (alta cobertura)

  | Campo | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | ticker | /v3/reference/tickers, /v3/reference/tickers/{ticker} | Sí | Alta | n/a |
  | name | mismos | Sí | Alta | n/a |
  | market | mismos | Sí | Alta | n/a |
  | type | mismos | Sí | Alta | validar catálogo types |
  | primary_exchange | mismos | Sí | Alta | mapear con /v3/reference/exchanges |
  | locale | mismos | Sí | Alta | n/a |
  | active | mismos | Sí | Alta | n/a |
  | delisted_utc | mismos | Sí (clave) | Media-Alta | usar active=false + fecha |
  | last_updated_utc | /v3/reference/tickers | Sí | Media | n/a |

  ### B) Identificadores y clasificación (buena cobertura, no perfecta)

  | Campo | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | cik | ambos de referencia | Sí (parcial) | Media | buscar por cik en financials |
  | composite_figi | ambos | Sí (parcial) | Media-Alta | usar como clave entidad |
  | share_class_figi | ambos | Sí (parcial) | Media | composite_figi |
  | sic_code | /v3/reference/tickers/{ticker} | Sí (parcial) | Media | sic_description + description |
  | sic_description | overview | Sí (parcial) | Media | n/a |
  | ticker_root | overview | Sí (parcial) | Media | parseo ticker |
  | ticker_suffix | overview | Sí (parcial) | Media | parseo ticker |

  ### C) Company profile / enriquecido descriptivo (cobertura variable)

  | Campo | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | description | overview | Sí (parcial) | Media | news + filings |
  | homepage_url | overview | Sí (parcial) | Media | n/a |
  | phone_number | overview | Sí (parcial) | Baja-Media | n/a |
  | address.* | overview | Sí (parcial) | Baja-Media | n/a |
  | branding.logo_url/icon_url | overview | Sí (parcial) | Media | n/a |
  | total_employees | overview | Sí (parcial) | Baja-Media | financial filings |
  | list_date | overview | Sí (parcial) | Media-Alta | IPO endpoint / eventos |

  ### D) Shares / tamaño (muy útil, pero no siempre completo en deslistadas)

  | Campo | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | market_cap | overview | Sí (parcial) | Media | recalcular con precio*shares |
  | weighted_shares_outstanding | overview | Sí (parcial) | Media | financials share counts |
  | share_class_shares_outstanding | overview | Sí (parcial) | Media | financials |
  | round_lot | overview | Sí (parcial) | Alta | n/a |

  ### E) Eventos corporativos (histórico robusto)

  | Campo/Dataset | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | Dividendos (cash_amount, ex_dividend_date, pay_date, etc.) | /v3/reference/dividends | Sí (histórico) | Alta en emisores con
  dividendos | n/a |
  | Splits (execution_date, split_from, split_to) | /v3/reference/splits | Sí (histórico) | Alta | n/a |
  | Ticker changes/eventos | /vX/reference/tickers/{id}/events | Sí (timeline entidad) | Media (experimental) | ligar por composite_figi
  |

  ### F) Fundamentales nuevos (muy enriquecido, cobertura parcial por universo)

  | Dataset | Endpoint oficial | Aplica deslistadas | Completitud esperada | Fallback recomendado |
  |---|---|---|---|---|
  | Balance sheets | /stocks/financials/v1/balance-sheets | Parcial | Media | endpoint deprecated financials |
  | Cash flow | /stocks/financials/v1/cash-flow-statements | Parcial | Media | idem |
  | Income statements | /stocks/financials/v1/income-statements | Parcial | Media | idem |
  | Ratios (valuation/profitability/liquidity/leverage) | /stocks/financials/v1/ratios | Parcial | Media | calcular ratios propios |

  ## Regla operativa para deslistadas (recomendada)

  1. Universo con /v3/reference/tickers usando active=false + filtros de market/type/exchange.
  2. Enriquecer identidad con /v3/reference/tickers/{ticker}?date=... (point-in-time).
  3. Resolver cambios de símbolo con ticker events + composite_figi.
  4. Completar fundamentals con endpoints nuevos; aceptar null y registrar cobertura por campo.

  ## Fuentes oficiales

  - All Tickers: https://polygon.io/docs/rest/stocks/tickers/all-tickers
  - Ticker Overview: https://polygon.io/docs/rest/stocks/tickers/ticker-overview
  - Ticker Types: https://polygon.io/docs/rest/stocks/tickers/ticker-types
  - Exchanges: https://polygon.io/docs/rest/stocks/market-operations/exchanges
  - Dividends: https://polygon.io/docs/rest/stocks/corporate-actions/dividends
  - Splits: https://polygon.io/docs/rest/stocks/corporate-actions/splits
  - Ticker Events: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events
  - Balance Sheets: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets
  - Ratios: https://polygon.io/docs/rest/stocks/fundamentals/ratios
  - Delisted handling (KB): https://polygon.io/knowledge-base/article/what-does-polygon-do-with-delisted-tickers
  - Ticker changes handling (KB): https://polygon.io/knowledge-base/article/how-does-polygon-handle-ticker-changes-andacquisitions
  - Market cap methodology:
    https://intercom.help/polygonio/en/articles/6589968-what-is-market-cap-and-how-does-polygon-calculate-that-value
```



```bash
Confirmado en docs oficiales (Massive/Polygon):

  1. GET /v3/reference/tickers (All Tickers) es endpoint PTI base para universo por fecha (date) y filtros (market, type, active, etc.).
  2. GET /v3/reference/tickers/{ticker} (Ticker Overview) soporta date y expone campos de enriquecimiento (market_cap,
     weighted_shares_outstanding, sic_code, total_employees, list_date, identificadores, etc.).
  3. Delisted no se elimina del histórico (KB oficial: sin survivorship bias).
  4. GET /vX/reference/tickers/{id}/events existe (experimental) para continuidad de identidad/cambios ticker.
  5. Catálogos oficiales para validar filtros: ticker types y exchanges.

  Importante (inferencia operativa desde tus datos):

  - Aunque Polygon soporta delisted en referencia, en tu panel actual delisted_utc vino 0 veces. No significa que “Polygon no lo tenga”;
    significa que en tu corrida/cortes y payloads no llegó poblado.

  ## Cómo plantearía ahora el paso Universo Híbrido Enriquecido

  1. Base: tickers_2005_2026.parquet (ya filtrado temporalmente).
  2. Enriquecimiento as-of por fila (ticker, snapshot_date) con /v3/reference/tickers/{ticker}?date=....
  3. Resolver identidad con prioridad composite_figi > share_class_figi > ticker|exchange.
  4. Delisted pipeline en 3 niveles:
      - delisted_utc_direct: valor directo de endpoints de referencia.
      - delisted_utc_events: derivado de ticker events (cuando aplique).
      - delisted_utc_inferred: inferido por lifecycle (last_seen_date < max_snapshot) con bandera de baja confianza.
  5. Merge final por prioridad de fuente: as_of > events > inferred.
  6. Guardar trazabilidad por campo: source_delisted_utc, source_market_cap, etc.
  7. QA de cierre:
      - cobertura por campo global y por status/exchange,
      - conflictos de identidad (mismo FIGI con tickers múltiples por fecha),
      - consistencia temporal (first_seen <= snapshot <= last_seen),
      - tabla de confianza (high/medium/low) por origen.

  Si quieres, el siguiente paso lo implemento en un script único con --resume + checkpoints para que no tengas que rehacer llamadas.

  Fuentes:

  - All Tickers: https://polygon.io/docs/rest/stocks/tickers/all-tickers
  - Ticker Overview: https://polygon.io/docs/rest/stocks/tickers/ticker-overview
  - Ticker Events: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events
  - Ticker Types: https://polygon.io/docs/rest/stocks/tickers/ticker-types
  - Exchanges: https://polygon.io/docs/rest/stocks/market-operations/exchanges
  - Delisted KB: https://polygon.io/knowledge-base/article/what-does-polygon-do-with-delisted-tickers
  - Ticker changes KB: https://polygon.io/knowledge-base/article/how-does-polygon-handle-ticker-changes-andacquisitions
```



```bash
JerBouma FinanceDatabase usage Python Equities select market us
    Mejor para catálogo de símbolos/metadata; el propio proyecto indica que conserva deslistadas para research (curación comunitaria).
    https://github.com/JerBouma/FinanceDatabase
  - sec-cik-mapper
    Open source para mapear ticker <-> CIK <-> exchange desde SEC (actualización diaria en su repo).
    https://github.com/jadchaar/sec-cik-mapper
  - SEC directo (company_tickers_exchange.json)
    Fuente oficial gratis para mapping, pero SEC aclara que “no garantiza exactitud ni alcance”.
    https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
    https://www.sec.gov/file/company-tickers-exchange
  - pandas-datareader (get_nasdaq_symbols)
    Sirve para símbolos Nasdaq actuales; docs dicen que Nasdaq actualiza diario y no hay histórico en ese archivo.
    https://pandas-datareader.readthedocs.io/en/latest/readers/nasdaq-trader.html
    https://pandas-datareader.readthedocs.io/en/latest/remote_data.html

  Si quieres, te paso un script Python único que combine:

  1. universo XNAS/XNYS/XASE/ARCX,
  2. flag listed/delisted,
  3. export a CSV/parquet listo para tu pipeline.
```



## Qué añadir (máximo valor)

  1. Referencia as-of por ticker+fecha
     Endpoint: /v3/reference/tickers/{ticker}?date=...
     Campos: 

     ctive, 
     market_cap, 
     weighted_shares_outstanding, 
     share_class_shares_outstanding, 
     description, 
     total_employees, 
     sic_code,
     sic_description, 
     list_date, 
     delisted_utc, 
     cik, 
     composite_figi, 
     share_class_figi, 
     primary_exchange, 
     type, 
     last_updated_utc.

  2. Eventos de identidad/cambios
     Endpoint: /vX/reference/tickers/{id}/events (experimental)
     Uso: continuidad de símbolo (ticker changes), resolver entidad histórica por FIGI/CUSIP/ticker.
  3. Corporate actions históricas

  - /v3/reference/dividends
  - /v3/reference/splits
    Uso: enriquecer histórico y validar consistencia de lifecycle.

  4. Fundamentals nuevos (cuando aplique plan/cobertura)

  - /stocks/financials/v1/balance-sheets
  - /stocks/financials/v1/cash-flow-statements
  - /stocks/financials/v1/income-statements
  - /stocks/financials/v1/ratios
    Uso: backfill de shares/métricas cuando overview no complete.

  5. Catálogos de control (QA)

  1. Base: tickers_2005_2026.parquet (ya filtrado temporal).
  2. As-of enrichment por cada fila (ticker,snapshot_date).
  3. Merge por prioridad de fuente: as_of > events > fallback_local > missing.
  4. Campos finales + source por campo (source_final_*) para trazabilidad.
  5. Delisted en 3 niveles:
      - delisted_utc_direct (overview),
      - delisted_utc_events (events),
      - delisted_utc_inferred (si no aparece más en panel; menor confianza).
  6. Salida: universe_hybrid_enriched.parquet + QA coverage por status/exchange.

  ———

  ## Qué esperar en cobertura

  - Activas: alta en description, market_cap, weighted_shares_outstanding, sic.
  - Inactivas/deslistadas: mejor en identidad/histórico; menor en enriquecidos (nulls esperables).
  - delisted_utc no será 100% en overview; por eso events + inferencia controlada.

  ———

  ## Fuentes oficiales usadas

  - All Tickers: https://polygon.io/docs/rest/stocks/tickers/all-tickers
  - Exchanges: https://polygon.io/docs/rest/stocks/market-operations/exchanges
  - Ticker Events: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events
  - Dividends: https://polygon.io/docs/rest/stocks/corporate-actions/dividends
  - Splits: https://polygon.io/docs/rest/stocks/corporate-actions/splits
  - Balance Sheets: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets
  - Ratios: https://polygon.io/docs/rest/stocks/fundamentals/ratios
  - Delisted KB: https://polygon.io/knowledge-base/article/what-does-polygon-do-with-delisted-tickers

  Si quieres, en el siguiente paso te actualizo tu script actual para incluir ticker events + dividends/splits y dejar
  source_delisted_utc con jerarquía completa.


› o sea propones añadir esto ahora?   2. Eventos de identidad/cambios
       Endpoint: /vX/reference/tickers/{id}/events (experimental)
       Uso: continuidad de símbolo (ticker changes), resolver entidad histórica por FIGI/CUSIP/ticker.
    3. Corporate actions históricas

    - /v3/reference/dividends
    - /v3/reference/splits
      Uso: enriquecer histórico y validar consistencia de lifecycle.


• Sí, pero en este orden:

  1. Ahora mismo: añadir ticker events

  - Impacto directo en identidad/lifecycle y resolución de deslistadas/cambios de símbolo.
  - Es lo más crítico para evitar errores de entidad.

  2. Después: añadir dividends/splits

  - Muy útil para QA histórico y enriquecimiento, pero no resuelve por sí solo la identidad base.

  Entonces:

  - events = prioridad inmediata
  - dividends/splits = segunda capa de robustez/consistencia.


› dame los atributos que pediras ahora y polygon ofrece en esta llamada


• Para la llamada de ticker events (/vX/reference/tickers/{id}/events) pediría y guardaría estos atributos que Polygon expone en el
  payload de eventos:

  1. results.ticker (ticker asociado al evento)
  2. results.name (nombre asociado)
  3. results.composite_figi
  4. results.share_class_figi
  5. results.cik
  6. results.event_type (tipo de evento)
  7. results.execution_date / date (fecha efectiva del evento)
  8. results.created_utc (si viene)
  9. results.updated_utc (si viene)
  10. results.* campos específicos por tipo de evento (p. ej. ticker antiguo/nuevo en symbol change)

  Además, para trazabilidad técnica guardaría siempre:
  11. request_id (ticker/identificador solicitado)
  12. http_status
  13. next_url paginado (si aplica)
  14. asof_snapshot_date (fecha del registro base que estamos enriqueciendo)

  Nota: los campos exactos pueden variar por event_type (payload heterogéneo), por eso conviene persistir también el raw_event_json por
  fila para no perder información.




  ----



   entonces, de nuevo que campos vamos a descargar ahora?
  - list_date
  - delisted_utc
  - cik
  - composite_figi
  - share_class_figi
  - primary_exchange
  - type
  - last_updated_utc

  2. Ticker Events (/vX/reference/tickers/{id}/events) para identidad/lifecycle

  - event_type
  - execution_date/date
  - ticker (old/new según evento)
  - name (si viene)
  - composite_figi (si viene)
  - share_class_figi (si viene)
  - cik (si viene)
  - updated_utc/created_utc (si vienen)
  - raw_event_json completo (para no perder campos específicos por tipo)

  3. Metadatos de trazabilidad en tu parquet

  - request_ticker
  - request_snapshot_date
  - http_status
  - asof_found
  - source_* por campo (asof, events, fallback, missing)

-----

  entonces no entiendo lo de marketcap como se save en deslistadas para toda la vida de la compañia? y para listadas? como filtradremos
  luego microcaps o smallcas? Y estoy comparando con tradingview y veo muchas que nuestro first_seen_date y last_seen_date no
  conincide, como arreglamos eso?


• Buena observación. Hay 3 temas distintos:

  1. Market cap PTI (listadas y deslistadas)

  - No se “guarda para toda la vida” con un único valor.
  - Se calcula por fecha:
      - market_cap_t = close_t * shares_outstanding_t
  - Para deslistadas, solo mientras tengan ventana de vida y datos disponibles.
  - Si falta shares_outstanding_t, usas LOCF<=180d; si no hay, queda null.

  2. Filtro micro/small caps

  - También es por fecha:
      - is_micro_t = market_cap_t < umbral_micro
      - is_small_t = market_cap_t < 2B
  - Un ticker puede entrar/salir según el día.
  - No se etiqueta fijo para siempre.

  3. first_seen_date/last_seen_date vs TradingView

  - Es normal que difiera: tus fechas son “primera/última aparición en Polygon reference PTI con tus filtros”, no necesariamente fechas
    oficiales de listing/delisting del exchange.
  - Para corregir:
      1. usar list_date/delisted_utc cuando estén disponibles,
      2. complementar con ticker events (changes/mergers),
      3. definir regla de reconciliación:
          - official_start = min(list_date, first_seen_date) (si list_date confiable),
          - official_end = max(delist_date, last_seen_date) (según evidencia),
      4. guardar date_source_start/end (official, events, inferred) y confianza.

  Si quieres, el próximo script que te hago es ese reconciliador de lifecycle para alinear mejor con fuentes tipo TradingView/official
  listings antes de calcular small-caps PTI.





  ----


═════════════════════════════════════════════════════════════════════════════════════════
Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bias
═════════════════════════════════════════════════════════════════════════════════════════
  
  1) Gate de datos (obligatorio antes de clasificar)

  1. Confirmar fuente de close_t (day_aggs_v1) disponible localmente.
  2. Confirmar fuente temporal de shares_outstanding_t (no está en tickers_panel_pti; hay que construirla).
  3. Si falta alguno, no calcular is_small_cap_t final.

  2) Spine PTI (ya lo tienes)

  1. Base diaria desde tickers_panel_pti (2005-2025/2026 parcial), con: date, ticker, entity_id, exchange, status.
  2. Clave de entidad para salida:
      - figi = coalesce(composite_figi, share_class_figi)
      - fallback técnico: entity_id si FIGI falta (con flag).

  3) Serie de precio (close_t)

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



