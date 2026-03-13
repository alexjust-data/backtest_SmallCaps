Documentación oficial de Polygon/Massive: ese dato existe y lo puedes recuperar por endpoint.

Endpoints clave para shares_outstanding_t y market_cap_t:

  1. Ticker Overview (point-in-time)
     GET /v3/reference/tickers/{ticker}?date=YYYY-MM-DD
     Campos relevantes:

  - market_cap
  - weighted_shares_outstanding
  - share_class_shares_outstanding
  - también list_date, delisted_utc, active, cik, figi, etc.
    Fuente: https://polygon.io/docs/rest/stocks/tickers/ticker-overview/

  2. Income Statements (fundamentals v1, históricos por período)
     GET /stocks/financials/v1/income-statements (filtrable por tickers, period_end, etc.)
     Campos relevantes:

  - basic_shares_outstanding
  - diluted_shares_outstanding
  - filing_date, period_end, timeframe
    Fuente: https://polygon.io/docs/rest/stocks/fundamentals/income-statements

  3. All Tickers (catálogo para activos/inactivos)
     GET /v3/reference/tickers
     Incluye:

  - active
  - delisted_utc
  - cik, composite_figi, share_class_figi
    Fuente: https://polygon.io/docs/rest/stocks/tickers/all-tickers

  4. Confirmación oficial de market cap en Ticker Details
     KB oficial: market cap se consulta en Ticker Details v3.
     Fuente: https://intercom.help/polygonio/en/articles/6589968-what-is-market-cap-and-how-does-polygon-calculate-that-value
  5. Ticker events para continuidad de identidad
     GET /vX/reference/tickers/{id}/events (experimental; id puede ser ticker/CUSIP/Composite FIGI).
     Fuente: https://polygon.io/docs/rest/stocks/corporate-actions/ticker-events

  Nota importante (temporal): el changelog de 19-Sep-2025 confirma que financials/ratios v1 son la vía actual (el experimental quedó
  deprecado).
  Fuente: https://polygon.io/changelog

  En resumen: para tu PTI small-cap, la ruta correcta es overview?date= como fuente primaria diaria de shares/market cap, y income-
  statements como refuerzo histórico (LOCF sin look-ahead).



  ----


  En Polygon, a fecha 8-mar-2026, los endpoints de fundamentals de Stocks son estos:

  1. GET /stocks/financials/v1/income-statements
     Docs: https://polygon.io/docs/rest/stocks/fundamentals/income-statements
     Incluye, entre otros: basic_shares_outstanding, diluted_shares_outstanding, revenue, net income, EPS.
  2. GET /stocks/financials/v1/balance-sheets
     Docs: https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets
     Incluye activos, pasivos, equity, deuda, caja, etc.
  3. GET /stocks/financials/v1/cash-flow-statements
     Docs: https://polygon.io/docs/rest/stocks/fundamentals/cash-flow-statements
     Incluye operating/investing/financing cash flow, FCF-related componentes.
  4. GET /stocks/financials/v1/ratios
     Docs: https://polygon.io/docs/rest/stocks/fundamentals/ratios
     Incluye ratios diarios (price-based + financials): market_cap, P/E, EV/EBITDA, ROE, liquidez, etc.

  Endpoint anterior (legacy):

  - GET /vX/reference/financials (deprecated)
    Docs: https://polygon.io/docs/rest/stocks/fundamentals/financials
    El changelog de Polygon (sep-2025) indica migrar a los 4 endpoints v1:
    https://polygon.io/changelog

----


Qué endpoint usar (y qué descargar)

  | Endpoint | Estado | Qué trae | Filtros clave | Llave recomendada |
  |---|---|---|---|---|
  | /stocks/financials/v1/balance-sheets | Vigente | Balance sheet granular | cik, tickers, period_end*, filing_date*, fiscal_year*,
  fiscal_quarter*, timeframe* | cik + period_end + timeframe + filing_date |
  | /stocks/financials/v1/cash-flow-statements | Vigente | Cash flow granular | mismos filtros que balance sheet | cik + period_end +
  timeframe + filing_date |
  | /stocks/financials/v1/income-statements | Vigente | Income statement granular | mismos filtros que balance sheet | cik + period_end
  + timeframe + filing_date |
  | /stocks/financials/v1/ratios | Vigente | Ratios + price/market_cap/EV | ticker/cik y muchos filtros numéricos (*_gt/lte) | ticker +
  date |
  | /stocks/vX/float | Vigente (ruta vX) | Free float y % free float | ticker*, free_float_percent* | ticker + effective_date |
  | /stocks/v1/short-interest | Vigente | Short interest (settlement) | ticker, settlement_date*, days_to_cover*, avg_daily_volume* |
  ticker + settlement_date |
  | /stocks/v1/short-volume | Vigente | Short volume diario detallado | ticker, date*, short_volume_ratio*, total_volume* | ticker +
  date |
  | /vX/reference/financials | Legado | Financials viejo (estructura distinta) | ticker, cik, filing_date*, period_of_report_date*,
  timeframe | usar solo para backfill legado |

  ## Campos modelados (SDK oficial)

  - Balance sheet: 38 campos
  - Cash flow: 32
  - Income statement: 34
  - Ratios: 23
  - Float: 4
  - Short interest: 5
  - Short volume: 15

  Esto lo saqué directamente de tus modelos locales:

  - financials.py
  - financials models
  - reference.py
  - tickers models
  - vX.py

  ## Qué vamos a descargar (recomendado para CS)

  1. Universo base CS: /v3/reference/tickers con market=stocks, type=CS, exchanges XNAS/XNYS/XASE/ARCX, active=true/false, por date
     mensual/trimestral.
  2. Fundamentals core:

  - balance-sheets
  - cash-flow-statements
  - income-statements
  - ratios

  3. Fundamentals de microestructura de flotante/corto:

  - float
  - short-interest
  - short-volume

  4. Evitar /vX/reference/financials salvo reconciliación histórica.

  ## Decisiones de ingeniería clave

  - timeframe: descargar annual + quarterly (separado).
  - Incremental por fecha:
  - Statements: filing_date_gte o period_end_gte.
  - Ratios: date_gte.
  - Float: effective_date incremental.
  - Short datasets: date/settlement_date incremental.
  - Dedupe por llave compuesta (arriba) + updated_at si existe en payload.
  - Mantener ticker y cik; usar cik como ancla contable.

  Fuentes docs:

  - https://polygon.io/docs/rest/stocks/fundamentals/balance-sheets
  - https://polygon.io/docs/rest/stocks/fundamentals/cash-flow-statements
  - https://polygon.io/docs/rest/stocks/fundamentals/income-statements
  - https://polygon.io/docs/rest/stocks/fundamentals/ratios
  - https://polygon.io/docs/rest/stocks/fundamentals/short-interest
  - https://polygon.io/docs/rest/stocks/fundamentals/short-volume
  - https://www.massive.com/changelog

