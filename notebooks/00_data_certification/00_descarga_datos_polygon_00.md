Polygon/Massive no tiene “endpoints distintos para CS” en elsentido de URL separadas.

Tiene endpoints de stocks, y tú filtras Common Stock con:

- market=stocks
- type=CS
- locale=us

La referencia maestra oficial es:

- Stocks Overview (https://massive.com/docs/stocks/)
- All Tickers (https://massive.com/docs/rest/stocks/tickers/all-tickers?auth=login)
- Ticker Overview (https://polygon.io/docs/rest/stocks/tickers/ticker-overview/)
- Changelog (https://www.massive.com/changelog)

Lista práctica de endpoints oficiales para U.S. common stocks (type=CS)

**Reference / Universe**

- GET /v3/reference/tickers
    - universo de tickers; aquí filtras market=stocks&type=CS&active=...&date=...
- GET /v3/reference/tickers/{ticker}
    - detalle del ticker; aquí salen market_cap, weighted_shares_outstanding, etc.
- GET /v3/reference/tickers/types
- GET /v3/reference/exchanges
- GET /vX/reference/tickers/{id}/events
    - experimental
- GET /v3/reference/splits
- GET /v3/reference/dividends

**Market Data / Historical**

- GET /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}
- GET /v2/aggs/grouped/locale/us/market/stocks/{date}
- GET /v1/open-close/{stocksTicker}/{date}
- GET /v3/trades/{stockTicker}
- GET /v3/quotes/{stockTicker}

**Snapshots**

- GET /v2/snapshot/locale/us/markets/stocks/tickers
- GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}

**Fundamentals**

- GET /stocks/financials/v1/income-statements
- GET /stocks/financials/v1/balance-sheets
- GET /stocks/financials/v1/cash-flow-statements
- GET /stocks/financials/v1/ratios
- GET /stocks/financials/v1/short-interest
- GET /stocks/financials/v1/short-volume

**News**

- GET /v2/reference/news

**WebSocket**

- Stocks trades stream
- Stocks quotes stream
Links oficiales:
- Stocks WebSocket Quotes (https://massive.com/docs/websocket/stocks/quotes)
- Stocks WebSocket Trades (https://massive.com/docs/stocks/ws_stocks_t)

**Flat Files**

- us_stocks_sip/day_aggs_v1
- us_stocks_sip/minute_aggs_v1
- us_stocks_sip/quotes_v1
- us_stocks_sip/trades_v1
Ejemplo oficial:
- Day Aggregates Flat Files (https://massive.com/docs/flat-files/stocks/day-aggregates/2025/05)

Lo que necesitas para market cap PTI

- universo PTI:
    - GET /v3/reference/tickers
- market cap por ticker:
    - GET /v3/reference/tickers/{ticker}?date=YYYY-MM-DD

Ese segundo es el endpoint clave.

Sobre C:\TSIS_Data\data\reference
He revisado C:\TSIS_Data\data\reference y ahí solo hay:

- condition_codes.parquet
- exchanges.parquet
- market_status_upcoming.parquet
- ticker_types.parquet

No hay ahí:

- all tickers
- ticker overview
- market_cap
- weighted_shares_outstanding

O sea: en ese path no está toda la data que Polygon ofrece para esos endpoints.

PTI
PTI = Point In Time
Significa usar solo la información que estaba disponible en la fecha t, sin mirar el futuro.

Si quieres, el siguiente paso te hago:

- un script download_polygon_overview_marketcap_pti.py
- y otro audit_reference_coverage_vs_polygon_endpoints.py para dejar cerrado qué os falta descargar.



## cómo se construyeron tickers_2005_2026.parquet y tickers_2005_2026_upper.parquet.

**1. Origen base del universo**

  El universo base se construyó con `build_universe_pti.py`.

  Ese script llama a Polygon:

  - `GET /v3/reference/tickers`

  Y aplica estos filtros exactos en cada corte temporal:

```
  - market = stocks
  - locale = us
  - type = CS
  - exchange IN {XNAS, XNYS, XASE, ARCX}
  - date = snapshot_date
  - active:
      - por defecto all (activos + inactivos)
      - en vuestra corrida real fue all
```
  - date en la llamada es la fecha que se consulta a Polygon
  - snapshot_date en el parquet es esa misma fecha guardada localmente como sello del corte


  Eso se ve en el código:

```
  - params["market"] = "stocks"
  - params["locale"] = "us"
  - params["type"] = "CS"
  - params["exchange"] = exchange
  - params["date"] = date_str
  - active_filter = "all" por defecto, y solo cambia si pasas true/false

```

  **2. Frecuencia temporal usada**

**Qué significa PTI** Point In Time.

  Significa:

  - usar solo la información que estaba disponible en una fecha concreta t
  - sin usar datos del futuro
  - para evitar look-ahead bias

  Ejemplo:

  - si el 15 de marzo de 2014 quieres saber si un ticker era small-cap,
  - solo puedes usar:
      - el precio conocido ese día
      - y el último shares_outstanding que ya estuviera publicado en esa fecha
  - no puedes usar un filing de abril de 2014 para clasificar marzo de 2014

  La corrida real del panel PTI fue:
```
  - `frequency = daily`
```
  Eso está en:
```
  - `build_universe_pti.meta.json`
  - `build_universe_pti.progress.json`
```
  Y sus métricas reales fueron:

```
  - start = 2005-01-01
  - end = 2026-03-09
  - requested_cuts = 7738
  - successful_cuts = 7738
  - frequency = daily
  - active_filter = all
```

  **3. Cómo se deduplicó dentro de cada snapshot**

  Dentro de cada fecha, el script no se queda con todas las filas brutas. Deduplica por:

```
  - snapshot_date
  - entity_id
```
  Y prioriza calidad de registro con este orden:

```
  - has_composite_figi desc
  - has_share_class_figi desc
  - active desc
  - has_list_date desc
  - exchange_priority asc
```
  Además exchange_priority se define así:

```
  - XNAS = 0
  - XNYS = 1
  - XASE = 2
  - ARCX = 3
  - otros = 999
```

  **4. Cómo se construyó tickers_all.parquet**

  Después de construir el panel diario tickers_panel_pti, el script agrupa por entity_id y calcula:

```
  - first_seen_date = min(snapshot_date)
  - last_seen_date = max(snapshot_date)
```

  Luego toma la última fila observada por entity_id y genera:

```
  - tickers_all.parquet
```
  Y clasifica:
```
  - status = active si last_seen_date == last_snapshot
  - status = inactive si no
```
  En vuestra corrida:
```
  - entities = 15979
  - active = 5256
  - inactive = 10723
```

  **5. Cómo se construyó tickers_2005_2026.parquet**

  Ese archivo no se genera directamente desde Polygon. Se genera desde `tickers_all.parquet` usando
  `build_tickers_2005_2026.py`.

  Filtro exacto aplicado:

  - incluir si el intervalo `[first_seen_date, last_seen_date]` intersecta `[2005-01-01, 2026-12-31]`

  En código:
```
  - first_seen_date.notna()
  - last_seen_date.notna()
  - first_seen_date <= end
  - last_seen_date >= start
```
  Eso es todo.

  Por eso:
```
  - tickers_2005_2026.parquet
  - tiene 15979 filas
  - exactamente igual que tickers_all.parquet
```
  Eso te dice que, en vuestra corrida real, todas las entidades de *tickers_all* intersectaban la ventana
  2005-2026.

**  6. Cómo se construyó tickers_2005_2026_upper.parquet**

  Este archivo no añade filtros de negocio nuevos.
  Se construyó en notebook con esta transformación sobre tickers_2005_2026.parquet:

```
  - ticker = upper(trim(ticker))
  - eliminar ticker nulos o vacíos
  - drop_duplicates(subset=["ticker"])
  - sort_values("ticker")
```

  Ese paso está documentado en `01_data_fundamentals.ipynb`, donde se dejó esta lógica:

```
  - leer tickers_2005_2026.parquet
  - ticker a uppercase
  - deduplicar por ticker
  - guardar tickers_2005_2026_upper.parquet
```
  Por eso:
```
  - tickers_2005_2026.parquet = 15979 filas
  - tickers_2005_2026_upper.parquet = 12468 filas
```
  La diferencia exacta es:

  - 15979 - 12468 = 3511
  - esas 3511 filas no son “filtradas por fundamentals” ni “quitadas por market cap”
  - son colapsadas al deduplicar múltiples filas que compartían el mismo ticker


**7. Qué filtros exactos hubo, resumidos**

  Filtros de negocio reales del universo PTI original:
```
  - market = stocks
  - locale = us
  - type = CS
  - exchange ∈ {XNAS, XNYS, XASE, ARCX}
  - date diario entre 2005-01-01 y 2026-03-09
  - active_filter = all
```

  Filtro temporal para tickers_2005_2026.parquet:
```
  - intersección de [first_seen_date, last_seen_date] con [2005-01-01, 2026-12-31]
```
  Transformación para tickers_2005_2026_upper.parquet:
```
  - uppercase del ticker
  - trim
  - quitar nulos/vacíos
  - deduplicar por ticker
```

**8. Interpretación importante**

  `tickers_2005_2026_upper.parquet` **no es un universo PTI** “científico” completo.  
  Es un universo operativo por ticker único para descargas masivas por símbolo.

  Al deduplicar por ticker:

  - pierdes multiplicidad de entidades/cortes
  - ganas una lista limpia de símbolos para downloaders

  Por eso era válido para:

  - daily
  - 1m
  - fundamentals

  Pero no es la misma cosa que:

  - el panel PTI diario
  - ni el universo enriquecido por entidad

  Si quieres, el siguiente paso te hago un script de auditoría audit_universe_filters_exact.py que te deje
  en una celda:

  - filtros aplicados
  - conteos por fase
  - y diferencias exactas entre:
      - tickers_all
      - tickers_2005_2026
      - tickers_2005_2026_upper


### Un universo PTI completo sería:

```
- para cada fecha t
- la lista de todos los instrumentos que realmente existían y eran elegibles en t
- con sus atributos conocidos en t
- sin usar información futura
- y sin perder los que luego desaparecieron
```

En vuestro caso, para US Common Stocks, eso significa:

```
- fecha a fecha
- market=stocks
- locale=us
- type=CS
- exchanges objetivo
- activos e inactivos históricos que en esa fecha seguían vivos
- con identidad estable por entity_id/FIGI si es posible, no solo por ticker
```

Qué tendría un PTI completo de verdad

Para cada date:

```
- ticker
- entity_id
- primary_exchange
- type
- active/as-of
- list_date si ya era conocida
- delist_date solo si ya había ocurrido o era inferible sin futuro
- campos de identificación consistentes
- continuidad ante ticker changes / mergers / delistings
```

Y además:

```
- si una acción existió en 2008 y murió en 2011, tiene que estar en 2008-2011
- aunque hoy ya no exista
- si cambia de ticker, debes preservar la continuidad de identidad
- si el ticker se recicla, no debes mezclar dos entidades distintas
```

Qué sería “completo” en la práctica

Completo no significa “perfecto”.

Significa que cubre estas tres cosas:

**1. Cobertura temporal** - todos los cortes del periodo objetivo  
**2. Cobertura de entidades** - todos los CS elegibles en cada corte  
**3. Cobertura de lifecycle** - altas, bajas, cambios y discontinuidades sin survivorship bias  

Qué tenéis vosotros ahora

Lo más cercano a eso es: - `tickers_panel_pti`

Porque:

```
- está construido por fecha
- con date = snapshot_date
- con active_filter = all
- y luego se resume a tickers_all
```

Pero no es necesariamente “completo perfecto” porque depende de lo que Polygon resuelva bien
históricamente en All Tickers.

Por eso luego necesitasteis reforzar con:

```
- lifecycle oficial
- inactive catalog
- ticker events
- hybrid enriched
```

**Diferencia con una simple lista de tickers**

- `tickers_2005_2026_upper.parquet` no es un universo PTI completo

Es solo:
```
- una lista operativa de tickers únicos
- útil para descargar por símbolo
- pero no conserva la dimensión fecha-entidad
```

**Definición corta**


Un universo PTI completo es una tabla diaria histórica de elegibilidad e identidad, donde cada fila
responde:
```
“¿Qué acción común de EE. UU. existía ese día,   
    bajo qué identidad,   
    en qué exchange,   
    y con qué estado conocido en ese momento?”  
```

Si quieres, el siguiente paso te lo aterrizo a vuestro proyecto:

- qué le falta exactamente a tickers_panel_pti para considerarlo “PTI completo certificable”.


## universo PTI completo y certificable para US Common Stocks

Primero descargar y materializar toda la capa Reference / Universe, y después construir el PTI sobre datos
  locales.
  Eso es mejor que mezclar construcción PTI y llamadas online al mismo tiempo.

  Por qué es mejor

  1. Separas adquisición de datos de lógica PTI.

  - primero descargas
  - luego reconstruyes

  2. Ganas reproducibilidad.

  - puedes versionar snapshots/reference
  - si cambias reglas PTI no vuelves a pegarle a Polygon

  3. Ganas auditabilidad.

  - sabes exactamente qué devolvió cada endpoint
  - puedes medir cobertura, huecos y conflictos antes de construir el panel final

  4. Ganas resiliencia.

  - si falla events o overview, no rompes toda la construcción PTI
  - puedes rerun por endpoint

  Orden correcto
  Fase A. Descargar Reference / Universe

  - GET /v3/reference/tickers
  - GET /v3/reference/tickers/{ticker}
  - GET /v3/reference/tickers/types
  - GET /v3/reference/exchanges
  - GET /vX/reference/tickers/{id}/events
  - GET /v3/reference/splits
  - GET /v3/reference/dividends

  Fase B. Auditar y normalizar

  - cobertura por endpoint
  - conflictos de identidad
  - continuidad ticker/entity
  - cobertura temporal real
  - campos faltantes

  Fase C. Construir PTI completo

  - usar los artefactos descargados
  - no llamar a Polygon en esta fase salvo reparación puntual

  Pero hay un matiz importante
  No todo lo de esa lista tiene la misma granularidad temporal.

  Sí son núcleo PTI

  - /v3/reference/tickers
  - /v3/reference/tickers/{ticker}
  - /vX/reference/tickers/{id}/events

  Son catálogos auxiliares

  - /v3/reference/tickers/types
  - /v3/reference/exchanges

  Son lifecycle/corporate actions

  - /v3/reference/splits
  - /v3/reference/dividends

  Sirven mucho, pero no sustituyen:

  - el universo por fecha
  - ni el overview as-of por ticker

  Diseño correcto
  Yo lo haría en dos scripts, no en uno:

  1. download_reference_universe_polygon.py
     Responsable de:

  - descargar todos esos endpoints
  - persistir raw y normalized
  - dejar checkpoints y auditoría

  2. build_universe_pti_complete_from_reference.py
     Responsable de:

  - leer solo local
  - reconciliar identidad/lifecycle
  - construir el PTI final

  Estructura recomendada de datos

  - reference/tickers_by_date/
  - reference/ticker_overview_asof/
  - reference/ticker_events/
  - reference/splits/
  - reference/dividends/
  - reference/catalogs/ticker_types.parquet
  - reference/catalogs/exchanges.parquet

  Ventaja clave
  Cuando quieras rehacer el PTI:

  - no redescargas
  - solo cambias reglas de construcción
