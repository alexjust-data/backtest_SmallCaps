# Programa de Integridad de Datos - Marco Cientifico de QA (Small Caps)

Esta carpeta es el marco canonico y exclusivo para validar la calidad de datos de mercado locales antes de cualquier uso en backtest de produccion.

Alcance:
- Solo integridad de datos de mercado.
- Sin afirmaciones de alpha de estrategia.
- Sin optimizacion de PnL de ejecucion.

Objetivo principal:
- Decidir GO / NO-GO sobre la fiabilidad de los datos locales bajo un proceso cientifico, reproducible y auditable.

---

## 1. Por que existe este programa

El universo small cap (especialmente en regimenes tipo pump-and-dump) es fragil desde el punto de vista de calidad de datos:
- discontinuidades de lifecycle (listadas/deslistadas/reactivadas),
- regimenes de liquidez escasa,
- concentracion off-exchange,
- inestabilidad de timestamps y secuencias,
- mal uso de condition codes,
- patologias de spread y distorsiones de microestructura.

Un simple "el esquema pasa" no es suficiente.
Se requiere validacion por capas:
1) integridad estructural,
2) integridad temporal,
3) integridad de calendario/sesiones,
4) integridad de microestructura,
5) contexto de benchmark externo.

---

## 2. Orden canonico de ejecucion (ascendente estricto)

Ejecutar notebooks en este orden exacto.

1. `01_snapshot_inventory_refactored.ipynb`
2. `02_schema_validation.ipynb`
3. `03_time_coverage.ipynb`
4. `04_calendar_definition.ipynb`
5. `05_session_rules.ipynb`
6. `06_ohlcv_vs_quotes.ipynb`
7. `07_outliers_and_anomalies.ipynb`
8. `08_condition_code_eligibility.ipynb`
9. `09_sequence_timestamp_integrity.ipynb`
10. `10_clock_drift_qa.ipynb`
11. `11_nbbo_spread_sanity.ipynb`
12. `12_off_exchange_trf_reconciliation.ipynb`
13. `13_execution_quality_external_benchmark_605.ipynb` (benchmark no bloqueante)

Por que este orden es necesario:
- 01-03 aseguran existencia, parseo y cobertura temporal coherente.
- 04-05 restringen observaciones a verdad oficial de lifecycle/calendario/sesion.
- 06-07 validan coherencia de precios y comportamiento anomalico.
- 08-12 validan semantica de microestructura y estabilidad de eventos.
- 13 aporta contexto externo; informa, pero no bloquea el gate local.

---

## 3. Que valida cada notebook (proposito paso a paso)

### `01_snapshot_inventory_refactored.ipynb`
Proposito:
- Construir inventario reproducible desde manifest + cache local.
- Confirmar completitud de materializacion antes de interpretar resultados.

Outputs:
- artefactos de inventario y gate inicial en runs.

Si falla:
- Ningun resultado posterior de QA es confiable.

### `02_schema_validation.ipynb`
Proposito:
- Validar esquemas canonicos y tipos de campos clave.
- Detectar schema drift y cambios incompatibles.

Si falla:
- Las metricas posteriores pueden estar corruptas sin error explicito.

### `03_time_coverage.ipynb`
Proposito:
- Validar continuidad temporal y patrones de cobertura.
- Verificar supuestos de unidades timestamp y alineacion.

Si falla:
- Analisis por regimen y eventos puede quedar sesgado por huecos ocultos.

### `04_calendar_definition.ipynb`
Proposito:
- Reconciliar datos observados contra expectativas oficiales de lifecycle/calendario.
- Detectar fechas faltantes/sobrantes y drift de particiones.

Si falla:
- El dataset no es production-grade para logica por fecha.

### `05_session_rules.ipynb`
Proposito:
- Construir fronteras de sesion deterministas desde calendario oficial.
- Convertir fechas validadas en restricciones intradia operativas.

Si falla:
- La segmentacion intradia queda ambigua y no reproducible.

### `06_ohlcv_vs_quotes.ipynb`
Proposito:
- Validar consistencia OHLCV contra comportamiento de quotes/trades.
- Detectar distorsiones de barras y problemas de rango por minuto.

Si falla:
- Senales derivadas de barras son estructuralmente poco confiables.

### `07_outliers_and_anomalies.ipynb`
Proposito:
- Cuantificar outliers en OHLCV/quotes/trades con estadistica robusta.
- Separar colas pesadas esperables de corrupcion patologica.

Si falla:
- La robustez de senales probablemente esta sobreestimada.

### `08_condition_code_eligibility.ipynb`
Proposito:
- Validar uso correcto de condition codes en trades/quotes.
- Asegurar interpretacion bajo semantica oficial.

Si falla:
- Last/high/low/volume y logica de quotes pueden estar mal definidos.

### `09_sequence_timestamp_integrity.ipynb`
Proposito:
- Validar monotonicidad de secuencia, duplicados y orden de eventos.
- Medir coherencia participant vs SIP timestamp.

Si falla:
- Analisis de tiempo de evento y microestructura puede ser invalido.

### `10_clock_drift_qa.ipynb`
Proposito:
- Medir drift y jitter por particiones tipo venue.
- Activar alertas si se exceden tolerancias internas.

Si falla:
- Logica sensible a tiempo (latencia/simulacion de orden) pierde fiabilidad.

### `11_nbbo_spread_sanity.ipynb`
Proposito:
- Medir spreads negativos, locked/crossed y colas extremas de spread.
- Detectar estados anormales de quotes.

Si falla:
- Supuestos de ejecucion basados en quotes no son estables.

### `12_off_exchange_trf_reconciliation.ipynb`
Proposito:
- Estimar cuota off-exchange/TRF y detectar concentraciones anormales.
- Contextualizar formacion de liquidez en small caps.

Si falla:
- Supuestos de composicion de venues pueden ser enganosos.

### `13_execution_quality_external_benchmark_605.ipynb`
Proposito:
- Comparar metricas internas de calidad de ejecucion contra referencias externas estilo Rule 605.
- Generar gap report estructurado.

Si falla:
- No implica NO-GO directo de integridad local.
- Es una senal de calidad comparativa externa.

---

## 4. Controles avanzados nivel hedge fund (solicitados e incluidos)

### 4.1 Trade/Quote Condition Eligibility
Que se valida:
- presencia de condition codes,
- completitud de mapeo,
- tasa de condiciones desconocidas,
- elegibilidad semantica para metrica descendente.

Fuentes oficiales:
- UTP UTDF spec:
  - https://nasdaqtrader.com/content/technicalsupport/specifications/utp/utdfspecification.pdf
- CTA/CTS/CQS spec:
  - https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Input_Specification.pdf
- Polygon condition mapping:
  - https://polygon.io/docs/rest/stocks/market-operations/condition-codes

### 4.2 Sequence & Timestamp Integrity
Que se valida:
- monotonicidad de `sequence_number`,
- grupos duplicados,
- delta participant vs SIP,
- riesgo de out-of-order bursts.

Referencia:
- Polygon trades fields:
  - https://polygon.io/docs/websocket/stocks/trades

### 4.3 Clock-Drift QA
Que se valida:
- drift medio/p99,
- jitter (desviacion estandar),
- alertas por umbral.

Referencias regulatorias:
- FINRA Rule 4590:
  - https://www.finra.org/rules-guidance/rulebooks/finra-rules/4590
- CAT clock sync FAQ:
  - https://www.catnmsplan.com/faq/r1

### 4.4 NBBO/Spread Sanity
Que se valida:
- tasa de spread negativo,
- locked/crossed,
- colas extremas de spread.

Referencia:
- SEC Reg NMS Rule 611:
  - https://www.law.cornell.edu/cfr/text/17/242.611

### 4.5 Off-Exchange/TRF Reconciliation
Que se valida:
- cuota estimada off-exchange,
- concentraciones anormales.

Fuentes:
- FINRA TRF overview:
  - https://www.finra.org/filing-reporting/trade-reporting-facility-trf
- FINRA OTC transparency:
  - https://www.finra.org/filing-reporting/otc-transparency
- FINRA short sale volume:
  - https://www.finra.org/finra-data/browse-catalog/short-sale-volume

### 4.6 Benchmark externo de calidad de ejecucion
Que se valida:
- comparabilidad metodologica interna vs externa,
- gap report reproducible.

Fuente:
- SEC Rule 605 update (2024):
  - https://www.sec.gov/newsroom/press-releases/2024-32

---

## 5. Politica GO / NO-GO (aceptacion de datos para produccion)

Condiciones minimas de GO:
- Esquema valido y estable.
- Cobertura coherente con lifecycle/calendario oficial.
- Sesiones y timestamps coherentes.
- Consistencia OHLCV vs quotes/trades dentro de umbral.
- Outliers/anomalias sin corrupcion critica no resuelta.
- Sin alertas criticas en condition/sequence/drift/spread/off-exchange.

Condiciones de NO-GO (cualquiera de estas bloquea):
- rotura estructural o schema break,
- fallo critico en reconciliacion de calendario/lifecycle,
- inestabilidad severa de secuencias/timestamps,
- patologia persistente de spread negativo/crossed,
- concentracion off-exchange anomala sin explicacion y con flags de integridad.

---

## 6. Reproducibilidad y artefactos

Cada notebook escribe artefactos versionados en carpetas de runs.
Outputs tipicos:
- tablas parquet de metricas,
- JSON de decision/gap,
- diagnosticos visuales en salida de notebook.

Principio operativo:
- Toda conclusion PASS/WARN/FAIL debe estar anclada a artefactos guardados.

---

## 7. Limite de alcance

- Fill model y cost model son capas de simulacion downstream.
- No son gates primarios de integridad de datos.
- Pueden consumir resultados de este QA, pero no sustituirlo.

---

## 8. Guia practica de ejecucion

Para cada batch nuevo de tickers:
1) refrescar manifest/cache,
2) ejecutar notebooks en orden ascendente estricto,
3) consolidar resumen de gate,
4) aprobar uso en produccion solo si se cumplen criterios GO.

Para small caps deslistadas/reactivadas:
- no saltar reconciliacion de lifecycle/calendario,
- no inferir sesiones solo desde heuristica de datos,
- mantener evidencia oficial adjunta a los artefactos de decision.
