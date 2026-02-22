
- [porque un estudio de la data para ver su calidad si ppolygon garantiza y vende una data de calidad?](#porque-un-estudio-de-la-data-para-ver-su-calidad-si-ppolygon-garantiza-y-vende-una-data-de-calidad)
- [Polygon vs Nosotros: por que ambos tratamientos son necesarios](#polygon-vs-nosotros-por-que-ambos-tratamientos-son-necesarios)
- [Constancia Oficial: Tratamiento de Datos Crudos en Polygon (ahora Massive)](#constancia-oficial-tratamiento-de-datos-crudos-en-polygon-ahora-massive)

---

## porque un estudio de la data para ver su calidad si ppolygon garantiza y vende una data de calidad?

Porque "data de calidad del proveedor" y "data apta para tu backtest" no son equivalentes.

**Calidad vendor != calidad para tu hipotesis**

- Polygon garantiza un estandar de distribucion/mercado.
- Tu necesitas validar que, en tu universo y periodo, el dato sostiene tus reglas de senal/ejecucion.

**Tu uso es mas exigente**

- Backtest requiere comparabilidad entre fuentes (OHLCV, quotes, trades), cobertura estable, y trazabilidad.
- Un feed puede ser correcto comercialmente y aun asi introducir sesgo en tu experimento (ej. truncados, huecos, desalineaciones puntuales).

**Responsabilidad cientifica**

- Si luego publicas resultados o tomas decisiones de capital, debes demostrar:
- que datos entraron,
- que se excluyo,
- por que,
- con que impacto.
- Eso no lo puede certificar el vendor por ti.

**Diferencias metodologicas legitimas**

- Polygon aplica reglas oficiales de agregacion (sale conditions, correcciones, late trades).
- Tu motor necesita reglas adicionales de elegibilidad para replicabilidad (GO/WARN/FAIL, coverage, comparable_set).

En una frase:

- Polygon vende market data de calidad general.
- Nosotros certificar "fitness-for-purpose" para nuestro backtest especifico.




## Polygon vs Nosotros: por que ambos tratamientos son necesarios

**1) Pregunta central**

Si Polygon ya trata los datos crudos oficiales, por que nosotros debemos volver a tratarlos para backtesting replicable?

Respuesta corta:
- Polygon hace tratamiento de proveedor (vendor layer) para publicar market data util de forma general.
- Nosotros hacemos tratamiento de investigacion/ejecucion (research layer) para garantizar inferencia valida, comparabilidad y reproducibilidad de estrategias.

No son capas redundantes; son capas con objetivos distintos.

**2) Que hace Polygon (vendor layer)**

Polygon/Massive toma datos de mercados oficiales/consolidados (SIP y, segun plan, otras fuentes de mercado) y aplica reglas de construccion y publicacion, por ejemplo:
- elegibilidad por `sale conditions` para actualizar OHLCV,
- inclusion de eventos de correccion/cancelacion y ajustes en agregados,
- gestion de ventanas para late trades / rebroadcast de barras,
- normalizacion de payloads API/WS.

Esto produce un "dato distribuible" y coherente para uso amplio.

Punto clave: Polygon si entrega resultados de su propio tratamiento (trades, quotes, aggregates), pero no entrega tu contrato cientifico especifico de backtest.

**3) Que hacemos nosotros (research/backtest layer)**

Nosotros partimos del dato de proveedor y definimos un contrato operativo de investigacion, explicitando reglas que dependen de nuestra pregunta cuantitativa:
- universo elegible (GO/WARN/FAIL/NOT_COMPARABLE),
- cobertura minima por ticker/periodo/sesion,
- manejo de truncados y gaps,
- comparabilidad de escala entre fuentes (OHLCV vs quotes),
- politicas de exclusion y sensibilidad,
- trazabilidad por corrida (thresholds, decisiones, artifacts).

Esto produce un "dato cientifico" util para medir estrategia sin mezclar ruido de muestra con efecto de se?al.

**4) Diferencia formal de objetivos**

- Vendor objective (Polygon): fidelidad y disponibilidad de market data para muchos casos de uso.
- Backtest objective (nuestro): estimar PnL/riesgo de una regla con control de sesgos y posibilidad de replicacion exacta.

Por eso, incluso con un vendor correcto, sigue siendo obligatorio un tratamiento interno.

**5) Por que no basta con usar Polygon "tal cual"**

Porque un backtest serio exige decisiones que el proveedor no puede tomar por ti:
- que dias/tickers son comparables para TU modelo,
- que umbrales de calidad son aceptables para TU horizonte,
- como separar fallo de muestra vs friccion microestructural,
- como congelar version de datos/reglas para repetir el experimento en el futuro.

Sin esa capa, el resultado puede ser util exploratoriamente, pero no auditado ni replicable de forma robusta.

**6) Ejemplo aplicado a este proyecto**

En este proyecto se detecto:
- comportamiento oficial de vendor (reglas de elegibilidad de agregados), y
- fenomenos de dataset/pipeline interno (`quotes_p95`, dias `exact_50k` truncados).

Conclusion metodologica:
- No atribuir automaticamente a "politica Polygon" lo que es politica/limitacion interna.
- Reportar siempre dos capas en los informes:
  1. `vendor_behavior` (documentado oficialmente),
  2. `internal_processing` (reglas y artefactos del proyecto).

**7) Regla de oro para replicabilidad**

Todo resultado de backtest debe poder responder, con evidencia versionada:
1. que entrego el proveedor,
2. que transformaciones internas se aplicaron,
3. por que esas transformaciones son necesarias para inferencia valida,
4. como reproducir exactamente el mismo dataset y decision en otra fecha.



## Constancia Oficial: Tratamiento de Datos Crudos en Polygon (ahora Massive)

Fecha de elaboración: 2026-02-17
Alcance: U.S. Stocks/Options market data usados en este proyecto.

**Resumen ejecutivo**

Esta nota documenta, con fuentes oficiales de Polygon/Massive y planes SIP, qué tratamiento aplica el proveedor a datos crudos oficiales.

Conclusión clave:
- Polygon/Massive **no entrega OHLCV como simple min/max de ticks**; aplica reglas de elegibilidad por `sale conditions` (CTA/UTP, modo consolidado).
- Polygon/Massive **incluye todos los trades en el feed**, incluso cancelados, y ajusta agregados posteriormente (especialmente EOD).
- Diferencias entre vendors en agregados son esperables; Polygon indica que sigue guías CTA/UTP para agregación consolidada.
- El recorte `quotes_p95` y el truncado operativo `exact_50k` detectado en este proyecto **no están descritos como política oficial de Polygon** en la documentación revisada; deben tratarse como política/artefacto del dataset o pipeline local.

**Hallazgos oficiales (fuente -> implicación)**

1) Origen de datos
- Fuente oficial: Polygon/Massive indica recepción directa desde SIPs (y también conexiones directas/proprietary feeds según plan).
- Implicación: la materia prima base viene de canales regulatorios/consolidados y/o prop feeds con licenciamiento.

2) Qué se transmite en tiempo real
- Fuente oficial: "stream exactly as we receive it" y envían cada trade y NBBO quote en WebSocket.
- Implicación: para reconciliación tick-level, la expectativa es alta completitud del stream recibido.

3) Timestamps
- Fuente oficial: en endpoints de trades/quotes retornan SIP, participant y TRF timestamp; en WebSocket de trades/quotes retornan SIP timestamp.
- Implicación: análisis de drift/alineación debe distinguir timestamp de origen vs timestamp de consolidación.

4) Canceled trades
- Fuente oficial: trades cancelados se incluyen en feed; señalados por trade correction field `e`; agregados se actualizan al final del día para reflejar cambios.
- Implicación: no asumir inmutabilidad intradía de agregados; revisar correcciones EOD.

5) Construcción de OHLCV
- Fuente oficial: elegibilidad por `sale conditions`; endpoint de condiciones expone `update_rules` (`updates_high_low`, `updates_open_close`, `updates_volume`).
- Implicación: reproducir OHLCV requiere aplicar matriz de condiciones, no solo agregación aritmética de todos los ticks.

6) Consolidated vs Market Center
- Fuente oficial: SIP publica guías consolidated y market_center; Polygon indica que sus agregados son de feed consolidado.
- Implicación: comparar contra barras por exchange sin ajustar metodología produce discrepancias legítimas.

7) Late trades y rebroadcast de barras
- Fuente oficial: para second bars esperan +2s y mantienen buffer 15 min; para minute bars recalculan y rebroadcast dentro de ventana; fuera de ventana impacta EOD.
- Implicación: en tiempo real puede haber barras tardías/reemitidas por QA deliberado.

8) Missing aggregates
- Fuente oficial: no se publica barra si no hubo trades elegibles o no cambió OHLC.
- Implicación: huecos de barras no implican necesariamente pérdida de datos.

9) Extended hours y dark pools
- Fuente oficial: incluyen trades en pre/after hours; muchos no actualizan agregados por condiciones. Dark pool identificado por `exchange=4` + `trf_id`.
- Implicación: cobertura de ticks extendidos puede no traducirse 1:1 en barras OHLCV.

**Comparativa: "crudo oficial" vs "tratamiento Polygon/Massive"**

- Input crudo: trades/quotes desde SIPs (y/o prop feeds según plan)  
  -> Tratamiento: normalización de campos y condiciones, publicación API/WS.

- Trades con sale conditions múltiples  
  -> Tratamiento: elegibilidad por matrices CTA/UTP; una condición "NO" puede bloquear update de campo agregado.

- Trades cancelados/corregidos  
  -> Tratamiento: se emiten en feed; agregados reflejan ajustes (especialmente EOD).

- Llegadas tardías (ej. reportes FINRA)  
  -> Tratamiento: buffers y recálculo/rebroadcast de barras en ventanas definidas.

- Sesión extendida  
  -> Tratamiento: ticks sí; update de OHLCV depende de sale conditions elegibles.

**Lo que NO queda sustentado como política oficial del proveedor (y debe marcarse interno)
**
- Filtro `quotes_p95` específico de este dataset del proyecto.
- Truncado por archivo `exact_50k` observado en ciertos días.

Ambos deben clasificarse como:
- política de ingestión/curación interna, o
- limitación operativa del pipeline/dataset entregado,
no como regla pública oficial de Polygon/Massive (según fuentes revisadas).

**Recomendación de trazabilidad para el proyecto**

Para cada notebook/artefacto de integridad y backtest:
1. Añadir campo `vendor_official_policy_ref` con URL fuente.
2. Añadir campo `internal_dataset_policy_ref` para reglas internas (`quotes_p95`, truncados, exclusiones).
3. Separar siempre en reporte final:
- `vendor_behavior` (oficial documentado)
- `internal_processing` (pipeline local)

**Fuentes oficiales**

- Where does stock data come from (KB): https://polygon.io/knowledge-base/article/where-does-stock-data-come-from
- Understanding the SIPs (blog): https://polygon.io/blog/understanding-the-sips
- Stocks REST overview (docs): https://polygon.io/docs/stocks/getting-started
- How does Polygon create aggregate bars (KB): https://polygon.io/knowledge-base/article/how-does-polygon-create-aggregate-bars
- How does Polygon create OHLCV bars (KB): https://polygon.io/knowledge-base/article/how-does-polygon-create-the-open-high-low-close-volume-aggregate-bars
- Understanding Trade Eligibility (blog): https://polygon.io/blog/understanding-trade-eligibility/
- Trade ticks not matching aggregates (blog): https://polygon.io/blog/trade-ticks-not-matching-aggregates/
- Why missing aggregates (KB): https://polygon.io/knowledge-base/article/why-are-there-missing-aggregates-in-polygons-data
- Late aggregate bars via WebSocket (KB): https://polygon.io/knowledge-base/article/why-am-i-receiving-a-late-aggregate-bar-through-polygons-websockets
- Aggregate bar delays (blog): https://polygon.io/blog/aggregate-bar-delays/
- Canceled trades handling (KB): https://polygon.io/knowledge-base/article/how-much-does-polygons-feeds-handle-canceled-trades
- WebSocket data volume / exact stream claim (KB): https://polygon.io/knowledge-base/article/how-much-data-is-streamed-through-polygons-websockets
- Timestamps returned (KB): https://polygon.io/knowledge-base/article/which-timestamps-are-returned-for-polygons-stock-trades-and-nbbo-quotes
- Dark pool identification (KB): https://polygon.io/knowledge-base/article/does-polygon-offer-dark-pool-data
- Pre/after-hours coverage (KB): https://polygon.io/knowledge-base/article/does-polygon-offer-pre-market-and-after-hours-data

**Especificaciones regulatorias referenciadas por Polygon**

- CTA spec (sale condition matrix): https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
- UTP spec (sale condition matrix): https://www.utpplan.com/DOC/UtpBinaryOutputSpec.pdf

**Canales oficiales de discrepancias**

- Support: support@polygon.io
- Public issue tracker (docs/issues): https://github.com/polygon-io/issues/issues

