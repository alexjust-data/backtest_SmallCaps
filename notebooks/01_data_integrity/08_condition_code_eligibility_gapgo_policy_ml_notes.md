# Marco tecnico de policies para Gap&Go en micro/small caps

## Referencias y proposito del documento

Este documento se construye como complemento operativo del notebook `08_condition_code_eligibility.ipynb` y del marco conceptual desarrollado en `08_condition_code_eligibility_SIP_sale_conditions_whitepaper.md`. Su objetivo no es repetir el diagnostico, sino traducirlo a una arquitectura de decision aplicable en backtesting y en etapas posteriores de modelado cuantitativo.

La tesis central es simple: en micro/small caps, la calidad de datos no puede tratarse como una propiedad binaria. Debe modelarse como un conjunto de restricciones explicitas sobre que eventos son utilizables, en que contexto temporal, con que nivel de liquidez y bajo que condiciones de robustez.

## Respuesta base de politica operativa

Las politicas recomendadas para estrategias tipo Gap&Go en micro y small caps son siete: una `condition policy` diferenciada por caso de uso; una politica por sesion intradia; filtros por spread y rango de barra; filtros minimos de liquidez; una politica formal de timestamps y orden temporal; un sistema de gating GO/WARN/FAIL; y, finalmente, pruebas de estres obligatorias sobre variantes de politica. La logica de estas siete capas no es redundante: cada una controla un mecanismo distinto de distorsion.

## Desarrollo didactico y cientifico por capas

### Condition policy por caso de uso

Una condition policy es un contrato de elegibilidad, no una preferencia de implementacion. En terminos practicos, define que eventos actualizan senales, que eventos construyen extremos de barra y que eventos pueden alimentar un modelo de ejecucion. Cientificamente, esta separacion evita mezclar observaciones de semantica microestructural distinta dentro de una misma variable objetivo. Si no se hace, el proceso generador de datos cambia de manera latente entre muestras, y el modelo interpreta ese cambio como "alpha".

En Gap&Go, la recomendacion es usar criterios mas estrictos para ejecucion que para senal y mas estrictos para extremos de barra que para metricas descriptivas. La validez de la policy debe medirse observando su impacto en cobertura retenida, estabilidad de metricas de coherencia y sensibilidad de resultados de estrategia.

### Policy por sesion

La microestructura intradia no es estacionaria. La apertura concentra discontinuidades de liquidez, spread y formacion de precio que no son equivalentes al tramo medio de sesion. Por eso, una politica unica para todo el dia suele introducir errores de calibracion.

La separacion por sesiones (`premarket`, apertura, tramo medio y cierre) permite ajustar umbrales al regimen real de negociacion. Esta capa se valida con metricas segmentadas por sesion, no con promedios globales. Si una estrategia "vive" unicamente en una franja con calidad fragil, debe tratarse como riesgo estructural y no como edge confirmado.

### Filtros por spread y por rango

En microcaps, barras con rango extremadamente estrecho pueden producir senales inestables ante desplazamientos de uno o dos ticks. En paralelo, spreads altos deterioran de forma no lineal la calidad de ejecucion inferida. Los filtros de spread/rango no son cosmeticos; son un mecanismo de control de error de medicion.

Su aplicacion correcta no consiste en "recortar outliers" arbitrariamente, sino en definir fronteras de elegibilidad por regimen. La evaluacion se realiza comparando distribucion de errores antes y despues del filtrado, ademas de verificar la estabilidad de desempeno fuera de muestra.

### Filtros de liquidez

Toda inferencia sobre senales intradia depende de que exista suficiente densidad de informacion. Minutos con baja actividad tienen varianza muy elevada y alta probabilidad de producir inferencias espurias. Por eso se requieren minimos explicitos de `quote_count`, `trade_count` y `dollar_volume`.

La politica de liquidez debe vigilar dos efectos simultaneos: mejorar calidad sin colapsar cobertura. Si la cobertura cae en exceso, se introduce sesgo de seleccion hacia dias/tickers "limpios" y se pierde representatividad operativa.

### Politica de timestamps

Los problemas de secuencia temporal son una fuente clasica de sesgo estructural. Un trade reportado tarde o fuera de orden puede generar ruptura aparente, senal falsa o reconciliacion vacia en joins de validacion. La politica temporal debe fijar reglas de normalizacion de unidad, zona horaria y precedencia entre timestamps de evento y reporte.

El control de esta capa se audita con metricas de drift, out-of-order, duplicidad y coherencia temporal entre fuentes (por ejemplo, OHLCV vs trades reconstruidos).

### Gating GO/WARN/FAIL

El gating no clasifica "datos buenos y malos" de forma abstracta; clasifica riesgo operativo de uso. `GO` habilita uso estandar, `WARN` habilita uso condicionado y `FAIL` bloquea uso productivo para ese slice. Esta taxonomia obliga a separar explicitamente evidencia robusta de evidencia fragil.

La utilidad del gating depende de su trazabilidad. Debe quedar registrado que umbral se incumplio, en que periodo y bajo que version de policy.

### Stress tests de policy

Una estrategia robusta no puede depender de una unica configuracion de limpieza. Por eso el protocolo minimo exige ejecutar variantes base, estricta y relajada de policy, y medir sensibilidad sobre PnL, drawdown, hit-rate, turnover y estabilidad temporal.

Cuando el edge desaparece al endurecer controles razonables, la lectura correcta es fragilidad metodologica, no oportunidad de mercado.

## Separacion explicita: trading algoritmico vs ML

Aunque comparten datos fuente, los objetivos de ambas capas son distintos y deben tratarse como pipelines separados.

En trading algoritmico, la policy busca realismo de senal y ejecucion bajo microestructura observable. En ML, la policy pasa a ser parte del contrato de features/labels y su funcion principal es evitar leakage, deriva no controlada y dependencia de artefactos de preprocesado.

La consecuencia practica es que train/validation/test deben compartir exactamente la misma logica de elegibilidad y versionado de reglas. Cualquier cambio de policy entre particiones invalida inferencia estadistica sobre generalizacion.

## Sobre sesgo: cuando una policy ayuda y cuando dana

Una policy bien disenada reduce sesgo estructural; una policy mal disenada puede introducir sesgo de seleccion, sesgo de regimen o leakage indirecto. El riesgo aparece cuando los filtros son tan agresivos que eliminan sistematicamente escenarios dificiles o cuando incorporan informacion que no estaba disponible ex-ante.

La mitigacion exige gobernanza formal: versionado de policy, monitoreo de cobertura perdida por ticker/sesion/anio/exchange, validacion walk-forward y reporte comparativo de resultados con variantes de policy. La pregunta correcta no es "si hay sesgo", sino "si esta medido, acotado y documentado".

## Plan minimo de implementacion

La implementacion recomendada comienza con `condition_policy_v1` conservadora y plenamente versionada. Luego se ejecuta el notebook 08 como capa de screening y se publica un reporte de cobertura y alertas. Despues se corre Gap&Go bajo variantes base/estricta/relajada para cuantificar sensibilidad real. Finalmente, solo se promueve a produccion aquello que mantiene estabilidad bajo endurecimiento razonable de policy.

Este enfoque conserva rigor cientifico sin bloquear operatividad: primero controla integridad, luego mide robustez, y solo despues interpreta performance como senal economica.

---

## Anexo complementario: policy aplicable (version operativa)

Esta seccion recupera y complementa el esquema operativo previo para dejar explicito que policy aplicaremos en practica.

### 1) Condition policy por uso

Aplicaremos una politica diferenciada por finalidad analitica. En `signal_price` se utilizaran unicamente trades regulares elegibles. En `bar_high_low` se excluiran condiciones tardias, fuera de secuencia y de precio promedio. En `execution_model` se usaran quotes junto con trades elegibles, excluyendo prints especiales que distorsionan simulacion de ejecucion.

* `signal_price`: usar solo trades elegibles evita disparos por prints no representativos del precio transable.
* `bar_high_low`: excluir condiciones tardias/fuera de secuencia reduce extremos artificiales en barras.
* `execution_model`: combinar quotes con trades elegibles mejora realismo de slippage y fill probability.


### 2) Session policy

Se trabajara por bloques intradia (`premarket`, `open 09:30-09:45`, `midday`, `close`). La apertura tendra umbrales mas estrictos porque concentra mayor ruido microestructural y mayor inestabilidad de spread.

* `open`: umbrales mas estrictos por mayor volatilidad microestructural y ensanchamiento de spread.
* `premarket`: exigir liquidez minima para evitar inferencias sobre mercado discontinuo.
* `midday/close`: controles estables para separar ruido de sesion de comportamiento estructural.


### 3) Filtros por spread y rango

Se excluiran minutos con `spread_bps` por encima del umbral definido por regimen y ticker. Tambien se excluiran barras con rango menor o igual a 1-2 ticks cuando se use logica de breakout, para evitar senales extremadamente sensibles al ruido de microticks.

* `spread_bps`: bloquear minutos fuera de umbral reduce se?ales sobre precios poco ejecutables.
* `range_ticks`: excluir barras ultra-estrechas evita sensibilidad extrema a 1-2 ticks.
* `regime-aware`: parametrizar por estado de mercado evita sesgo por umbral fijo unico.


### 4) Filtros de liquidez

Se impondran minimos de `quote_count`, `trade_count` y `dollar_volume` por minuto. Si no se cumplen, la observacion no sera elegible para senal o ejecucion, segun el caso de uso.

* `quote_count` y `trade_count`: garantizan densidad minima de informacion por ventana temporal.
* `dollar_volume`: evita operar sobre observaciones con profundidad economica insuficiente.
* `coverage_guard`: medir cobertura retenida evita sobre-filtrado y sesgo de seleccion.


### 5) Policy de timestamps

Se priorizara un esquema temporal coherente de evento/ejecucion, con normalizacion explicita de unidad y zona horaria. Se activaran flags de drift, duplicados y out-of-order para bloquear inferencias que puedan generar lookahead estructural.

* `time-unit` y `timezone`: normalizacion previa obligatoria para joins y comparaciones consistentes.
* `integrity checks`: drift, duplicados y out-of-order deben registrarse como controles de calidad.
* `anti-lookahead`: priorizar timestamp de evento reduce riesgo de se?al retrospectiva falsa.


### 6) Gating GO/WARN/FAIL

`GO` habilita uso normal, `WARN` habilita uso condicionado con filtros duros y control de exposicion, y `FAIL` bloquea uso en entrenamiento y simulacion de produccion. Esta clasificacion se aplicara por slice y quedara registrada con trazabilidad de umbrales.

* `GO`: habilita uso normal con trazabilidad completa.
* `WARN`: habilita uso condicionado con filtros mas duros y control de exposicion.
* `FAIL`: bloquea entrenamiento/simulacion productiva hasta resolver causa raiz.


### 7) Stress tests obligatorios

Toda estrategia se evaluara bajo tres configuraciones (`base`, `estricta`, `relajada`). Si el edge desaparece al endurecer controles razonables de calidad, la estrategia se clasificara como no robusta.

* `base/estricta/relajada`: cuantifica sensibilidad del edge a la policy de calidad.
* `metricas`: comparar PnL, drawdown, hit-rate y turnover entre escenarios.
* `criterio robustez`: si el edge desaparece en `estricta`, no promover a produccion.


### Sintesis operativa para implementacion inmediata

La policy inicial sera conservadora (`condition_policy_v1`), versionada y congelada por experimento. Se aplicara primero en la capa de trading algoritmico, y luego en la capa de ML con el mismo contrato de datos para evitar inconsistencias entre entrenamiento, validacion y test.
