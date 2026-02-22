---

# **INTEGRIDAD DE DATOS DE MERCADO EN RENTA VARIABLE USA**

## **SIP (CTA/UTP), Sale Conditions y su Impacto Cientifico en el Backtesting Sistematico**

**Documento de investigacion tecnica**
Nivel: Doctorado en Microestructura de Mercados
Ambito: US Equities - Datos consolidados (SIP)
Aplicacion: Backtesting cuantitativo, small caps, estrategias intradia

---

# Capitulo 1 - Introduccion: El Problema Epistemologico del OHLCV

En investigacion cuantitativa aplicada al trading existe un error estructural frecuente: asumir que las barras OHLCV son entidades estadisticas neutrales. No lo son.

Una barra OHLCV es el resultado de:

1. Reglas regulatorias.
2. Matrices de elegibilidad de actualizacion.
3. Estructura de reporte de mercado.
4. Normalizacion por parte de vendors.

Por tanto:

> Un backtest no es una simulacion sobre precios "puros", sino sobre precios filtrados por reglas microestructurales.

Si estas reglas no se entienden ni se controlan, el investigador incurre en un sesgo estructural invisible.

Este documento demuestra, con base normativa y academica, por que comprender el SIP y las trade conditions es imprescindible para investigacion cuantitativa rigurosa.

---

# Capitulo 2 - Arquitectura Regulatoria del Mercado: El SIP

## 2.1 Definicion formal

El **Securities Information Processor (SIP)** es el mecanismo centralizado que consolida:

* Trades
* Quotes
* NBBO (National Best Bid and Offer)

para todos los mercados de renta variable estadounidense bajo el marco de Regulation NMS.

Entidad supervisora:
Securities and Exchange Commission

Marco normativo:

* SEC Regulation NMS (2005)

---

## 2.2 Planes CTA y UTP

La consolidacion no es homogenea. Se divide en dos planes:

### CTA (Consolidated Tape Association)

Gestiona:

* Tape A (NYSE listings)
* Tape B (NYSE American y regionales)

### UTP (Unlisted Trading Privileges Plan)

Gestiona:

* Tape C (NASDAQ listings)

Cada plan publica:

* Especificaciones tecnicas de mensajes
* Codigos de condicion
* Matrices de actualizacion
* Reglas de timestamp

Estos documentos son publicos y normativos.

---

# Capitulo 3 - Sale Conditions: Ontologia del Trade

## 3.1 Naturaleza informacional

Cada trade transmitido via SIP incluye un campo denominado **Sale Condition**.

Este campo no es decorativo.

Representa metainformacion que responde a preguntas fundamentales:

* Es una transaccion regular?
* Es un trade tardio?
* Esta fuera de secuencia?
* Es precio promedio?
* Es horario extendido?
* Es odd lot?

Desde la teoria de microestructura (Harris, 2003; Hasbrouck, 2007):

> La informacion no es solo el precio, sino el contexto temporal y estructural del precio.

Por tanto, una transaccion sin condicion es epistemologicamente incompleta.

---

## 3.2 Matrices de update eligibility

Las especificaciones oficiales (CTA Pillar, UTDF) incluyen matrices formales que determinan si un trade actualiza:

* Open
* High
* Low
* Last
* Volume

Esto implica que:

High != maximo observado
High = maximo observado entre trades elegibles

Si existe un trade fuera de secuencia a 10.50 tras un maximo regular de 10.20:

* Estadisticamente el maximo observado es 10.50
* Microestructuralmente el high oficial puede permanecer en 10.20

Esto cambia:

* Breakouts
* Stops
* Indicadores tecnicos

---

# Capitulo 4 - Evidencia academica: Por que importa la secuencia

## 4.1 Lee & Ready (1991)

Lee & Ready demostraron que la clasificacion incorrecta de trades altera la inferencia sobre direccion de orden.

## 4.2 Ellis, Michaely & O'Hara (2000)

Mostraron que errores en clasificacion afectan significativamente metricas de microestructura.

Implicacion:

Si el orden importa para inferir direccion, tambien importa para inferir estructura de precios.

---

# Capitulo 5 - Vendors y normalizacion

Empresas como Polygon, Databento, Alpaca, QuantConnect y TradeStation no redistribuyen SIP en bruto.

Aplican:

* Normalizacion
* Agregacion propia
* Filtrado interno
* Mapeo unificado de condiciones

Por tanto:

Vendor OHLCV = funcion (SIP raw + policy vendor)

Sin conocer esa funcion, el investigador desconoce el proceso generador de datos.

---

# Capitulo 6 - Impacto directo en backtesting

## 6.1 Inflacion de breakouts

En small caps, un unico trade no elegible puede crear un high artificial.

Impacto:

* Aumento ficticio de triggers
* Mejora artificial del winrate
* Sesgo optimista en equity curve

## 6.2 Distorsion de VWAP

VWAP = sum(P*V) / sum(V)

Si volumen incluye trades no elegibles pero precio no actualiza last, aparece inconsistencia estructural.

## 6.3 Lookahead invisible

Trade reportado a 09:30:05 pero ejecutado a 09:29:59.

Si el motor usa timestamp de reporte, puede generar ruptura aparente posterior y senal falsa.

---

# Capitulo 7 - Drift historico

La estructura de mercado ha cambiado (pre-Reg NMS, fragmentacion post-2008, crecimiento TRF, reforma odd lots).

Por tanto:

Distribucion de conditions != estacionaria en 20 anios.

Un backtest 2005-2025 requiere analisis de estabilidad de condiciones y deteccion de cambios estructurales.

---

# Capitulo 8 - Comparar Polygon vs SIP raw

Comparacion directa solo necesaria para auditoria institucional, validacion de vendor o anomalias extremas.

Para investigacion avanzada retail/pro suele ser mas eficiente:

* Aceptar SIP via vendor
* Implementar policy propia
* Medir impacto

Descargar SIP raw es costoso y operacionalmente complejo.

---

# Capitulo 9 - Disenio cientifico de una condition policy

Principio fundamental:

> Toda investigacion cuantitativa debe declarar explicitamente su politica de elegibilidad.

Ejemplo conceptual:

condition_policy_v1:

* incluir: Regular Sale
* excluir: Late, Out-of-Sequence, Average Price

Esta politica debe versionarse, guardarse junto al backtest y medirse contra alternativas.

---

# Capitulo 10 - Aplicacion especifica: Gap & Go small caps

En microcaps hay baja profundidad, alta fragmentacion y prints erraticos.

Por tanto, hay sensibilidad extrema a conditions.

Recomendacion:

* Filtrado estricto
* Secuenciacion por execution time
* Validacion de extremos intraminuto

---

# Capitulo 11 - Arquitectura escalable

Para miles de tickers:

1. Screening diario de distribucion de conditions.
2. Flag de anomalias.
3. Reproceso profundo solo en casos criticos.
4. Versionado de policy.

Coste inicial medio/alto. Coste recurrente bajo. Beneficio en integridad alto.

---

# Capitulo 12 - Sintesis final

El conocimiento sobre SIP y conditions no sirve solo para comparar proveedores.

Sirve para:

* Entender el proceso generador de datos.
* Eliminar sesgos estructurales.
* Modelar microestructura real.
* Evitar alfa ilusorio.
* Elevar el nivel cientifico del backtesting.

Sin este control: se modelan artefactos.
Con este control: se modela mejor el mercado.

---

# Bibliografia academica

Hasbrouck, J. (2007). *Empirical Market Microstructure*. Oxford University Press.
Harris, L. (2003). *Trading and Exchanges*. Oxford University Press.
O'Hara, M. (1995). *Market Microstructure Theory*. Wiley.
Lee, C., & Ready, M. (1991). *Inferring Trade Direction from Intraday Data*. Journal of Finance.
Ellis, K., Michaely, R., & O'Hara, M. (2000). *The Accuracy of Trade Classification*. Journal of Financial and Quantitative Analysis.

Documentacion oficial:

* CTA Pillar Specifications
* UTP UTDF Specification
* SEC Regulation NMS
* Documentacion tecnica de Polygon
* Databento OHLCV Schema Notes

---

Si deseas, puedo ahora:

* Transformarlo en documento publicable estilo whitepaper academico formal
* Aniadir ecuaciones formales y pruebas estadisticas
* O diseniar el protocolo experimental para validar tu dataset de 20 anios

Tu decides el siguiente nivel.
