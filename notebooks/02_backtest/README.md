# README - 02_backtest

Este documento define **como** vamos a estructurar la fase de backtesting en notebooks, despues de data integrity, para trabajar con rigor cuantitativo y trazabilidad completa.


## Flujo de trabajo (orden obligatorio)
1. `01_dataset_builder.ipynb`
2. `02_strategy_definition.ipynb`
3. `03_event_backtest_engine.ipynb`
4. `04_portfolio_and_risk.ipynb`
5. `05_performance_report.ipynb`
6. `06_sensitivity_and_stress.ipynb`

Pipeline conceptual:

- `dataset -> signal -> execution -> portfolio -> metrics -> stress`

## Decisiones metodologicas clave
**1) Separacion por capas**  
Cada notebook responde a una pregunta distinta:
- datos operables,
- reglas de estrategia,
- simulacion de ejecucion,
- agregacion y riesgo,
- performance,
- robustez.

No se mezcla logica de ejecucion con logica de evaluacion.

**2) Entrada controlada por QA**  
El universo de backtest debe venir etiquetado desde data quality:
- `GO`
- `WARN`
- `FAIL`
- `NOT_COMPARABLE`

Politica inicial:
- `GO`: entra normal.
- `WARN`: entra con etiqueta de riesgo y reporte separado.
- `FAIL` y `NOT_COMPARABLE`: fuera del universo operativo.

**3) Doble vista siempre**  
Todo resultado relevante se reporta en:
- `full_set`
- `comparable_set`

Objetivo: evitar conclusiones sesgadas por mezcla de activos no comparables.

**4) Trazabilidad por corrida**  
Cada run debe dejar artifacts en `runs/` con:
- configuracion,
- universo,
- decisiones de gating,
- resultados intermedios,
- resultados finales.

Sin artifacts versionados no hay reproducibilidad real.

## Criterios de calidad para avanzar de etapa
- No avanzar si la etapa anterior no es reproducible.
- No avanzar si no hay trazabilidad de parametros y universo.
- No avanzar si no existe explicacion causal de resultados (no solo metricas agregadas).

## Convenciones de salida minima por notebook
**Dataset builder**  
- universo final,
- filtros aplicados,
- cobertura retenida,
- exclusiones con causa.

**Strategy definition**  
- reglas formales,
- parametros,
- supuestos de no-leakage.

**Event engine**  
- fills,
- costos,
- slippage,
- logs de orden/ejecucion.

**Portfolio & risk**  
- posicion,
- exposicion,
- limites,
- eventos de riesgo.

**Performance report**  
- retorno,
- drawdown,
- hit-rate,
- turnover,
- estabilidad temporal.

**Sensitivity & stress**  
- variacion por parametros,
- variacion por costos,
- variacion por universo,
- robustez de conclusiones.

## Regla de oro
No se acepta un resultado de backtest como valido si no puede explicarse con:
1. datos de entrada trazables,
2. reglas de decision explicitas,
3. evidencia de robustez.


## 01_dataset_builder.ipynb
`01_dataset_builder.ipynb` debería construir el **dataset operable** para backtest, no señales ni performance.

Contenido recomendado:

1. Objetivo y contrato de salida
- Definir universo, periodo, sesión, columnas y granularidad final.

2. Carga de universos y gates QA
- Leer decisiones de `01_data_integrity`.
- Incluir `GO` y `WARN` (opcional), excluir `FAIL/NOT_COMPARABLE`.

3. Definición temporal
- Calendario, timezone, sesiones, minutos válidos, handling de half-days.

4. Ingesta de fuentes
- OHLCV, quotes, trades (si aplica) con schemas canónicos.

5. Normalización y alineación
- Tipos, unidades de tiempo, llaves (`ticker`, `minute_ny`), deduplicación.

6. Reglas de elegibilidad de muestra
- Cobertura mínima, filtros de truncado, comparabilidad de escala, filtros básicos de microestructura.

7. Construcción de dataset final
- Tabla principal por `ticker-minute` con features base (sin alpha complejo todavía).

8. Validaciones de integridad de salida
- Nulls, gaps, cobertura retenida, checks de consistencia por ticker.

9. Artifacts y metadata
- Guardar `dataset.parquet`, `dataset_manifest.json`, `exclusions.parquet`, `build_report.json`.

10. Resumen didáctico
- Qué se excluyó, por qué, qué cobertura quedó y riesgos residuales.

Si quieres, te paso una plantilla de celdas (sección por sección) para que ese notebook quede listo para ejecutar.
