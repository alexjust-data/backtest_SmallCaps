

* **el notebook documenta**
* **el código vive en `src/`**
* **los resultados quedan ligados a un snapshot + commit**

### Los notebooks sirven para:

✔️ Diario técnico (qué se hizo, por qué, con qué datos)
✔️ Exploración inicial de datos
✔️ Validaciones visuales y estadísticas
✔️ Decisiones documentadas (umbral X elegido por motivo Y)
✔️ Evidencia de QA (gráficos, tablas, ejemplos)

### Los notebooks NO sirven para:

❌ Implementar lógica de producción
❌ Ejecutar backtests “finales”
❌ Ser la única copia de un cálculo crítico

Regla de oro:

> **Si algo afecta a resultados → vive en `src/`**
> **Si algo explica, justifica o explora → vive en `notebooks/`**

---

## Estructura recomendada de `notebooks/` (diario trazable)

```text
notebooks/
├─ 00_index.ipynb
├─ 01_data_integrity/
│  ├─ 01_snapshot_inventory.ipynb
│  ├─ 02_schema_validation.ipynb
│  ├─ 03_time_coverage.ipynb
│  ├─ 04_ohlcv_vs_quotes.ipynb
│  └─ 05_outliers_and_anomalies.ipynb
├─ 02_decisions/
│  ├─ 01_calendar_definition.ipynb
│  ├─ 02_session_rules.ipynb
│  ├─ 03_fill_model_assumptions.ipynb
│  └─ 04_cost_model_assumptions.ipynb
├─ 03_research/
│  ├─ relvol_distribution.ipynb
│  └─ spread_by_market_cap.ipynb
└─ _templates/
   └─ research_template.ipynb
```

---

## Notebook = documento científico, no “script”

Cada notebook debería empezar **SIEMPRE** con una celda tipo:

```markdown
# Data Integrity – Schema Validation (OHLCV 1m)

**Date:** 2026-02-02  
**Author:** Alex  
**Purpose:** Validate schema consistency of minute OHLCV data  
**Dataset snapshot:** r2_snapshot_2026-02-02.json  
**Git commit:** <auto-filled>  
**Status:** DRAFT / FINAL
```

Y una celda automática:

```python
from src.core.env import get_run_context

get_run_context()
```

Que imprima:

* git commit
* python version
* snapshot id
* timezone
* calendario

Esto es **trazabilidad real**.

---

## Flujo correcto notebook ↔ código

### 1️⃣ Exploración en notebook

Ejemplo:

* descubres que en `quotes_p95` hay `bid > ask` en el 0.003% de filas
* analizas cuándo pasa
* decides que:

  * `<0.01%` → WARN
  * `>=0.01%` → FAIL

📓 Esto queda documentado en el notebook.

---

### 2️⃣ Formalización en código

Creas/modificas:

```text
src/data/validation/quotes_checks.py
```

Con:

```python
MAX_CROSSED_MARKET_RATIO = 1e-4
```

---

### 3️⃣ Referencia cruzada

En el notebook:

```markdown
This threshold is implemented in:
`src/data/validation/quotes_checks.py:L87`
```

Y en el código:

```python
# Threshold justified in notebook:
# notebooks/01_data_integrity/04_ohlcv_vs_quotes.ipynb
```

Esto es **audit-grade documentation**.

---

## Cómo versionar notebooks correctamente

### Reglas obligatorias

1. Notebooks **versionados en git**
2. Outputs grandes (dataframes enormes) **NO guardados**
3. Ejecutables de arriba a abajo
4. Sin paths hardcoded
5. Solo importan desde `src/`, nunca código inline crítico

### Extra pro (muy recomendable)

* `nbstripout` o similar para limpiar outputs automáticamente
* Tags de celda: `parameters`, `analysis`, `decision`

---

## Relación notebooks ↔ runs

Un backtest o validación debe poder decir:

> “Las decisiones usadas aquí están justificadas en estos notebooks”

Ejemplo en `runs/.../metadata.json`:

```json
{
  "decision_docs": [
    "notebooks/01_data_integrity/02_schema_validation.ipynb",
    "notebooks/01_data_integrity/04_ohlcv_vs_quotes.ipynb"
  ]
}
```

Eso es **trazabilidad total**.

---

## Conclusión clara

✔️ **Sí**, `notebooks/` es el sitio correcto
✔️ **Sí**, pueden ser tu diario técnico
✔️ **No**, no son fuente de verdad
✔️ **Sí**, bien hechos elevan el proyecto a nivel hedge fund real
