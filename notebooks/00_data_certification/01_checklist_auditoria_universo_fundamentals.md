# Checklist Canónico: Universo + Fundamentals (Evidencia Reproducible)

Objetivo: validar de principio a fin qué datos se descargaron, con evidencia reproducible y criterios de aceptación.

Convención: cada bloque incluye:
- Texto tipo celda markdown (qué valida).
- Código tipo celda (cómo demostrarlo).

---

## 0) Contexto y rutas oficiales

### Celda markdown (texto)
Definir rutas únicas de trabajo para evitar mezclar corridas históricas.

### Celda código
```python
from pathlib import Path

PATHS = {
    "universe_upper": Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet"),
    "universe_all": Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet"),
    "lifecycle_official": Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.multisource.parquet"),
    "financial_root": Path(r"D:\financial"),
    "download_progress": Path(r"D:\financial\_run\download_fundamentals_v1.progress.json"),
    "download_errors": Path(r"D:\financial\_run\download_fundamentals_v1.errors.csv"),
    "audit_root": Path(r"C:\Users\AlexJ\financial_audit"),
}

for k, v in PATHS.items():
    print(f"{k:20s} -> {v} | exists={v.exists()}")
```

---

## 1) Universo base usado para descargar fundamentals

### Celda markdown (texto)
Validar que el input de fundamentals es el universo normalizado (`upper`) y su cardinalidad.

### Celda código
```python
import pandas as pd

u = pd.read_parquet(PATHS["universe_upper"], columns=["ticker"])
t = u["ticker"].astype("string").str.strip().dropna()

print("rows:", len(u))
print("unique_tickers:", t.nunique())
print("sample:", t.drop_duplicates().sort_values().head(20).tolist())
```

---

## 2) Estado final de la corrida de descarga fundamentals

### Celda markdown (texto)
Confirmar que la corrida terminó (`status=completed`) y con qué parámetros.

### Celda código
```python
import json

progress = json.loads(PATHS["download_progress"].read_text(encoding="utf-8"))
print(json.dumps(progress, indent=2, ensure_ascii=False))
```

---

## 3) Cobertura de archivos descargados por endpoint

### Celda markdown (texto)
Demostrar que cada endpoint tiene cobertura completa vs universo esperado.

### Celda código
```python
import pandas as pd

expected = set(
    pd.read_parquet(PATHS["universe_upper"], columns=["ticker"])["ticker"]
    .astype("string").str.strip().dropna().str.upper().drop_duplicates().tolist()
)

for ds in ["income_statements", "balance_sheets", "cash_flow_statements", "ratios"]:
    got = set(p.name.replace("ticker=", "").upper() for p in (PATHS["financial_root"] / ds).glob("ticker=*"))
    print(f"\n{ds}")
    print(" expected_tickers:", len(expected))
    print(" downloaded_tickers:", len(got))
    print(" missing_tickers:", len(expected - got))
    print(" extra_tickers:", len(got - expected))
```

---

## 4) Auditoría estructural consolidada (file_level_audit.csv)

### Celda markdown (texto)
Leer salida del agente auditor y revisar integridad técnica de todos los archivos.

### Celda código
```python
import pandas as pd

f = PATHS["audit_root"] / "file_level_audit.csv"
d = pd.read_csv(f)

print("rows:", len(d))
print("read_ok:", d["read_ok"].value_counts(dropna=False).to_dict())
print("ticker_col_ok:", d["ticker_col_ok"].value_counts(dropna=False).to_dict())
print("dataset_col_ok:", d["dataset_col_ok"].value_counts(dropna=False).to_dict())
print("ingested_utc_parseable:", d["ingested_utc_parseable"].value_counts(dropna=False).to_dict())
print("rows_total_min/max:", d["rows_total"].min(), d["rows_total"].max())
print("rows_business_zero:", int((d["rows_business"].fillna(0) == 0).sum()))
print("tickers_col_mismatch_rows_gt0:", int((d["tickers_col_mismatch_rows"].fillna(0) > 0).sum()))
print("cik_nunique_dist:", d["cik_nunique"].fillna(0).value_counts(dropna=False).sort_index().to_dict())
print("missing_required_cols:", d["missing_required_cols"].fillna("").value_counts(dropna=False).to_dict())
print("rows_hit_page_cap:", int(d["issues"].astype(str).str.contains("rows_hit_page_cap", na=False).sum()))
```

---

## 5) Integridad por endpoint (explícito, no agregado)

### Celda markdown (texto)
Desglosar resultados por endpoint para evitar mezclar métricas globales.

### Celda código
```python
for ds, g in d.groupby("dataset", dropna=False):
    print(f"\n=== {ds} ===")
    print("files:", len(g))
    print("read_ok:", g["read_ok"].value_counts(dropna=False).to_dict())
    print("ticker_col_ok:", g["ticker_col_ok"].value_counts(dropna=False).to_dict())
    print("dataset_col_ok:", g["dataset_col_ok"].value_counts(dropna=False).to_dict())
    print("ingested_utc_parseable:", g["ingested_utc_parseable"].value_counts(dropna=False).to_dict())
    print("rows_business_zero:", int((g["rows_business"].fillna(0) == 0).sum()))
    print("tickers_col_mismatch_rows_gt0:", int((g["tickers_col_mismatch_rows"].fillna(0) > 0).sum()))
    print("cik_nunique:", g["cik_nunique"].fillna(0).value_counts(dropna=False).sort_index().to_dict())
    print("missing_required_cols:", g["missing_required_cols"].fillna("").value_counts(dropna=False).to_dict())
```

---

## 6) Rango temporal por archivo (endpoint + ticker)

### Celda markdown (texto)
Probar que tenemos `date_start` y `date_end` por ticker y endpoint.

### Celda código
```python
r = pd.read_csv(PATHS["audit_root"] / "date_ranges_by_ticker_endpoint.csv")
for c in ["date_start", "date_end"]:
    r[c] = pd.to_datetime(r[c], errors="coerce", utc=True)

print(r[["dataset", "ticker", "date_col_used", "date_start", "date_end"]].head(25).to_string(index=False))

for ds, g in r.groupby("dataset", dropna=False):
    nonnull = g[g["date_start"].notna() & g["date_end"].notna()]
    print(f"\n{ds}: with_date_range={len(nonnull)} without_date_range={len(g)-len(nonnull)}")
    if len(nonnull):
        print(" date_start_min/max:", nonnull["date_start"].min(), nonnull["date_start"].max())
        print(" date_end_min/max:", nonnull["date_end"].min(), nonnull["date_end"].max())
```

---

## 7) Validación temporal global por ticker (fundamentals vs lifecycle/PTI)

### Celda markdown (texto)
Verificar coherencia temporal:
- `fin_min_date`, `fin_max_date`.
- ventana de referencia (prioridad lifecycle oficial, fallback PTI).
- estado final por ticker (`OK`, `NO_DATA`, `ANOMALY_PRE_START`, `ANOMALY_POST_END`, `UNKNOWN_WINDOW`).

### Celda código
```python
t = pd.read_csv(PATHS["audit_root"] / "temporal_validation_by_ticker.csv")
print("temporal_status_counts:")
print(t["temporal_status"].value_counts(dropna=False).to_string())

issues = pd.read_csv(PATHS["audit_root"] / "temporal_issues.csv")
print("\ntemporal_issues_rows:", len(issues))
print(issues.head(20).to_string(index=False))
```

---

## 8) Muestra demostrativa por endpoint (archivo real)

### Celda markdown (texto)
Demostrar estructura real de un archivo descargado y las columnas críticas para merges posteriores.

### Celda código
```python
p_income = Path(r"D:\financial\income_statements\ticker=A\income_statements_A.parquet")
df = pd.read_parquet(p_income)

print("path:", p_income)
print("columns:", df.columns.tolist())

id_cols = [c for c in ["ticker", "cik", "tickers"] if c in df.columns]
date_cols = [c for c in ["period_end", "filing_date", "fiscal_year", "fiscal_quarter", "_ingested_utc"] if c in df.columns]
print("\nhead(5) ids+fechas:")
print(df[id_cols + date_cols].head(5).to_string(index=False))
```

---

## 9) Criterios de aceptación para continuar

### Celda markdown (texto)
Se puede continuar si:
1. Cobertura por endpoint: missing=0 y extra=0.
2. Integridad técnica: `read_ok=True`, `ticker_col_ok=True`, `dataset_col_ok=True`, `ingested_utc_parseable=True`.
3. No truncado (`rows_hit_page_cap=0`).
4. Anomalías temporales revisadas y clasificadas (aceptadas o en cola de remediación).
5. Riesgos explícitos documentados:
   - `rows_business_zero`.
   - `cik_nunique > 1`.
   - `NO_DATA`.

### Celda código
```python
summary = json.loads((PATHS["audit_root"] / "audit_summary.json").read_text(encoding="utf-8"))
print(json.dumps(summary, indent=2, ensure_ascii=False))
```

---

## 10) Evidencia final mínima a archivar

### Celda markdown (texto)
Archivos obligatorios de evidencia:
- `C:\Users\AlexJ\financial_audit\audit_summary.json`
- `C:\Users\AlexJ\financial_audit\coverage_by_endpoint.csv`
- `C:\Users\AlexJ\financial_audit\file_level_audit.csv`
- `C:\Users\AlexJ\financial_audit\date_ranges_by_ticker_endpoint.csv`
- `C:\Users\AlexJ\financial_audit\temporal_validation_by_ticker.csv`
- `C:\Users\AlexJ\financial_audit\temporal_issues.csv`

Si estos 6 existen y son consistentes con criterios de aceptación, la descarga queda certificada para la siguiente fase.

