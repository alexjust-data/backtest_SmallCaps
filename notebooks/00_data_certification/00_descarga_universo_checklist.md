# 00 Descarga Universo - Checklist de Certificación (End-to-End)

Este documento está diseñado para ejecutarse como guía de notebook: cada bloque tiene:
- una **celda markdown** (qué valida y por qué)
- una **celda código** (cómo demostrarlo)

Objetivo: saber exactamente qué data se descargó, su cobertura, su integridad y si se puede continuar.

---

## Celda Markdown
## 1) Verificar artefactos base del Universo PTI
Validamos existencia de outputs principales del universo PTI y metadatos de corrida.

## Celda Código
```python
from pathlib import Path

outdir = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti")
required = [
    "build_universe_pti.progress.json",
    "build_universe_pti.meta.json",
    "qa_coverage_by_cut.csv",
    "tickers_all.parquet",
    "tickers_active.parquet",
    "tickers_inactive.parquet",
]
for x in required:
    p = outdir / x
    print(x, "->", p.exists())
```

---

## Celda Markdown
## 2) Verificar cierre de corrida PTI
La corrida debe estar cerrada (`status=completed`) y coherente con meta/QA/panel.

## Celda Código
```python
import json
import pandas as pd
import pyarrow.dataset as ds
from pathlib import Path

outdir = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti")
progress = json.loads((outdir / "build_universe_pti.progress.json").read_text(encoding="utf-8"))
meta = json.loads((outdir / "build_universe_pti.meta.json").read_text(encoding="utf-8"))
qa = pd.read_csv(outdir / "qa_coverage_by_cut.csv")

panel = ds.dataset(str(outdir / "tickers_panel_pti"), format="parquet").to_table(columns=["snapshot_date","entity_id"]).to_pandas()
panel["snapshot_date"] = pd.to_datetime(panel["snapshot_date"], errors="coerce")

print("progress.status:", progress.get("status"))
print("progress snapshot:", progress.get("snapshot_date"), progress.get("snapshot_index"), "/", progress.get("snapshot_total"))
print("meta successful/requested:", meta.get("successful_cuts"), "/", meta.get("requested_cuts"))
print("panel rows:", len(panel), "snapshots:", panel["snapshot_date"].nunique(), "max:", panel["snapshot_date"].max())
print("qa rows:", len(qa), "qa max:", pd.to_datetime(qa["snapshot_date"], errors="coerce").max())
```

---

## Celda Markdown
## 3) Auditor formal del universo PTI (go/no-go)
Ejecuta el agente auditor para certificar coherencia final de la corrida.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\scripts\agent_universe_audit.py --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti --stale-minutes 10 --validate-latest-checkpoints 50 --out-report-json C:\TSIS_Data\v1\backtest_SmallCaps\runs\data_quality\00_data_certification\universe_audit_final.json
```

---

## Celda Markdown
## 4) Construir y validar universo filtrado listed/deslisted (2005-2026)
Debe usar intersección temporal `[first_seen_date,last_seen_date]` con ventana objetivo.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_tickers_2005_2026.py

import pandas as pd
p = r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet"
d = pd.read_parquet(p)
print("rows:", len(d))
print(d[["ticker","first_seen_date","last_seen_date","status","primary_exchange"]].head(10).to_string(index=False))
```

---

## Celda Markdown
## 5) Ejecutar enriquecimiento híbrido (as-of + catálogo inactivos)
Descarga atributos de referencia/fundamentales por ticker y aplica merge por prioridad de fuentes.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_universe_hybrid_enriched.py --input C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched --batch-size 200 --inactive-catalog-refresh --resume
```

---

## Celda Markdown
## 6) Validar cobertura de campos enriquecidos
Debemos medir cobertura por `final_*` y distribución por fuente (`source_final_*`).

## Celda Código
```python
import pandas as pd
from pathlib import Path

p = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet")
d = pd.read_parquet(p)

final_cols = [c for c in d.columns if c.startswith("final_")]
cov = pd.DataFrame({
    "field": final_cols,
    "non_null": [int(d[c].notna().sum()) for c in final_cols]
})
cov["rows"] = len(d)
cov["coverage_pct"] = (cov["non_null"] / cov["rows"] * 100).round(2)

display(cov.sort_values("coverage_pct", ascending=False).reset_index(drop=True))
print("source_final_delisted_utc:")
print(d["source_final_delisted_utc"].value_counts(dropna=False).to_string())
```

---

## Celda Markdown
## 7) Validar semántica de deslistado (oficial vs inferido)
No tratar como equivalente `inactive_catalog` (oficial) y `inferred_last_seen` (inferencia).

## Celda Código
```python
import pandas as pd

p = r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet"
d = pd.read_parquet(p)

m = d[d["source_final_delisted_utc"].eq("missing")]
print("missing delisted rows:", len(m))
print("missing by status:")
print(m["status"].value_counts(dropna=False).to_string())
print("missing by exchange:")
print(m["primary_exchange"].value_counts(dropna=False).to_string())
```

---

## Celda Markdown
## 8) Preparar universo canónico para fundamentals (UPPER + dedup)
Evita colisiones por case en filesystem Windows (`ADSw` vs `ADSW`).

## Celda Código
```python
import pandas as pd

src = r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet"
out = r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet"

u = pd.read_parquet(src)
u["ticker"] = u["ticker"].astype("string").str.strip().str.upper()
u = u.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"]).sort_values("ticker")
u.to_parquet(out, index=False)
print("rows:", len(u))
print("saved:", out)
```

---

## Celda Markdown
## 9) Descargar fundamentals por endpoint (en D:\financial)
Descarga por ticker para 4 datasets: income, balance, cashflow, ratios.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\download_fundamentals_v1.py --input C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet --outdir D:\financial --datasets income_statements,balance_sheets,cash_flow_statements,ratios --batch-size 200 --workers 12 --limit 1000 --max-pages 50 --resume --resume-validate --progress-every 25
```

---

## Celda Markdown
## 10) Monitor de corrida fundamentals
Verificar progreso y errores/warnings.

## Celda Código
```python
# En terminal (loop monitor):
# while ($true) { if (Test-Path D:\financial\_run\download_fundamentals_v1.progress.json) { Get-Content D:\financial\_run\download_fundamentals_v1.progress.json -Raw } else { "waiting progress file..." }; Start-Sleep -Seconds 30 }
```

---

## Celda Markdown
## 11) Auditar fundamentals de extremo a extremo
Cobertura por endpoint, integridad file-by-file, y rango temporal por ticker+endpoint.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\01_data_fundamentals\cell_code\audit_fundamentals_download.py --input-universe C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet --outdir D:\financial --datasets income_statements,balance_sheets,cash_flow_statements,ratios --limit 1000 --max-pages 50
```

---

## Celda Markdown
## 12) Sonda de filtro server-side (5 tickers)
Prueba diagnóstica: detecta si el endpoint devuelve masivamente tickers distintos al solicitado.

## Celda Código
```python
# Ejecutar en terminal:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\01_data_fundamentals\cell_code\probe_fundamentals_ticker_filter.py --tickers AAPL,MSFT,NVDA,JNJ,WMT --datasets income_statements,balance_sheets,cash_flow_statements,ratios --outdir D:\financial\_audit --limit 100 --max-pages 3
```

---

## Celda Markdown
## 13) Evidencia final mínima para certificar (GO)
Para continuar, deben cumplirse simultáneamente:
1. Universo PTI: auditor final `overall=OK`.
2. Fundamentals: cobertura por endpoint = 100% sobre `tickers_2005_2026_upper.parquet`.
3. `file_level_audit`: sin issues severas (`read_error`, `ticker_col_mismatch`, `multi_cik`, `rows_hit_page_cap`, `tickers_field_mismatch`).
4. `date_ranges_by_ticker_endpoint.csv` completo (con `date_start/date_end` cuando hay datos).
5. Separación explícita entre dato oficial e inferido (`source_final_delisted_utc`).

## Celda Código
```python
import json
import pandas as pd
from pathlib import Path

audit = Path(r"D:\financial\_audit")
summary = json.loads((audit / "audit_summary.json").read_text(encoding="utf-8"))
cov = pd.read_csv(audit / "coverage_by_endpoint.csv")
sev = pd.read_csv(audit / "severe_issues.csv") if (audit / "severe_issues.csv").exists() else pd.DataFrame()

print("status:", summary.get("status"))
print("coverage:")
print(cov.to_string(index=False))
print("severe_issues:", len(sev))
```

---

## Celda Markdown
## 14) Próximo paso (Small-Cap PTI)
Solo si el punto 13 está en GO:
- construir `population_target_pti(date, figi, ticker, exchange, status, close_t, shares_outstanding_t, market_cap_t, is_small_cap_t)`
- con LOCF <= 180 días y sin look-ahead.

## Celda Código
```python
# Placeholder de launcher futuro:
# python C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\build_population_target_pti.py --universe C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet --fundamentals-root D:\financial --outdir C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\population_target_pti --locf-days 180 --resume
```
