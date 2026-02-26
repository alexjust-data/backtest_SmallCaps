from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
BASE = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "schema_gap_diagnostics"

# Fuente principal: salida del deep audit ya calculado
cand = sorted(
    BASE.glob("step12_deep_audit_3629_*/step12_excluded_3629_diagnostics.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not cand:
    raise FileNotFoundError("No existe step12_excluded_3629_diagnostics.parquet (ejecuta Paso 12)")

audit_fp = cand[-1]
df = pl.read_parquet(audit_fp)

# Clasificación simple y explícita
classified = df.with_columns([
    pl.when(pl.col("has_overlap_any_2019_2025") == True)
    .then(pl.lit("recoverable"))
    .otherwise(pl.lit("no_overlap_real"))
    .alias("recovery_class")
])

summary = (
    classified.group_by("recovery_class")
    .agg(pl.len().alias("n_tickers"))
    .sort("n_tickers", descending=True)
)

recoverable = (
    classified.filter(pl.col("recovery_class") == "recoverable")
    .select([
        "ticker",
        "reason_prefilter",
        "overlap_months_n_2019_2025",
        "q_first",
        "q_last",
        "o_first",
        "year_gap_o_first_minus_q_last",
    ])
    .sort("ticker")
)

no_overlap = (
    classified.filter(pl.col("recovery_class") == "no_overlap_real")
    .select([
        "ticker",
        "reason_prefilter",
        "q_first",
        "q_last",
        "o_first",
        "year_gap_o_first_minus_q_last",
    ])
    .sort("ticker")
)

out_dir = BASE / f"step14_overlap_check_2004_2025_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
out_dir.mkdir(parents=True, exist_ok=True)

summary_fp = out_dir / "step14_overlap_summary.parquet"
recoverable_fp = out_dir / "step14_recoverable_tickers.parquet"
no_overlap_fp = out_dir / "step14_no_overlap_real_tickers.parquet"
manifest_fp = out_dir / "step14_overlap_manifest.json"

summary.write_parquet(summary_fp)
recoverable.write_parquet(recoverable_fp)
no_overlap.write_parquet(no_overlap_fp)

manifest = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_step12": str(audit_fp),
    "n_input": int(classified.height),
    "summary": summary.to_dicts(),
    "outputs": {
        "summary": str(summary_fp),
        "recoverable": str(recoverable_fp),
        "no_overlap_real": str(no_overlap_fp),
    },
}
manifest_fp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 14 - OVERLAP CHECK 2004-2025 ===")
print("source:", audit_fp)
print("output_dir:", out_dir)
print("[summary]")
print(summary)
print("recoverable_n:", recoverable.height)
print("no_overlap_real_n:", no_overlap.height)
print("Saved:")
print(summary_fp)
print(recoverable_fp)
print(no_overlap_fp)
print(manifest_fp)