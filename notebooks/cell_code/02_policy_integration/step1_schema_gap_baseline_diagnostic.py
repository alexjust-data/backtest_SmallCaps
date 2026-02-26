# PASO 1 - DIAGNOSTICO BASE SCHEMA_GAP (CONGELADO)
from pathlib import Path
from datetime import datetime, timezone
import json
import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
RUNS_ROOT = PROJECT_ROOT / "runs"
BASELINE_ROOT = RUNS_ROOT / "baseline"
OUT_ROOT = RUNS_ROOT / "backtest" / "02_policy_integration" / "schema_gap_diagnostics"

# 1) Cargar baseline mas reciente (robusto)
baseline_dir = None
manifest = None

if "baseline_dir" in globals():
    bd = Path(str(globals()["baseline_dir"]))
    if bd.exists():
        baseline_dir = bd

if baseline_dir is None:
    candidates = sorted(BASELINE_ROOT.glob("before_remediation_*"), key=lambda p: p.stat().st_mtime)
    if candidates:
        baseline_dir = candidates[-1]

if baseline_dir is None:
    candidates = sorted(RUNS_ROOT.rglob("before_remediation_*"), key=lambda p: p.stat().st_mtime)
    if candidates:
        baseline_dir = candidates[-1]

if baseline_dir is None:
    raise FileNotFoundError(
        f"No hay baseline before_remediation_* en {BASELINE_ROOT} ni en busqueda recursiva de {RUNS_ROOT}."
    )

manifest_path = baseline_dir / "baseline_before_manifest.json"
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
else:
    if "manifest" in globals() and isinstance(globals()["manifest"], dict):
        manifest = globals()["manifest"]
    else:
        raise FileNotFoundError(f"No existe baseline_before_manifest.json en {baseline_dir}")

policy_path = Path(manifest["source"]["06_policy_table"])
if not policy_path.exists():
    raise FileNotFoundError(f"No existe 06_policy_table referenciado: {policy_path}")
policy = pl.read_parquet(policy_path)

# 2) Resolver columnas de decision/root cause con compatibilidad
if "decision_final" in policy.columns:
    decision_col = "decision_final"
elif "decision_policy" in policy.columns:
    decision_col = "decision_policy"
elif "decision" in policy.columns:
    decision_col = "decision"
else:
    raise ValueError(f"No se encontro columna de decision en policy: {policy.columns}")

if "root_cause_policy" in policy.columns:
    root_col = "root_cause_policy"
elif "root_cause_final" in policy.columns:
    root_col = "root_cause_final"
elif "root_cause" in policy.columns:
    root_col = "root_cause"
else:
    root_col = None

work = policy.with_columns(pl.col(decision_col).cast(pl.Utf8).alias("decision_norm"))
if root_col is None:
    work = work.with_columns(pl.lit("unknown").alias("root_cause_norm"))
else:
    work = work.with_columns(
        pl.col(root_col).cast(pl.Utf8).fill_null("unknown").alias("root_cause_norm")
    )

# 3) Filtrar NOT_COMPARABLE y clasificar causas schema-related
not_comp = work.filter(pl.col("decision_norm") == "NOT_COMPARABLE")

schema_like = not_comp.filter(
    pl.col("root_cause_norm").str.to_lowercase().str.contains("schema|scale_mismatch|column|dtype|type")
)

not_comp_by_cause = (
    not_comp.group_by("root_cause_norm")
    .agg(pl.len().alias("n_tickers"))
    .with_columns((pl.col("n_tickers") / pl.lit(max(not_comp.height, 1))).alias("ratio_within_not_comparable"))
    .sort("n_tickers", descending=True)
)

schema_share = {
    "not_comparable_total": int(not_comp.height),
    "schema_like_total": int(schema_like.height),
    "schema_like_share_within_not_comparable": float(schema_like.height / max(not_comp.height, 1)),
}

# 4) Guardar artifacts del paso 1
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = OUT_ROOT / f"step1_schema_gap_baseline_{stamp}"
out_dir.mkdir(parents=True, exist_ok=True)

not_comp_by_cause.write_parquet(out_dir / "not_comparable_by_root_cause.parquet")
schema_like.select([c for c in ["ticker", "decision_norm", "root_cause_norm"] if c in schema_like.columns]).write_parquet(
    out_dir / "schema_like_tickers.parquet"
)

summary = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "baseline_dir": str(baseline_dir),
    "policy_source": str(policy_path),
    "decision_col_used": decision_col,
    "root_col_used": root_col,
    "metrics": schema_share,
    "output_dir": str(out_dir),
}
(out_dir / "step1_schema_gap_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

# 5) Imprimir salida congelable
print("=== STEP 1 - SCHEMA_GAP BASELINE DIAGNOSTIC ===")
print("baseline:", baseline_dir)
print("policy source:", policy_path)
print("decision_col:", decision_col)
print("root_col:", root_col)
print("output_dir:", out_dir)
print("\n[Schema-like share dentro de NOT_COMPARABLE]")
print(schema_share)
print("\n[Top root causes en NOT_COMPARABLE]")
print(not_comp_by_cause.head(25))
