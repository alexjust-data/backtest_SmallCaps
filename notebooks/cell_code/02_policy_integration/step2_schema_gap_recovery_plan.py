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
    baseline_dirs = sorted(BASELINE_ROOT.glob("before_remediation_*"), key=lambda p: p.stat().st_mtime)
    if baseline_dirs:
        baseline_dir = baseline_dirs[-1]

if baseline_dir is None:
    baseline_dirs = sorted(RUNS_ROOT.rglob("before_remediation_*"), key=lambda p: p.stat().st_mtime)
    if baseline_dirs:
        baseline_dir = baseline_dirs[-1]

if baseline_dir is None:
    raise FileNotFoundError(
        f"No hay baseline before_remediation_* en {BASELINE_ROOT} ni en b?squeda recursiva de {RUNS_ROOT}."
    )

manifest_path_in = baseline_dir / "baseline_before_manifest.json"
if manifest_path_in.exists():
    manifest = json.loads(manifest_path_in.read_text(encoding="utf-8"))
else:
    if "manifest" in globals() and isinstance(globals()["manifest"], dict):
        manifest = globals()["manifest"]
    else:
        raise FileNotFoundError(f"No existe baseline_before_manifest.json en {baseline_dir}")

policy_path = Path(manifest["source"]["06_policy_table"])
if not policy_path.exists():
    raise FileNotFoundError(f"No existe 06_policy_table referenciado: {policy_path}")
policy = pl.read_parquet(policy_path)

# 2) Resolver columnas de decision y root cause
if "decision_final" in policy.columns:
    decision_col = "decision_final"
elif "decision_policy" in policy.columns:
    decision_col = "decision_policy"
elif "decision" in policy.columns:
    decision_col = "decision"
else:
    raise ValueError("No se encontro columna de decision en policy")

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
    work = work.with_columns(pl.col(root_col).cast(pl.Utf8).fill_null("unknown").alias("root_cause_norm"))

if "ticker" not in work.columns:
    raise ValueError("No existe columna ticker en policy table")

# 3) Construir universo objetivo (NOT_COMPARABLE)
target = work.filter(pl.col("decision_norm") == "NOT_COMPARABLE")

# columnas proxy de severidad (si existen)
coverage_col = "coverage_global" if "coverage_global" in target.columns else None
overlap_col = "p_overlap" if "p_overlap" in target.columns else None
pin_col = "p_in_range" if "p_in_range" in target.columns else None

exprs = [
    pl.col("ticker"),
    pl.col("decision_norm").alias("decision_final"),
    pl.col("root_cause_norm").alias("root_cause"),
]
if coverage_col:
    exprs.append(pl.col(coverage_col).cast(pl.Float64).alias("coverage_global"))
if overlap_col:
    exprs.append(pl.col(overlap_col).cast(pl.Float64).alias("p_overlap"))
if pin_col:
    exprs.append(pl.col(pin_col).cast(pl.Float64).alias("p_in_range"))

plan = target.select(exprs)

# 4) Clasificar carril de recuperaci?n
plan = plan.with_columns(
    pl.when(pl.col("root_cause").str.to_lowercase().str.contains("schema_gap|schema|column|dtype|type"))
      .then(pl.lit("TRACK_A_SCHEMA_NORMALIZATION"))
      .when(pl.col("root_cause").str.to_lowercase().str.contains("scale_mismatch|scale"))
      .then(pl.lit("TRACK_B_SCALE_NORMALIZATION"))
      .otherwise(pl.lit("TRACK_C_MANUAL_REVIEW"))
      .alias("recovery_track")
)

# 5) Prioridad (proxy): menor coverage/overlap/p_in_range => mayor prioridad
if "coverage_global" in plan.columns:
    cov_expr = pl.col("coverage_global").fill_null(0.0)
else:
    cov_expr = pl.lit(0.0)
if "p_overlap" in plan.columns:
    ov_expr = pl.col("p_overlap").fill_null(0.0)
else:
    ov_expr = pl.lit(0.0)
if "p_in_range" in plan.columns:
    pi_expr = pl.col("p_in_range").fill_null(0.0)
else:
    pi_expr = pl.lit(0.0)

plan = plan.with_columns(
    (0.5 * (1.0 - cov_expr) + 0.3 * (1.0 - ov_expr) + 0.2 * (1.0 - pi_expr)).alias("severity_proxy")
).with_columns(
    pl.when(pl.col("severity_proxy") >= 0.60).then(pl.lit("P0"))
      .when(pl.col("severity_proxy") >= 0.40).then(pl.lit("P1"))
      .otherwise(pl.lit("P2"))
      .alias("priority_bucket")
)

plan = plan.sort(["priority_bucket", "severity_proxy"], descending=[False, True])

# 6) Resumen de plan
track_summary = (
    plan.group_by(["recovery_track", "priority_bucket"])
    .agg(pl.len().alias("n_tickers"))
    .sort(["recovery_track", "priority_bucket"])
)
root_summary = (
    plan.group_by("root_cause")
    .agg(pl.len().alias("n_tickers"))
    .with_columns((pl.col("n_tickers") / pl.lit(max(plan.height, 1))).alias("ratio"))
    .sort("n_tickers", descending=True)
)

# 7) Guardar artifacts
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = OUT_ROOT / f"step2_schema_gap_recovery_plan_{stamp}"
out_dir.mkdir(parents=True, exist_ok=True)

plan_path = out_dir / "schema_gap_recovery_plan.parquet"
track_path = out_dir / "schema_gap_recovery_track_summary.parquet"
root_path = out_dir / "schema_gap_recovery_root_summary.parquet"
manifest_path = out_dir / "step2_recovery_plan_manifest.json"

plan.write_parquet(plan_path)
track_summary.write_parquet(track_path)
root_summary.write_parquet(root_path)

manifest_out = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "baseline_dir": str(baseline_dir),
    "policy_source": str(policy_path),
    "decision_col_used": decision_col,
    "root_col_used": root_col,
    "n_not_comparable": int(plan.height),
    "files": {
        "recovery_plan": str(plan_path),
        "track_summary": str(track_path),
        "root_summary": str(root_path),
    },
}
manifest_path.write_text(json.dumps(manifest_out, indent=2, ensure_ascii=False), encoding="utf-8")

# 8) Imprimir salida congelable
print("=== STEP 2 - SCHEMA_GAP RECOVERY PLAN ===")
print("baseline:", baseline_dir)
print("policy source:", policy_path)
print("output_dir:", out_dir)
print("n_not_comparable:", plan.height)
print("[Recovery track x priority]")
print(track_summary)
print("[Root cause summary]")
print(root_summary)
print("[Sample recovery plan rows]")
print(plan.head(20))
