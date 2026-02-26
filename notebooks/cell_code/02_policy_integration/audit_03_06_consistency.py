from pathlib import Path
from datetime import datetime, timezone
import json
import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
RUNS_ROOT = PROJECT_ROOT / "runs"
BASELINE_ROOT = RUNS_ROOT / "baseline"
OUT_ROOT = RUNS_ROOT / "backtest" / "02_policy_integration" / "schema_gap_diagnostics"

# baseline robusto
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

manifest_path = baseline_dir / "baseline_before_manifest.json"
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
else:
    if "manifest" in globals() and isinstance(globals()["manifest"], dict):
        manifest = globals()["manifest"]
    else:
        raise FileNotFoundError(f"No existe baseline_before_manifest.json en {baseline_dir}")

p06 = Path(manifest["source"]["06_policy_table"])
p03 = Path(manifest["source"]["03_run_dir"]) / "03_massive_metrics.parquet"

if not p06.exists():
    raise FileNotFoundError(f"No existe source_06: {p06}")
if not p03.exists():
    raise FileNotFoundError(f"No existe source_03: {p03}")

m06 = pl.read_parquet(p06)
m03 = pl.read_parquet(p03)

if "decision_policy" in m06.columns:
    dcol = "decision_policy"
elif "decision_final" in m06.columns:
    dcol = "decision_final"
else:
    dcol = "decision"

if "root_cause_policy" in m06.columns:
    rcol = "root_cause_policy"
elif "root_cause_final" in m06.columns:
    rcol = "root_cause_final"
else:
    rcol = "root_cause"

j = m06.select(["ticker", dcol, rcol]).join(
    m03.select(["ticker", "n_days", "coverage_mean", "violation_ratio_full"]),
    on="ticker", how="left"
).with_columns([
    (pl.col("n_days") > 0).alias("has_days"),
    (pl.col("coverage_mean").is_finite() & pl.col("violation_ratio_full").is_finite()).alias("has_finite_metrics"),
])

id_counts = {
    "n_total": int(j.height),
    "n_valid03_finite": int(j.filter(pl.col("has_finite_metrics")).height),
    "n_comparable_06": int(j.filter(pl.col(dcol) != "NOT_COMPARABLE").height),
    "n_notc_scale": int(j.filter((pl.col(dcol) == "NOT_COMPARABLE") & (pl.col(rcol) == "scale_mismatch")).height),
    "n_notc_schema": int(j.filter((pl.col(dcol) == "NOT_COMPARABLE") & (pl.col(rcol) == "schema_gap")).height),
}
id_counts["identity_comparable_plus_scale_equals_valid03"] = (id_counts["n_comparable_06"] + id_counts["n_notc_scale"] == id_counts["n_valid03_finite"])

cross_decision_days = j.group_by([dcol, "has_days"]).agg(pl.len().alias("n")).sort([dcol, "has_days"])
cross_notc_root_days = j.filter(pl.col(dcol) == "NOT_COMPARABLE").group_by([rcol, "has_days"]).agg(pl.len().alias("n")).sort([rcol, "has_days"])

nan_null_summary = j.select([
    pl.len().alias("n_total"),
    pl.col("coverage_mean").is_null().sum().alias("coverage_null"),
    pl.col("coverage_mean").is_nan().sum().alias("coverage_nan"),
    pl.col("violation_ratio_full").is_null().sum().alias("viol_null"),
    pl.col("violation_ratio_full").is_nan().sum().alias("viol_nan"),
])

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = OUT_ROOT / f"audit_03_06_consistency_{stamp}"
out_dir.mkdir(parents=True, exist_ok=True)

cross_decision_days.write_parquet(out_dir / "cross_decision_has_days.parquet")
cross_notc_root_days.write_parquet(out_dir / "cross_notc_root_has_days.parquet")
nan_null_summary.write_parquet(out_dir / "nan_null_summary.parquet")

(out_dir / "audit_identity_summary.json").write_text(json.dumps({
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "baseline_dir": str(baseline_dir),
    "source_03": str(p03),
    "source_06": str(p06),
    "identity": id_counts,
}, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== AUDIT 03<->06 CONSISTENCY ===")
print("baseline:", baseline_dir)
print("source_03:", p03)
print("source_06:", p06)
print("output_dir:", out_dir)
print("[Identity check]")
print(id_counts)
print("[Cross decision x has_days]")
print(cross_decision_days)
print("[Cross NOT_COMPARABLE root_cause x has_days]")
print(cross_notc_root_days)
print("[NaN vs null summary]")
print(nan_null_summary)
