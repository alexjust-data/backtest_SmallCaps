from pathlib import Path
from datetime import datetime, timezone
import json
import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
RUNS_ROOT = PROJECT_ROOT / "runs"
BASELINE_ROOT = RUNS_ROOT / "baseline"
OUT_ROOT = RUNS_ROOT / "backtest" / "02_policy_integration" / "schema_gap_diagnostics"

# 1) Cargar baseline/policy (robusto)
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

policy_path = Path(manifest["source"]["06_policy_table"])
if not policy_path.exists():
    raise FileNotFoundError(f"No existe 06_policy_table referenciado: {policy_path}")
policy = pl.read_parquet(policy_path)

# 2) Resolver columnas
if "decision_policy" in policy.columns:
    decision_col = "decision_policy"
elif "decision_final" in policy.columns:
    decision_col = "decision_final"
elif "decision" in policy.columns:
    decision_col = "decision"
else:
    raise ValueError("No se encontro columna de decision")

if "root_cause_policy" in policy.columns:
    root_col = "root_cause_policy"
elif "root_cause_final" in policy.columns:
    root_col = "root_cause_final"
elif "root_cause" in policy.columns:
    root_col = "root_cause"
else:
    root_col = None

w = policy.with_columns(pl.col(decision_col).cast(pl.Utf8).alias("decision_norm"))
if root_col is None:
    w = w.with_columns(pl.lit("unknown").alias("root_cause_norm"))
else:
    w = w.with_columns(pl.col(root_col).cast(pl.Utf8).fill_null("unknown").alias("root_cause_norm"))

n_total = w.height
baseline_counts = (
    w.group_by("decision_norm").agg(pl.len().alias("n")).rename({"decision_norm": "decision"}).sort("decision")
)
baseline_map = {r["decision"]: int(r["n"]) for r in baseline_counts.to_dicts()}

schema_gap_n = w.filter((pl.col("decision_norm") == "NOT_COMPARABLE") & (pl.col("root_cause_norm") == "schema_gap")).height
scale_mismatch_n = w.filter((pl.col("decision_norm") == "NOT_COMPARABLE") & (pl.col("root_cause_norm") == "scale_mismatch")).height

# comparable actual (hoy): GO/WARN/FAIL ya comparables
go = baseline_map.get("GO", 0)
warn = baseline_map.get("WARN", 0)
fail = baseline_map.get("FAIL", 0)
comp_total = go + warn + fail

if comp_total == 0:
    prior_go = prior_warn = prior_fail = 0.0
else:
    prior_go = go / comp_total
    prior_warn = warn / comp_total
    prior_fail = fail / comp_total

# 3) Escenarios
# Escenario A (conservador): schema_gap desbloquea comparabilidad, pero cae a FAIL inicial hasta rerun real
a_go, a_warn, a_fail = go, warn, fail + schema_gap_n
a_notc = baseline_map.get("NOT_COMPARABLE", 0) - schema_gap_n

# Escenario B (distribucional): schema_gap desbloqueado se reparte seg?n mix actual comparable (GO/WARN/FAIL)
b_add_go = round(schema_gap_n * prior_go)
b_add_warn = round(schema_gap_n * prior_warn)
b_add_fail = schema_gap_n - b_add_go - b_add_warn
b_go, b_warn, b_fail = go + b_add_go, warn + b_add_warn, fail + b_add_fail
b_notc = baseline_map.get("NOT_COMPARABLE", 0) - schema_gap_n

scenario_df = pl.DataFrame([
    {"scenario": "BASELINE", "GO": go, "WARN": warn, "FAIL": fail, "NOT_COMPARABLE": baseline_map.get("NOT_COMPARABLE", 0)},
    {"scenario": "A_conservative_unlock_to_FAIL", "GO": a_go, "WARN": a_warn, "FAIL": a_fail, "NOT_COMPARABLE": a_notc},
    {"scenario": "B_distributional_unlock", "GO": b_go, "WARN": b_warn, "FAIL": b_fail, "NOT_COMPARABLE": b_notc},
]).with_columns([
    (pl.col("GO") / pl.lit(max(n_total, 1))).alias("GO_ratio"),
    (pl.col("WARN") / pl.lit(max(n_total, 1))).alias("WARN_ratio"),
    (pl.col("FAIL") / pl.lit(max(n_total, 1))).alias("FAIL_ratio"),
    (pl.col("NOT_COMPARABLE") / pl.lit(max(n_total, 1))).alias("NOT_COMPARABLE_ratio"),
])

assumptions = {
    "n_total": int(n_total),
    "schema_gap_n": int(schema_gap_n),
    "scale_mismatch_n": int(scale_mismatch_n),
    "current_comparable_mix": {
        "GO": go,
        "WARN": warn,
        "FAIL": fail,
        "prior_go": prior_go,
        "prior_warn": prior_warn,
        "prior_fail": prior_fail,
    },
    "notes": [
        "Esto es simulacion de impacto; no reemplaza rerun real de 03/06.",
        "En schema_gap actual no hay metricas de cobertura/coherencia disponibles (null).",
        "Reclasificacion real requiere remediacion + rerun masivo.",
    ],
}

# 4) Guardar
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = OUT_ROOT / f"step3_impact_simulation_{stamp}"
out_dir.mkdir(parents=True, exist_ok=True)

scenario_path = out_dir / "step3_impact_scenarios.parquet"
assum_path = out_dir / "step3_impact_assumptions.json"
scenario_df.write_parquet(scenario_path)
assum_path.write_text(json.dumps(assumptions, indent=2, ensure_ascii=False), encoding="utf-8")

# 5) Imprimir
print("=== STEP 3 - IMPACT SIMULATION (PRE-RERUN) ===")
print("baseline:", baseline_dir)
print("policy source:", policy_path)
print("output_dir:", out_dir)
print("[Assumptions]")
print(assumptions)
print("[Scenario table]")
print(scenario_df)
