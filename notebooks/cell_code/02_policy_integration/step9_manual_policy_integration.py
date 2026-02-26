from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
RUN06_ROOT = PROJECT_ROOT / "runs" / "data_quality" / "06_ohlcv_vs_quotes"
POLICY_RUN = RUN06_ROOT / "20260224_140714_mvp_03_06_alignment_massive_v2"

policy_fp = POLICY_RUN / "mvp_policy_mode_table.parquet"
if not policy_fp.exists():
    raise FileNotFoundError(f"Policy file no encontrado: {policy_fp}")

manual_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "scale_mismatch_diagnostics"
reviewed_candidates = sorted(
    manual_root.glob(
        "step4_scale_mismatch_plan_*/manual_review/review_outcome_v1/manual_review_decision_table_reviewed_v1.parquet"
    ),
    key=lambda p: p.stat().st_mtime,
)
if not reviewed_candidates:
    raise FileNotFoundError("No existe manual_review_decision_table_reviewed_v1.parquet")
reviewed_fp = reviewed_candidates[-1]

df = pl.read_parquet(policy_fp)
rv = pl.read_parquet(reviewed_fp).select([
    "ticker",
    "reviewer_decision",
    "reviewer_manual_factor",
    "reviewer_notes",
])

joined = df.join(rv, on="ticker", how="left")

final_df = joined.with_columns([
    pl.when(pl.col("reviewer_decision") == "manual_factor")
    .then(pl.lit("FAIL"))
    .otherwise(pl.col("decision_policy"))
    .alias("decision_policy_final"),
    pl.when(pl.col("reviewer_decision") == "manual_factor")
    .then(pl.lit("scale_manual_recheck"))
    .otherwise(pl.col("root_cause_policy"))
    .alias("root_cause_policy_final"),
    pl.when(pl.col("reviewer_decision") == "manual_factor")
    .then(pl.col("reviewer_manual_factor"))
    .otherwise(pl.col("auto_scale_factor"))
    .alias("effective_scale_factor"),
])

out_dir = (
    PROJECT_ROOT
    / "runs"
    / "backtest"
    / "02_policy_integration"
    / "final_policy"
    / f"step9_manual_integration_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
out_dir.mkdir(parents=True, exist_ok=True)

final_fp = out_dir / "mvp_policy_mode_table_final_manual_v1.parquet"
summary_fp = out_dir / "step9_manual_integration_summary.json"

final_df.write_parquet(final_fp)

before_counts = df.group_by("decision_policy").agg(pl.len().alias("n")).sort("decision_policy")
after_counts = final_df.group_by("decision_policy_final").agg(pl.len().alias("n")).sort("decision_policy_final")
before_rc = df.group_by("root_cause_policy").agg(pl.len().alias("n")).sort("root_cause_policy")
after_rc = final_df.group_by("root_cause_policy_final").agg(pl.len().alias("n")).sort("root_cause_policy_final")

summary = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_policy": str(policy_fp),
    "source_reviewed_table": str(reviewed_fp),
    "n_total": int(final_df.height),
    "manual_overrides_applied": int(
        final_df.filter(pl.col("reviewer_decision") == "manual_factor").height
    ),
    "decision_before": before_counts.to_dicts(),
    "decision_after": after_counts.to_dicts(),
    "root_cause_before": before_rc.to_dicts(),
    "root_cause_after": after_rc.to_dicts(),
    "output_final_policy": str(final_fp),
}
summary_fp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 9 - MANUAL POLICY INTEGRATION ===")
print("source_policy:", policy_fp)
print("source_reviewed_table:", reviewed_fp)
print("output_dir:", out_dir)
print("\n[Decision BEFORE]")
print(before_counts)
print("\n[Decision AFTER]")
print(after_counts)
print("\n[Root cause BEFORE]")
print(before_rc)
print("\n[Root cause AFTER]")
print(after_rc)
print("\nmanual_overrides_applied:", summary["manual_overrides_applied"])
print("Saved:")
print(final_fp)
print(summary_fp)