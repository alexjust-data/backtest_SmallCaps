from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")

# Inputs
STEP14_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "schema_gap_diagnostics"
rec_candidates = sorted(
    STEP14_ROOT.glob("step14_overlap_check_2004_2025_*/step14_recoverable_tickers.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not rec_candidates:
    raise FileNotFoundError("No existe step14_recoverable_tickers.parquet")
recoverable_fp = rec_candidates[-1]

V3_ROOT = PROJECT_ROOT / "runs" / "data_quality" / "03_time_coverage_v3"
v3_candidates = sorted(
    V3_ROOT.glob("*_prefilter_v3_multi_era/03_universe_prefilter_v3.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not v3_candidates:
    raise FileNotFoundError("No existe 03_universe_prefilter_v3.parquet")
v3_prefilter_fp = v3_candidates[-1]

thr_min_months = 3
thr_min_days = 20
thr_cont_gap = 2

rec = pl.read_parquet(recoverable_fp)
v3 = pl.read_parquet(v3_prefilter_fp).select([
    "ticker",
    "overlap_months_total",
    "overlap_quote_days_total",
    "year_gap_o_first_minus_q_last",
    "prefilter_decision",
    "prefilter_reason",
])

q = rec.join(v3, on="ticker", how="left")

q = q.with_columns([
    pl.lit("PENDING_REPAIR").alias("repair_state"),
    pl.when(pl.col("year_gap_o_first_minus_q_last").is_not_null() & (pl.col("year_gap_o_first_minus_q_last") >= thr_cont_gap))
    .then(pl.lit("continuity_gap"))
    .when(pl.col("overlap_months_total") < thr_min_months)
    .then(pl.lit("low_overlap_months"))
    .when(pl.col("overlap_quote_days_total") < thr_min_days)
    .then(pl.lit("low_overlap_days"))
    .otherwise(pl.lit("window_shift"))
    .alias("repair_cause"),
]).with_columns([
    pl.when(pl.col("repair_cause") == "window_shift")
    .then(pl.lit("P1"))
    .when(pl.col("repair_cause") == "low_overlap_months")
    .then(pl.lit("P2"))
    .when(pl.col("repair_cause") == "low_overlap_days")
    .then(pl.lit("P3"))
    .otherwise(pl.lit("P4"))
    .alias("priority_bucket"),
])

# First automatic repair pass: window_shift -> eligible (candidate)
pass_df = q.with_columns([
    pl.when(pl.col("repair_cause") == "window_shift")
    .then(pl.lit(True))
    .otherwise(pl.lit(False))
    .alias("auto_repair_applied"),
    pl.when(pl.col("repair_cause") == "window_shift")
    .then(pl.lit("eligible"))
    .otherwise(pl.lit("pending_repair"))
    .alias("post_repair_status"),
    pl.when(pl.col("repair_cause") == "window_shift")
    .then(pl.lit("auto_window_shift_repair_v1"))
    .otherwise(pl.lit("pending_manual_or_rule_v2"))
    .alias("post_repair_reason"),
])

# Outputs
out_dir = (
    PROJECT_ROOT
    / "runs"
    / "backtest"
    / "02_policy_integration"
    / "repair_queue"
    / f"step15_repair_queue_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
out_dir.mkdir(parents=True, exist_ok=True)

queue_fp = out_dir / "repair_queue_v1.parquet"
queue_csv_fp = out_dir / "repair_queue_v1.csv"
summary_fp = out_dir / "step15_repair_queue_summary.json"
pass_fp = out_dir / "step15_first_pass_results.parquet"
promoted_fp = out_dir / "step15_first_pass_promoted_tickers.parquet"

pass_df.write_parquet(pass_fp)
# CSV plano (sin columnas anidadas)
csv_cols = [
    "ticker",
    "repair_state",
    "repair_cause",
    "priority_bucket",
    "overlap_months_total",
    "overlap_quote_days_total",
    "year_gap_o_first_minus_q_last",
    "prefilter_decision",
    "prefilter_reason",
    "post_repair_status",
    "post_repair_reason",
    "auto_repair_applied",
]
csv_df = pass_df.select([c for c in csv_cols if c in pass_df.columns]).sort("ticker")
csv_df.write_csv(queue_csv_fp)
pass_df.write_parquet(queue_fp)

promoted = (
    pass_df.filter(pl.col("post_repair_status") == "eligible")
    .select(["ticker", "repair_cause", "priority_bucket"])
    .sort("ticker")
)
promoted.write_parquet(promoted_fp)

cause_counts = (
    pass_df.group_by(["repair_cause", "priority_bucket"])
    .agg(pl.len().alias("n_tickers"))
    .sort(["priority_bucket", "n_tickers"], descending=[False, True])
)
status_counts = (
    pass_df.group_by("post_repair_status")
    .agg(pl.len().alias("n_tickers"))
    .sort("n_tickers", descending=True)
)

summary = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "inputs": {
        "recoverable_tickers": str(recoverable_fp),
        "v3_prefilter": str(v3_prefilter_fp),
    },
    "thresholds": {
        "min_overlap_months": thr_min_months,
        "min_overlap_quote_days": thr_min_days,
        "continuity_gap_years": thr_cont_gap,
    },
    "n_pending_repair": int(pass_df.height),
    "n_auto_repaired_window_shift": int(promoted.height),
    "repair_cause_counts": cause_counts.to_dicts(),
    "post_repair_status_counts": status_counts.to_dicts(),
    "outputs": {
        "repair_queue_parquet": str(queue_fp),
        "repair_queue_csv": str(queue_csv_fp),
        "first_pass_results": str(pass_fp),
        "promoted_tickers": str(promoted_fp),
        "summary": str(summary_fp),
    },
}
summary_fp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 15 - REPAIR QUEUE V1 ===")
print("recoverable_source:", recoverable_fp)
print("v3_prefilter_source:", v3_prefilter_fp)
print("output_dir:", out_dir)
print("[repair cause counts]")
print(cause_counts)
print("[post-repair status counts]")
print(status_counts)
print("n_pending_repair:", pass_df.height)
print("n_auto_repaired_window_shift:", promoted.height)
print("Saved:")
print(queue_fp)
print(queue_csv_fp)
print(pass_fp)
print(promoted_fp)
print(summary_fp)