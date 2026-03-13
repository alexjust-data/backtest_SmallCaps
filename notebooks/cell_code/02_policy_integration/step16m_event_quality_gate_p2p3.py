# STEP 16M - EVENT QUALITY GATE FOR P2/P3
from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")

# Inputs
repair_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "repair_queue"
queue_fp = sorted(repair_root.glob("step15_repair_queue_*/repair_queue_v1.parquet"), key=lambda p: p.stat().st_mtime)[-1]

life_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"
life_fp = sorted(life_root.glob("step16e_event_lifecycle_*/step16e_event_lifecycle_scores.parquet"), key=lambda p: p.stat().st_mtime)[-1]

reg_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle_regime"
reg_fp = sorted(reg_root.glob("step16i_regime_*/step16i_event_regime_labels.parquet"), key=lambda p: p.stat().st_mtime)[-1]

# Optional split/outlier sources (may have zero P2/P3 coverage)
k_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "outlier_audit"
k_fp = sorted(k_root.glob("step16k_split_extreme_*/step16k_extremes_labeled.parquet"), key=lambda p: p.stat().st_mtime)[-1]

l_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "split_reverse_engineering"
l_fp = sorted(l_root.glob("step16l_split_reverse_*/step16l_jumps_labeled.parquet"), key=lambda p: p.stat().st_mtime)[-1]

queue = pl.read_parquet(queue_fp)
p23 = queue.filter(pl.col("priority_bucket").is_in(["P2", "P3"])).select([
    c for c in [
        "ticker", "priority_bucket", "repair_cause", "overlap_months_total",
        "overlap_quote_days_total", "year_gap_o_first_minus_q_last"
    ] if c in queue.columns
]).unique(subset=["ticker"])

life = pl.read_parquet(life_fp)
reg = pl.read_parquet(reg_fp)
ext16k = pl.read_parquet(k_fp)
jumps16l = pl.read_parquet(l_fp)

# 16E aggregation by ticker
life_tk = (
    life.group_by("ticker").agg([
        pl.len().alias("n_days_16e"),
        pl.col("is_lifecycle_event").sum().alias("n_lifecycle_events"),
        pl.col("event_lifecycle_score").max().alias("max_lifecycle_score"),
        pl.col("score_explosion").max().alias("max_score_explosion"),
        pl.col("score_decay").max().alias("max_score_decay"),
    ])
)

# 16I best regime row per ticker (highest lifecycle score)
reg_best = (
    reg.sort(["ticker", "event_lifecycle_score"], descending=[False, True])
       .unique(subset=["ticker"], keep="first")
       .select([
           "ticker", "regime_label", "event_lifecycle_score", "dd10", "rebound_ratio_10d"
       ])
       .rename({"event_lifecycle_score": "regime_event_lifecycle_score"})
)

# Optional split signals; may not cover P2/P3 in current run
k_tk = ext16k.group_by("ticker").agg([
    pl.len().alias("n_extreme_16k"),
    pl.col("extreme_class").n_unique().alias("n_extreme_classes_16k"),
]) if ext16k.height > 0 and "ticker" in ext16k.columns else pl.DataFrame({"ticker": []}, schema={"ticker": pl.Utf8})

l_tk = jumps16l.group_by("ticker").agg([
    pl.len().alias("n_jumps_16l"),
    (pl.col("jump_class") == "split_like_unmatched").sum().alias("n_split_like_unmatched"),
    (pl.col("jump_class") == "split_matched").sum().alias("n_split_matched"),
]) if jumps16l.height > 0 and "ticker" in jumps16l.columns else pl.DataFrame({"ticker": []}, schema={"ticker": pl.Utf8})

# Join all evidence on P2/P3 queue
gate = (
    p23
    .join(life_tk, on="ticker", how="left")
    .join(reg_best, on="ticker", how="left")
    .join(k_tk, on="ticker", how="left")
    .join(l_tk, on="ticker", how="left")
    .with_columns([
        pl.col("n_days_16e").fill_null(0),
        pl.col("n_lifecycle_events").fill_null(0),
        pl.col("max_lifecycle_score").fill_null(-1.0),
        pl.col("max_score_explosion").fill_null(-1.0),
        pl.col("max_score_decay").fill_null(-1.0),
        pl.col("n_extreme_16k").fill_null(0),
        pl.col("n_extreme_classes_16k").fill_null(0),
        pl.col("n_jumps_16l").fill_null(0),
        pl.col("n_split_like_unmatched").fill_null(0),
        pl.col("n_split_matched").fill_null(0),
    ])
)

# Coverage flags
gate = gate.with_columns([
    (pl.col("n_days_16e") > 0).alias("has_16e"),
    pl.col("regime_label").is_not_null().alias("has_16i"),
    (pl.col("n_extreme_16k") > 0).alias("has_16k"),
    (pl.col("n_jumps_16l") > 0).alias("has_16l"),
])

# Gate rules
# usable_event: strong lifecycle + useful regime label
usable_regimes = ["pump_then_dump_confirmed", "pump_then_shakeout"]

gate = gate.with_columns([
    (
        pl.col("has_16e")
        & (pl.col("n_lifecycle_events") > 0)
        & (pl.col("max_lifecycle_score") >= 2.8)
        & pl.col("has_16i")
        & pl.col("regime_label").is_in(usable_regimes)
    ).alias("rule_usable_event"),
    (
        pl.col("has_16e")
        & (
            (pl.col("n_lifecycle_events") > 0)
            | (pl.col("max_score_explosion") >= 2.0)
        )
    ).alias("rule_usable_partial_base"),
])

# split signal handling: if 16K/16L not covering this ticker => unknown (do not block)
gate = gate.with_columns([
    pl.when(~pl.col("has_16k") & ~pl.col("has_16l")).then(pl.lit("split_signal_unknown"))
      .when((pl.col("n_split_like_unmatched") > 0) & (pl.col("n_split_matched") == 0)).then(pl.lit("split_signal_risky"))
      .otherwise(pl.lit("split_signal_ok"))
      .alias("split_signal_status")
])

gate = gate.with_columns([
    pl.when(pl.col("rule_usable_event")).then(pl.lit("usable_event"))
      .when(pl.col("rule_usable_partial_base") & (pl.col("split_signal_status") != "split_signal_risky")).then(pl.lit("usable_partial"))
      .otherwise(pl.lit("not_usable_event"))
      .alias("event_quality_gate"),
])

gate = gate.with_columns([
    pl.when(pl.col("event_quality_gate") == "usable_event").then(pl.lit("lifecycle+regime_strong"))
      .when(pl.col("event_quality_gate") == "usable_partial").then(pl.lit("lifecycle_or_explosion_without_strong_regime"))
      .otherwise(pl.lit("insufficient_event_evidence_or_split_risk"))
      .alias("gate_reason")
])

# Outputs
out_dir = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_quality_gate" / f"step16m_event_quality_gate_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
out_dir.mkdir(parents=True, exist_ok=True)

gate_fp = out_dir / "step16m_p2p3_event_quality_gate.parquet"
gate_csv = out_dir / "step16m_p2p3_event_quality_gate.csv"
promoted_event_fp = out_dir / "step16m_promoted_usable_event.parquet"
promoted_partial_fp = out_dir / "step16m_promoted_usable_partial.parquet"
summary_fp = out_dir / "step16m_gate_summary.parquet"
manifest_fp = out_dir / "step16m_gate_manifest.json"

gate.write_parquet(gate_fp)
gate.write_csv(gate_csv)

promoted_event = gate.filter(pl.col("event_quality_gate") == "usable_event").select(["ticker", "priority_bucket", "repair_cause", "event_quality_gate", "gate_reason", "regime_label", "max_lifecycle_score"]).sort("ticker")
promoted_partial = gate.filter(pl.col("event_quality_gate") == "usable_partial").select(["ticker", "priority_bucket", "repair_cause", "event_quality_gate", "gate_reason", "regime_label", "max_lifecycle_score"]).sort("ticker")

promoted_event.write_parquet(promoted_event_fp)
promoted_partial.write_parquet(promoted_partial_fp)

summary = gate.group_by(["event_quality_gate", "split_signal_status"]).agg([
    pl.len().alias("n_tickers"),
    pl.col("has_16e").sum().alias("n_with_16e"),
    pl.col("has_16i").sum().alias("n_with_16i"),
]).sort(["event_quality_gate", "split_signal_status"])
summary.write_parquet(summary_fp)

manifest = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "inputs": {
        "repair_queue": str(queue_fp),
        "step16e": str(life_fp),
        "step16i": str(reg_fp),
        "step16k": str(k_fp),
        "step16l": str(l_fp),
    },
    "rules": {
        "usable_event": "has_16e & n_lifecycle_events>0 & max_lifecycle_score>=2.8 & has_16i & regime in {pump_then_dump_confirmed,pump_then_shakeout}",
        "usable_partial": "has_16e & (n_lifecycle_events>0 or max_score_explosion>=2.0) & split_signal_status!=split_signal_risky",
        "not_usable_event": "otherwise",
    },
    "outputs": {
        "gate_table": str(gate_fp),
        "gate_csv": str(gate_csv),
        "promoted_usable_event": str(promoted_event_fp),
        "promoted_usable_partial": str(promoted_partial_fp),
        "summary": str(summary_fp),
    },
}
manifest_fp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 16M - EVENT QUALITY GATE (P2/P3) ===")
print("output_dir:", out_dir)
print("n_input_p2p3:", gate.height)
print("\n[Gate summary]")
print(summary)
print("\n[promoted usable_event]")
print(promoted_event.head(20))
print("\n[promoted usable_partial]")
print(promoted_partial.head(20))
print("\nSaved:")
print(gate_fp)
print(gate_csv)
print(promoted_event_fp)
print(promoted_partial_fp)
print(summary_fp)
print(manifest_fp)