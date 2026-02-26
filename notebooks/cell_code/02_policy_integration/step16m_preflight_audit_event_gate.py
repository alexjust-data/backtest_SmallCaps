from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")

# Latest inputs
repair_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "repair_queue"
q_fp = sorted(repair_root.glob("step15_repair_queue_*/repair_queue_v1.parquet"), key=lambda p: p.stat().st_mtime)[-1]

life_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"
life_fp = sorted(life_root.glob("step16e_event_lifecycle_*/step16e_event_lifecycle_scores.parquet"), key=lambda p: p.stat().st_mtime)[-1]

reg_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle_regime"
reg_fp = sorted(reg_root.glob("step16i_regime_*/step16i_event_regime_labels.parquet"), key=lambda p: p.stat().st_mtime)[-1]

k_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "outlier_audit"
k_fp = sorted(k_root.glob("step16k_split_extreme_*/step16k_extremes_labeled.parquet"), key=lambda p: p.stat().st_mtime)[-1]

l_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "split_reverse_engineering"
l_fp = sorted(l_root.glob("step16l_split_reverse_*/step16l_jumps_labeled.parquet"), key=lambda p: p.stat().st_mtime)[-1]

queue = pl.read_parquet(q_fp)
p23 = queue.filter(pl.col("priority_bucket").is_in(["P2", "P3"]))
life = pl.read_parquet(life_fp)
reg = pl.read_parquet(reg_fp)
ext16k = pl.read_parquet(k_fp)
jumps16l = pl.read_parquet(l_fp)

# Required columns audit
required = {
    "16e": ["ticker", "date", "is_lifecycle_event", "score_explosion", "score_decay", "event_lifecycle_score"],
    "16i": ["ticker", "event_date", "regime_label", "dd10", "rebound_ratio_10d"],
    "16k": ["ticker", "event_date", "extreme_class"],
    "16l": ["ticker", "event_date", "jump_class", "factor_rel_error"],
}

missing = {
    "16e": [c for c in required["16e"] if c not in life.columns],
    "16i": [c for c in required["16i"] if c not in reg.columns],
    "16k": [c for c in required["16k"] if c not in ext16k.columns],
    "16l": [c for c in required["16l"] if c not in jumps16l.columns],
}

# Duplicate checks
life_dup = life.group_by(["ticker", "date"]).agg(pl.len().alias("n")).filter(pl.col("n") > 1)
reg_dup = reg.group_by(["ticker", "event_date"]).agg(pl.len().alias("n")).filter(pl.col("n") > 1)

# Coverage vs P2/P3
p23_tk = p23.select("ticker").unique()
life_tk = life.select("ticker").unique()
reg_tk = reg.select("ticker").unique()
k_tk = ext16k.select("ticker").unique() if "ticker" in ext16k.columns else pl.DataFrame({"ticker": []}, schema={"ticker": pl.Utf8})
l_tk = jumps16l.select("ticker").unique() if "ticker" in jumps16l.columns else pl.DataFrame({"ticker": []}, schema={"ticker": pl.Utf8})

cov = pl.DataFrame([
    {"source": "P2P3_queue", "n_tickers": p23_tk.height},
    {"source": "16E_lifecycle", "n_tickers": p23_tk.join(life_tk, on="ticker", how="inner").height},
    {"source": "16I_regime", "n_tickers": p23_tk.join(reg_tk, on="ticker", how="inner").height},
    {"source": "16K_outlier", "n_tickers": p23_tk.join(k_tk, on="ticker", how="inner").height},
    {"source": "16L_jumps", "n_tickers": p23_tk.join(l_tk, on="ticker", how="inner").height},
])

# Quality flags
l_counts = jumps16l.group_by("jump_class").agg(pl.len().alias("n")).sort("n", descending=True)
k_counts = ext16k.group_by("extreme_class").agg(pl.len().alias("n")).sort("n", descending=True)

preflight_ok = (
    all(len(v) == 0 for v in missing.values())
    and life_dup.height == 0
    and cov.filter(pl.col("source") == "16E_lifecycle")["n_tickers"][0] > 0
)

out_dir = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_gate_preflight" / f"step16m_preflight_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
out_dir.mkdir(parents=True, exist_ok=True)

cov.write_parquet(out_dir / "preflight_coverage_p2p3.parquet")
l_counts.write_parquet(out_dir / "preflight_16l_jump_class_counts.parquet")
k_counts.write_parquet(out_dir / "preflight_16k_extreme_class_counts.parquet")
life_dup.write_parquet(out_dir / "preflight_16e_duplicates.parquet")
reg_dup.write_parquet(out_dir / "preflight_16i_duplicates.parquet")

summary = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "inputs": {
        "repair_queue": str(q_fp),
        "step16e": str(life_fp),
        "step16i": str(reg_fp),
        "step16k": str(k_fp),
        "step16l": str(l_fp),
    },
    "missing_columns": missing,
    "duplicates": {
        "16e_ticker_date": int(life_dup.height),
        "16i_ticker_event_date": int(reg_dup.height),
    },
    "coverage_p2p3": cov.to_dicts(),
    "ok_for_event_gate": bool(preflight_ok),
}
(out_dir / "preflight_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 16M PRE-FLIGHT AUDIT ===")
print("output_dir:", out_dir)
print("ok_for_event_gate:", preflight_ok)
print("\n[Missing columns]")
print(missing)
print("\n[Duplicates]")
print({"16e": life_dup.height, "16i": reg_dup.height})
print("\n[Coverage P2/P3 across sources]")
print(cov)
print("\n[16L jump_class counts]")
print(l_counts)
print("\n[16K extreme_class counts]")
print(k_counts)