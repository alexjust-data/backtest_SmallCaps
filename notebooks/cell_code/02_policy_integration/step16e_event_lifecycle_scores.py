# -*- coding: utf-8 -*-
# PASO 16E - EVENT LIFECYCLE SCORING (explosion + decay)
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import polars as pl
from IPython.display import display, Markdown

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
OUT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"
OUT_DIR = OUT_ROOT / f"step16e_event_lifecycle_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Thresholds (v1)
TH_EXPLOSION = 2.0
TH_DECAY = 1.2
TH_EVENT_LIFECYCLE = 2.8

run_dirs = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)
if not run_dirs:
    raise FileNotFoundError("No hay salidas step16_event_index_*")
run_dir = run_dirs[-1]
fp = run_dir / "step16_event_index_quotes_only.parquet"
if not fp.exists():
    raise FileNotFoundError(f"No existe {fp}")

base = pl.read_parquet(fp).sort(["ticker", "date"])
if base.height == 0:
    raise RuntimeError("step16_event_index vac?o")

# Deduplicaci?n estricta por clave diaria antes de features forward
# Conserva la fila m?s informativa por (ticker,date): mayor n_ticks y mayor event_score.
base = (
    base.sort(["ticker", "date", "n_ticks", "event_score"], descending=[False, False, True, True])
        .unique(subset=["ticker", "date"], keep="first")
        .sort(["ticker", "date"])
)

pdf = base.to_pandas()
pdf["date"] = pd.to_datetime(pdf["date"])

# Ensure numeric
for c in ["day_return", "day_range", "n_ticks", "z_range", "z_ticks", "event_score"]:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

parts = []
for ticker, g in pdf.groupby("ticker", sort=False):
    g = g.sort_values("date").copy()

    # Explosion side
    g["ret_pos"] = g["day_return"].clip(lower=0.0)
    med_ret_pos = g.loc[g["ret_pos"] > 0, "ret_pos"].median()
    if pd.isna(med_ret_pos) or med_ret_pos <= 1e-9:
        med_ret_pos = 1e-6
    g["ret_pos_scale"] = g["ret_pos"] / med_ret_pos

    g["score_explosion"] = (
        0.45 * g["z_range"].fillna(0.0)
        + 0.35 * g["z_ticks"].fillna(0.0)
        + 0.20 * g["ret_pos_scale"].fillna(0.0)
    )

    # Decay side (future reversal / dump after spike)
    r1 = g["day_return"].shift(-1)
    r2 = g["day_return"].shift(-2)
    r3 = g["day_return"].shift(-3)

    # 3-day forward cumulative return from t+1..t+3
    g["fwd_ret_3d"] = (1.0 + r1).fillna(1.0) * (1.0 + r2).fillna(1.0) * (1.0 + r3).fillna(1.0) - 1.0
    g["fwd_ret_1d"] = r1

    # Negative future return intensity (only decay component)
    g["neg_fwd3d"] = (-g["fwd_ret_3d"]).clip(lower=0.0)
    med_neg = g.loc[g["neg_fwd3d"] > 0, "neg_fwd3d"].median()
    if pd.isna(med_neg) or med_neg <= 1e-9:
        med_neg = 1e-6
    g["neg_fwd3d_scale"] = g["neg_fwd3d"] / med_neg

    # Minimum next-3-day return as drawdown proxy
    g["min_fwd_3d"] = pd.concat([r1, r2, r3], axis=1).min(axis=1)
    g["drawdown_proxy"] = (-g["min_fwd_3d"]).clip(lower=0.0)
    med_dd = g.loc[g["drawdown_proxy"] > 0, "drawdown_proxy"].median()
    if pd.isna(med_dd) or med_dd <= 1e-9:
        med_dd = 1e-6
    g["drawdown_scale"] = g["drawdown_proxy"] / med_dd

    # Activity fade proxy (next-day ticks drop)
    n1 = g["n_ticks"].shift(-1)
    g["ticks_drop_1d"] = ((g["n_ticks"] - n1) / g["n_ticks"].replace(0, np.nan)).clip(lower=0.0).fillna(0.0)

    g["score_decay"] = (
        0.50 * g["neg_fwd3d_scale"].fillna(0.0)
        + 0.30 * g["drawdown_scale"].fillna(0.0)
        + 0.20 * g["ticks_drop_1d"].fillna(0.0)
    )

    # Lifecycle event = explosion now + decay evidence after
    g["is_explosion_day"] = (
        (g["score_explosion"] >= TH_EXPLOSION)
        | (g["day_range"] >= 0.12)
        | (g["day_return"] >= 0.10)
    )
    g["is_decay_followup"] = g["score_decay"] >= TH_DECAY

    g["event_lifecycle_score"] = 0.6 * g["score_explosion"] + 0.4 * g["score_decay"]
    g["is_lifecycle_event"] = (
        g["is_explosion_day"] & g["is_decay_followup"] & (g["event_lifecycle_score"] >= TH_EVENT_LIFECYCLE)
    )

    parts.append(g)

out_pd = pd.concat(parts, axis=0).sort_values(["ticker", "date"])
out = pl.DataFrame(out_pd.to_dict(orient="list"))

# Save
out_fp = OUT_DIR / "step16e_event_lifecycle_scores.parquet"
out.write_parquet(out_fp)

summary = (
    out.group_by("priority_bucket")
    .agg([
        pl.len().alias("n_days"),
        pl.col("ticker").n_unique().alias("n_tickers"),
        pl.col("is_explosion_day").sum().alias("n_explosion_days"),
        pl.col("is_decay_followup").sum().alias("n_decay_days"),
        pl.col("is_lifecycle_event").sum().alias("n_lifecycle_events"),
        pl.col("score_explosion").mean().alias("score_explosion_mean"),
        pl.col("score_decay").mean().alias("score_decay_mean"),
        pl.col("event_lifecycle_score").quantile(0.95).alias("lifecycle_score_p95"),
    ])
    .sort("priority_bucket")
)
summary_fp = OUT_DIR / "step16e_summary.parquet"
summary.write_parquet(summary_fp)

# Top lifecycle cases
top = (
    out.filter(pl.col("is_lifecycle_event") == True)
       .sort(["event_lifecycle_score", "score_explosion", "score_decay"], descending=True)
       .select([
           "ticker", "date", "priority_bucket", "source_group",
           "day_return", "day_range", "n_ticks",
           "score_explosion", "score_decay", "event_lifecycle_score",
           "is_explosion_day", "is_decay_followup", "is_lifecycle_event"
       ])
       .head(100)
)
top_fp = OUT_DIR / "step16e_top_lifecycle_events.parquet"
top.write_parquet(top_fp)

print("=== STEP 16E - EVENT LIFECYCLE SCORING ===")
print(f"input_step16: {fp}")
print(f"output_dir: {OUT_DIR}")
print(f"output_scores: {out_fp}")
print(f"summary: {summary_fp}")
print(f"top_lifecycle: {top_fp}")
print(f"n_rows: {out.height}")
print(f"n_tickers: {out.select(pl.col('ticker').n_unique()).item()}")
print(f"n_explosion_days: {out.filter(pl.col('is_explosion_day')).height}")
print(f"n_decay_followup: {out.filter(pl.col('is_decay_followup')).height}")
print(f"n_lifecycle_events: {out.filter(pl.col('is_lifecycle_event')).height}")

display(Markdown(f"### Step 16E output dir\n`{OUT_DIR}`"))
display(summary.to_pandas())
display(top.head(30).to_pandas())
