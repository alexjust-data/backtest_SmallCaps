# -*- coding: utf-8 -*-
# PASO 16C - QA VISUAL: validar que Paso 16 calcula n_ticks/day_return/day_range desde quotes
from pathlib import Path
import random
import numpy as np
import polars as pl
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path(r"C:/TSIS_Data/data")

event_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
run_dir = sorted(event_root.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)[-1]
event_fp = run_dir / "step16_event_index_quotes_only.parquet"
df = pl.read_parquet(event_fp)

quote_roots = [
    DATA_ROOT / "quotes_p95_2004_2018",
    DATA_ROOT / "quotes_p95_2019_2025",
    DATA_ROOT / "quotes_p95",
]

def find_quote_file(ticker: str, date_str: str):
    y, m, d = map(int, date_str.split("-"))
    for root in quote_roots:
        fp = root / ticker / f"year={y}" / f"month={m:02d}" / f"day={d:02d}" / "quotes.parquet"
        if fp.exists():
            return fp
    return None

def recompute_from_quotes(fp: Path):
    q = (
        pl.read_parquet(fp, columns=["bid_price", "ask_price"])
        .with_columns(((pl.col("bid_price") + pl.col("ask_price")) / 2.0).alias("mid"))
        .filter(pl.col("mid").is_finite())
    )
    n_ticks = q.height
    s = q.select([
        pl.col("mid").first().alias("mid_open"),
        pl.col("mid").last().alias("mid_close"),
        pl.col("mid").min().alias("mid_min"),
        pl.col("mid").max().alias("mid_max"),
    ]).to_dicts()[0]

    day_return = (s["mid_close"] - s["mid_open"]) / s["mid_open"]
    day_range = (s["mid_max"] - s["mid_min"]) / s["mid_open"]
    return n_ticks, day_return, day_range

# Muestra aleatoria para QA visual
N = 300
rows = df.select(["ticker", "date", "n_ticks", "day_return", "day_range"]).to_dicts()
sample = random.sample(rows, min(N, len(rows)))

idx_ticks, calc_ticks = [], []
idx_ret, calc_ret = [], []
idx_rng, calc_rng = [], []

for r in sample:
    fp = find_quote_file(r["ticker"], r["date"])
    if fp is None:
        continue
    n2, ret2, rng2 = recompute_from_quotes(fp)
    idx_ticks.append(r["n_ticks"]); calc_ticks.append(n2)
    idx_ret.append(r["day_return"]); calc_ret.append(ret2)
    idx_rng.append(r["day_range"]); calc_rng.append(rng2)

idx_ticks = np.array(idx_ticks); calc_ticks = np.array(calc_ticks)
idx_ret = np.array(idx_ret); calc_ret = np.array(calc_ret)
idx_rng = np.array(idx_rng); calc_rng = np.array(calc_rng)

err_ticks = calc_ticks - idx_ticks
err_ret = calc_ret - idx_ret
err_rng = calc_rng - idx_rng

fig, axes = plt.subplots(2, 3, figsize=(18, 10))

# Identidad n_ticks
axes[0,0].scatter(idx_ticks, calc_ticks, s=12, alpha=0.65)
mn, mx = min(idx_ticks.min(), calc_ticks.min()), max(idx_ticks.max(), calc_ticks.max())
axes[0,0].plot([mn, mx], [mn, mx], "r--", lw=1.3)
axes[0,0].set_title("n_ticks: index vs recomputed")
axes[0,0].set_xlabel("index")
axes[0,0].set_ylabel("recomputed")
axes[0,0].grid(alpha=0.2)

# Identidad day_return
axes[0,1].scatter(idx_ret, calc_ret, s=12, alpha=0.65)
mn, mx = min(idx_ret.min(), calc_ret.min()), max(idx_ret.max(), calc_ret.max())
axes[0,1].plot([mn, mx], [mn, mx], "r--", lw=1.3)
axes[0,1].set_title("day_return: index vs recomputed")
axes[0,1].set_xlabel("index")
axes[0,1].set_ylabel("recomputed")
axes[0,1].grid(alpha=0.2)

# Identidad day_range
axes[0,2].scatter(idx_rng, calc_rng, s=12, alpha=0.65)
mn, mx = min(idx_rng.min(), calc_rng.min()), max(idx_rng.max(), calc_rng.max())
axes[0,2].plot([mn, mx], [mn, mx], "r--", lw=1.3)
axes[0,2].set_title("day_range: index vs recomputed")
axes[0,2].set_xlabel("index")
axes[0,2].set_ylabel("recomputed")
axes[0,2].grid(alpha=0.2)

# Hist error n_ticks
axes[1,0].hist(err_ticks, bins=30, color="#4f7ea8", alpha=0.9)
axes[1,0].set_title("Error n_ticks (recomputed - index)")
axes[1,0].grid(alpha=0.2)

# Hist error day_return
axes[1,1].hist(err_ret, bins=30, color="#e07a1f", alpha=0.9)
axes[1,1].set_title("Error day_return")
axes[1,1].grid(alpha=0.2)

# Hist error day_range
axes[1,2].hist(err_rng, bins=30, color="#5b8f5b", alpha=0.9)
axes[1,2].set_title("Error day_range")
axes[1,2].grid(alpha=0.2)

fig.suptitle(f"Step16 QA visual | run={run_dir.name} | sample={len(idx_ticks)}", y=1.02)
fig.tight_layout()
plt.show()
