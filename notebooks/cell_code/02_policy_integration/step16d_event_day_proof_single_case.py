# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt

# Caso objetivo
TICKER = "AABA"
TARGET_DATE = "2019-01-04"

# Umbrales de Paso 16
TH_SCORE = 2.0
TH_RANGE = 0.12
TH_RET = 0.15

# Cargar ?ltimo event_index
root = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps/runs/backtest/02_policy_integration/event_index")
run_dir = sorted(root.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)[-1]
df = pl.read_parquet(run_dir / "step16_event_index_quotes_only.parquet")

t = (
    df.filter(pl.col("ticker") == TICKER)
      .sort("date")
      .with_columns(pl.col("day_return").abs().alias("abs_day_return"))
)

row = t.filter(pl.col("date") == TARGET_DATE)
if row.height == 0:
    raise ValueError(f"No existe {TICKER} {TARGET_DATE} en {run_dir}")

r = row.to_dicts()[0]
score = float(r["event_score"])
rng = float(r["day_range"])
ret = float(r["abs_day_return"])
is_event = bool(r["is_event_day"])

# Ventana visual alrededor de la fecha
pdf = t.to_pandas()
pdf["date"] = pd.to_datetime(pdf["date"])
target_dt = pd.to_datetime(TARGET_DATE)
win = pdf[(pdf["date"] >= target_dt - pd.Timedelta(days=25)) &
          (pdf["date"] <= target_dt + pd.Timedelta(days=25))].copy()

fig, axes = plt.subplots(3, 1, figsize=(13, 9), sharex=True)

# 1) event_score
axes[0].plot(win["date"], win["event_score"], lw=1.4, label="event_score")
axes[0].axhline(TH_SCORE, ls="--", c="red", lw=1.1, label=f"threshold={TH_SCORE}")
axes[0].axvline(target_dt, ls="--", c="black", lw=1.1, label=f"target={TARGET_DATE}")
axes[0].scatter([target_dt], [score], c="crimson", s=45, zorder=5)
axes[0].set_title(f"{TICKER} | Event-day proof on {TARGET_DATE}")
axes[0].legend(loc="best")
axes[0].grid(alpha=0.2)

# 2) day_range
axes[1].plot(win["date"], win["day_range"], lw=1.4, color="#1f77b4", label="day_range")
axes[1].axhline(TH_RANGE, ls="--", c="red", lw=1.1, label=f"threshold={TH_RANGE}")
axes[1].axvline(target_dt, ls="--", c="black", lw=1.1)
axes[1].scatter([target_dt], [rng], c="crimson", s=45, zorder=5)
axes[1].legend(loc="best")
axes[1].grid(alpha=0.2)

# 3) abs(day_return)
axes[2].plot(win["date"], win["abs_day_return"], lw=1.4, color="#2ca02c", label="abs(day_return)")
axes[2].axhline(TH_RET, ls="--", c="red", lw=1.1, label=f"threshold={TH_RET}")
axes[2].axvline(target_dt, ls="--", c="black", lw=1.1)
axes[2].scatter([target_dt], [ret], c="crimson", s=45, zorder=5)
axes[2].legend(loc="best")
axes[2].grid(alpha=0.2)

txt = (
    "Formula:\n"
    "is_event_day = (event_score >= 2.0) OR (day_range >= 0.12) OR (abs(day_return) >= 0.15)\n\n"
    f"{TICKER} {TARGET_DATE}\n"
    f"event_score={score:.6f}  -> {score >= TH_SCORE}\n"
    f"day_range={rng:.6f}     -> {rng >= TH_RANGE}\n"
    f"abs(day_return)={ret:.6f} -> {ret >= TH_RET}\n"
    f"is_event_day={is_event}"
)

fig.text(0.01, 0.01, txt, fontsize=10, family="monospace")

plt.tight_layout(rect=[0, 0.08, 1, 1])
plt.show()
