# -*- coding: utf-8 -*-
# PASO 16G - DECOMPOSICION DE EVENT_SCORE
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt

# Caso objetivo (editable)
TICKER = "AABA"
TARGET_DATE = "2019-01-04"

# Pesos Paso 16
W_RANGE = 0.50
W_TICKS = 0.30
W_RET = 0.20
TH_SCORE = 2.0

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
run_dir = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)[-1]
fp = run_dir / "step16_event_index_quotes_only.parquet"

df = pl.read_parquet(fp)

need_cols = {"ticker", "date", "z_range", "z_ticks", "return_scale", "event_score", "is_event_day"}
missing = sorted(list(need_cols - set(df.columns)))
if missing:
    raise ValueError(f"Faltan columnas en step16_event_index: {missing}")

t = (
    df.filter(pl.col("ticker") == TICKER)
      .sort("date")
      .with_columns([
          (W_RANGE * pl.col("z_range")).alias("c_range"),
          (W_TICKS * pl.col("z_ticks")).alias("c_ticks"),
          (W_RET * pl.col("return_scale")).alias("c_ret"),
      ])
      .with_columns((pl.col("c_range") + pl.col("c_ticks") + pl.col("c_ret")).alias("event_score_rebuilt"))
)

row = t.filter(pl.col("date") == TARGET_DATE)
if row.height == 0:
    raise ValueError(f"No existe {TICKER} {TARGET_DATE} en {run_dir}")
r = row.to_dicts()[0]

pd_t = t.to_pandas()
pd_t["date"] = pd.to_datetime(pd_t["date"])
target_dt = pd.to_datetime(TARGET_DATE)
win = pd_t[(pd_t["date"] >= target_dt - pd.Timedelta(days=25)) &
           (pd_t["date"] <= target_dt + pd.Timedelta(days=25))].copy()

# Figura principal: componentes temporales + score
fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

axes[0].plot(win["date"], win["z_range"], label="z_range", linewidth=1.3)
axes[0].plot(win["date"], win["z_ticks"], label="z_ticks", linewidth=1.3)
axes[0].plot(win["date"], win["return_scale"], label="return_scale", linewidth=1.3)
axes[0].axvline(target_dt, linestyle="--", color="black", linewidth=1.0, label=f"target={TARGET_DATE}")
axes[0].set_title(f"{TICKER} | Componentes normalizados de event_score")
axes[0].legend(loc="best")
axes[0].grid(alpha=0.2)

axes[1].plot(win["date"], win["event_score"], label="event_score", linewidth=1.4)
axes[1].plot(win["date"], win["event_score_rebuilt"], label="rebuilt_score", linewidth=1.2, linestyle=":")
axes[1].axhline(TH_SCORE, linestyle="--", color="red", linewidth=1.0, label=f"threshold={TH_SCORE}")
axes[1].axvline(target_dt, linestyle="--", color="black", linewidth=1.0)
axes[1].scatter([target_dt], [float(r["event_score"])], color="crimson", s=45, zorder=5)
axes[1].set_title("event_score (original vs reconstruido)")
axes[1].legend(loc="best")
axes[1].grid(alpha=0.2)

fig.tight_layout()
plt.show()

# Figura de contribuciones del dia objetivo
labels = ["0.5*z_range", "0.3*z_ticks", "0.2*return_scale"]
vals = [float(r["c_range"]), float(r["c_ticks"]), float(r["c_ret"])]

plt.figure(figsize=(8, 4.5))
bars = plt.bar(labels, vals, color=["#1f77b4", "#ff7f0e", "#2ca02c"])
plt.axhline(0, color="black", linewidth=0.8)
plt.title(f"{TICKER} {TARGET_DATE} | Contribuciones al event_score")
plt.ylabel("contribution")
plt.grid(alpha=0.2, axis="y")
for b, v in zip(bars, vals):
    plt.text(b.get_x() + b.get_width()/2, b.get_height(), f"{v:.4f}", ha="center", va="bottom", fontsize=9)
plt.tight_layout()
plt.show()

orig = float(r["event_score"])
reb = float(r["event_score_rebuilt"])

print("=== STEP 16G - EVENT_SCORE DECOMPOSITION ===")
print(f"run_dir: {run_dir}")
print(f"ticker/date: {TICKER} {TARGET_DATE}")
print(f"z_range={float(r['z_range']):.6f}, z_ticks={float(r['z_ticks']):.6f}, return_scale={float(r['return_scale']):.6f}")
print(f"c_range={float(r['c_range']):.6f}, c_ticks={float(r['c_ticks']):.6f}, c_ret={float(r['c_ret']):.6f}")
print(f"event_score(original)={orig:.6f}")
print(f"event_score(rebuilt) ={reb:.6f}")
print(f"delta={abs(orig-reb):.12f}")
print(f"is_event_day={bool(r['is_event_day'])} (threshold={TH_SCORE})")
