# -*- coding: utf-8 -*-
# PASO 16I - PUMP/DUMP REGIME CLASSIFIER
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import polars as pl
from IPython.display import display, Markdown

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
LIFE_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"
OUT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle_regime"
OUT_DIR = OUT_ROOT / f"step16i_regime_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Reglas v1 (enfocadas a diferenciar dump real vs shakeout)
MIN_DD_10D_CONFIRMED = -0.30          # drawdown acumulado >= 30% en 10 dias
MAX_REBOUND_RATIO_CONFIRMED = 0.50     # no recuperar mas del 50% del drawdown
MIN_DD_5D_SHAKEOUT = -0.12             # shakeout minimo 12%
MAX_DD_10D_CONTINUATION = -0.10        # si no cae mucho, continuation

life_runs = sorted(LIFE_ROOT.glob("step16e_event_lifecycle_*"), key=lambda p: p.stat().st_mtime)
if not life_runs:
    raise FileNotFoundError("No hay step16e_event_lifecycle_*. Ejecuta Paso 16E primero.")
life_dir = life_runs[-1]
fp = life_dir / "step16e_event_lifecycle_scores.parquet"
if not fp.exists():
    raise FileNotFoundError(f"No existe {fp}")

base = pl.read_parquet(fp).sort(["ticker", "date"])
if base.height == 0:
    raise RuntimeError("step16e_event_lifecycle_scores vacio")

# solo anclas lifecycle
anchors = (
    base.filter(pl.col("is_lifecycle_event") == True)
        .select(["ticker", "date", "priority_bucket", "source_group", "event_lifecycle_score", "score_explosion", "score_decay"])
        .sort(["ticker", "date", "event_lifecycle_score"], descending=[False, False, True])
        .unique(subset=["ticker", "date"], keep="first")
        .sort(["event_lifecycle_score"], descending=True)
)
if anchors.height == 0:
    raise RuntimeError("No hay lifecycle events en la corrida actual")

pdf = base.to_pandas()
pdf["date"] = pd.to_datetime(pdf["date"])

rows = []
for a in anchors.to_dicts():
    ticker = a["ticker"]
    event_dt = pd.to_datetime(a["date"])

    t = pdf[pdf["ticker"] == ticker].sort_values("date").reset_index(drop=True).copy()
    if t.empty:
        continue

    # proxy precio acumulado
    t["price_idx"] = (1.0 + t["day_return"].fillna(0.0)).cumprod()

    # ?ndice del evento
    hit = t[t["date"] == event_dt]
    if hit.empty:
        continue
    i = int(hit.index[0])

    # ventana post-evento (positional)
    post = t.iloc[i+1:i+21].copy()
    if post.empty:
        continue

    p0 = float(t.iloc[i]["price_idx"])
    p10 = post.head(10)
    p20 = post.head(20)

    if p10.empty:
        continue

    min10 = float(p10["price_idx"].min())
    min5 = float(p10.head(5)["price_idx"].min()) if not p10.head(5).empty else min10
    dd10 = (min10 / p0) - 1.0
    dd5 = (min5 / p0) - 1.0

    # recuperaci?n desde el m?nimo (en 10d)
    after_min = p10[p10["price_idx"] >= min10]
    max_after_min = float(after_min["price_idx"].max()) if not after_min.empty else min10
    # rebound ratio respecto al drawdown
    dd_abs = abs(min10 - p0)
    rebound = (max_after_min - min10) / dd_abs if dd_abs > 1e-12 else 0.0

    # direccionalidad simple
    slope10 = float(np.polyfit(range(len(p10)), p10["price_idx"].values, 1)[0]) if len(p10) >= 2 else 0.0

    # clasificaci?n
    if (dd10 <= MIN_DD_10D_CONFIRMED) and (rebound <= MAX_REBOUND_RATIO_CONFIRMED):
        regime = "pump_then_dump_confirmed"
    elif (dd5 <= MIN_DD_5D_SHAKEOUT) and (dd10 > MIN_DD_10D_CONFIRMED):
        regime = "pump_then_shakeout"
    elif dd10 >= MAX_DD_10D_CONTINUATION:
        regime = "pump_continuation"
    else:
        # zona intermedia la aproximamos a shakeout por prudencia
        regime = "pump_then_shakeout"

    rows.append({
        "ticker": ticker,
        "event_date": str(a["date"]),
        "priority_bucket": a["priority_bucket"],
        "source_group": a["source_group"],
        "event_lifecycle_score": float(a["event_lifecycle_score"]),
        "score_explosion": float(a["score_explosion"]),
        "score_decay": float(a["score_decay"]),
        "dd5": float(dd5),
        "dd10": float(dd10),
        "rebound_ratio_10d": float(rebound),
        "slope10": float(slope10),
        "regime_label": regime,
    })

reg = pl.DataFrame(rows)
if reg.height == 0:
    raise RuntimeError("Paso 16I no produjo filas")

# Defensa final contra duplicados por clave evento
reg = (
    reg.sort(["ticker", "event_date", "event_lifecycle_score"], descending=[False, False, True])
       .unique(subset=["ticker", "event_date"], keep="first")
)

out_fp = OUT_DIR / "step16i_event_regime_labels.parquet"
reg.write_parquet(out_fp)

summary = (
    reg.group_by("regime_label")
       .agg([
           pl.len().alias("n_events"),
           pl.col("ticker").n_unique().alias("n_tickers"),
           pl.col("event_lifecycle_score").mean().alias("lifecycle_score_mean"),
           pl.col("dd10").median().alias("dd10_median"),
           pl.col("rebound_ratio_10d").median().alias("rebound_median"),
       ])
       .sort("n_events", descending=True)
)
summary_fp = OUT_DIR / "step16i_summary.parquet"
summary.write_parquet(summary_fp)

# merge-friendly table for downstream
joined = anchors.join(reg, on=["ticker"], how="left")
joined_fp = OUT_DIR / "step16i_event_regime_joined.parquet"
joined.write_parquet(joined_fp)

print("=== STEP 16I - PUMP/DUMP REGIME CLASSIFIER ===")
print(f"input_step16e: {fp}")
print(f"output_dir: {OUT_DIR}")
print(f"labels: {out_fp}")
print(f"summary: {summary_fp}")
print(f"joined: {joined_fp}")
print(f"n_events: {reg.height}")
print(f"n_tickers: {reg.select(pl.col('ticker').n_unique()).item()}")

# quick ACC diagnostic if present
acc = reg.filter(pl.col("ticker") == "ACC").sort("event_lifecycle_score", descending=True).head(5)
if acc.height > 0:
    print("ACC_sample:")
    print(acc.to_dict(as_series=False))

display(Markdown(f"### Step 16I output dir\n`{OUT_DIR}`"))
display(summary.to_pandas())
display(reg.sort("event_lifecycle_score", descending=True).head(30).to_pandas())
