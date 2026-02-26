# -*- coding: utf-8 -*-
# PASO 16K - SPLITS VS EXTREMES AUDIT + CHARTS
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown, Image as IPyImage

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")

SPLITS_FP = DATA_ROOT / "additional" / "corporate_actions" / "splits.parquet"
if not SPLITS_FP.exists():
    raise FileNotFoundError(f"No existe {SPLITS_FP}")

# latest step16
EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
runs = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)
if not runs:
    raise FileNotFoundError("No hay step16_event_index_*")
run_dir = runs[-1]
EVENT_FP = run_dir / "step16_event_index_quotes_only.parquet"
if not EVENT_FP.exists():
    raise FileNotFoundError(f"No existe {EVENT_FP}")

OUT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "outlier_audit"
OUT_DIR = OUT_ROOT / f"step16k_split_extreme_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR = OUT_DIR / "non_error_charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load data
# -----------------------------
ev = pl.read_parquet(EVENT_FP)
sp = pl.read_parquet(SPLITS_FP)

# normalize dates
sp = sp.with_columns(pl.col("execution_date").cast(pl.Utf8).alias("split_date"))
ev = ev.with_columns(pl.col("date").cast(pl.Utf8).alias("event_date"))

# split ratio
sp = sp.with_columns((pl.col("split_to") / pl.col("split_from")).alias("split_ratio"))

# -----------------------------
# Extreme definitions
# -----------------------------
ev = ev.with_columns([
    pl.col("day_return").abs().alias("abs_return"),
    pl.col("day_range").abs().alias("abs_range"),
])

q_ret_995 = ev.select(pl.col("abs_return").quantile(0.995)).item()
q_rng_995 = ev.select(pl.col("abs_range").quantile(0.995)).item()
q_scr_995 = ev.select(pl.col("event_score").quantile(0.995)).item()

TH_RET = float(max(0.25, q_ret_995))
TH_RNG = float(max(0.30, q_rng_995))
TH_SCR = float(q_scr_995)

ev = ev.with_columns([
    (pl.col("abs_return") >= TH_RET).alias("extreme_return"),
    (pl.col("abs_range") >= TH_RNG).alias("extreme_range"),
    (pl.col("event_score") >= TH_SCR).alias("extreme_score"),
])
ev = ev.with_columns((pl.col("extreme_return") | pl.col("extreme_range") | pl.col("extreme_score")).alias("is_extreme"))

ext = ev.filter(pl.col("is_extreme") == True)
if ext.height == 0:
    raise RuntimeError("No hay extremos con umbrales actuales")

# -----------------------------
# Join splits (exact and +/-1 day)
# -----------------------------
# exact
sp_exact = sp.select(["ticker", "split_date", "split_ratio"]).rename({"split_date":"event_date"})
ext = ext.join(sp_exact, on=["ticker", "event_date"], how="left")
ext = ext.with_columns(pl.col("split_ratio").is_not_null().alias("split_exact"))

# near +/-1 day
sp_pd = sp.select(["ticker", "split_date", "split_ratio"]).to_pandas()
sp_pd["split_date"] = pd.to_datetime(sp_pd["split_date"])
rows_near = []
for _, r in sp_pd.iterrows():
    for d in [-1, 1]:
        rows_near.append({
            "ticker": r["ticker"],
            "event_date": (r["split_date"] + pd.Timedelta(days=d)).strftime("%Y-%m-%d"),
            "split_ratio_near": float(r["split_ratio"]),
        })
sp_near = pl.from_pandas(pd.DataFrame(rows_near)) if rows_near else pl.DataFrame({"ticker":[],"event_date":[],"split_ratio_near":[]})

ext = ext.join(sp_near, on=["ticker", "event_date"], how="left")
ext = ext.with_columns([
    pl.col("split_ratio_near").is_not_null().alias("split_near_1d"),
    (pl.col("split_exact") | pl.col("split_ratio_near").is_not_null()).alias("split_related"),
])

# -----------------------------
# Error heuristic among non-split extremes
# -----------------------------
ext = ext.with_columns([
    (
        ((pl.col("abs_return") > 1.0) | (pl.col("abs_range") > 1.5))
        & (pl.col("n_ticks") < 300)
    ).alias("likely_data_error")
])

ext = ext.with_columns([
    pl.when(pl.col("split_related")).then(pl.lit("split_related"))
    .when(pl.col("likely_data_error")).then(pl.lit("non_split_likely_error"))
    .otherwise(pl.lit("non_split_candidate_event")).alias("extreme_class")
])

# Save table
ext_fp = OUT_DIR / "step16k_extremes_labeled.parquet"
ext.write_parquet(ext_fp)

# -----------------------------
# Summary tables
# -----------------------------
summary = ext.group_by("extreme_class").agg([
    pl.len().alias("n_rows"),
    pl.col("ticker").n_unique().alias("n_tickers"),
    pl.col("abs_return").median().alias("abs_return_med"),
    pl.col("abs_range").median().alias("abs_range_med"),
]).sort("n_rows", descending=True)

by_bucket = ext.group_by(["priority_bucket", "extreme_class"]).agg(pl.len().alias("n")).sort(["priority_bucket", "n"], descending=[False, True])

summary_fp = OUT_DIR / "step16k_summary.parquet"
summary.write_parquet(summary_fp)
summary.write_csv(OUT_DIR / "step16k_summary.csv")
by_bucket.write_csv(OUT_DIR / "step16k_by_bucket.csv")

# -----------------------------
# Global charts
# -----------------------------
pd_ext = ext.select([
    "ticker", "event_date", "abs_return", "abs_range", "event_score", "extreme_class"
]).to_pandas()

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Hist abs_return
for cls, color in [("split_related", "#4f7ea8"), ("non_split_likely_error", "#d62728"), ("non_split_candidate_event", "#2ca02c")]:
    s = pd_ext[pd_ext["extreme_class"] == cls]["abs_return"]
    if len(s):
        axes[0,0].hist(s, bins=40, alpha=0.6, label=cls, color=color)
axes[0,0].axvline(TH_RET, ls="--", c="black", lw=1.0, label=f"TH_RET={TH_RET:.3f}")
axes[0,0].set_title("abs_return extremes")
axes[0,0].legend(loc="best")
axes[0,0].grid(alpha=0.2)

# Hist abs_range
for cls, color in [("split_related", "#4f7ea8"), ("non_split_likely_error", "#d62728"), ("non_split_candidate_event", "#2ca02c")]:
    s = pd_ext[pd_ext["extreme_class"] == cls]["abs_range"]
    if len(s):
        axes[0,1].hist(s, bins=40, alpha=0.6, label=cls, color=color)
axes[0,1].axvline(TH_RNG, ls="--", c="black", lw=1.0, label=f"TH_RNG={TH_RNG:.3f}")
axes[0,1].set_title("abs_range extremes")
axes[0,1].legend(loc="best")
axes[0,1].grid(alpha=0.2)

# Histogram event score
for cls, color in [("split_related", "#4f7ea8"), ("non_split_likely_error", "#d62728"), ("non_split_candidate_event", "#2ca02c")]:
    s = pd_ext[pd_ext["extreme_class"] == cls]["event_score"]
    if len(s):
        axes[1,0].hist(s, bins=40, alpha=0.6, label=cls, color=color)
axes[1,0].axvline(TH_SCR, ls="--", c="black", lw=1.0, label=f"TH_SCR={TH_SCR:.3f}")
axes[1,0].set_title("event_score extremes")
axes[1,0].legend(loc="best")
axes[1,0].grid(alpha=0.2)

# Bar counts
cnt = pd_ext.groupby("extreme_class").size().reset_index(name="n")
axes[1,1].bar(cnt["extreme_class"], cnt["n"], color=["#4f7ea8", "#d62728", "#2ca02c"][:len(cnt)])
axes[1,1].set_title("Count by extreme_class")
axes[1,1].tick_params(axis="x", rotation=15)
axes[1,1].grid(alpha=0.2)

fig.tight_layout()
global_fp = OUT_DIR / "step16k_global_histograms.png"
fig.savefig(global_fp, dpi=140)
plt.close(fig)

# -----------------------------
# Charts for non-error non-split candidates
# -----------------------------
cand = (
    ext.filter(pl.col("extreme_class") == "non_split_candidate_event")
       .sort(["event_score", "abs_range", "abs_return"], descending=True)
       .select(["ticker", "event_date", "event_score", "abs_return", "abs_range"])
       .head(30)
)

cand_fp = OUT_DIR / "step16k_non_split_candidate_events.parquet"
cand.write_parquet(cand_fp)

base_pd = ev.select(["ticker", "event_date", "day_return", "day_range", "event_score"]).to_pandas()
base_pd["event_date"] = pd.to_datetime(base_pd["event_date"])

for r in cand.to_dicts():
    tk = r["ticker"]
    dt = pd.to_datetime(r["event_date"])
    w = base_pd[(base_pd["ticker"] == tk) & (base_pd["event_date"] >= dt - pd.Timedelta(days=20)) & (base_pd["event_date"] <= dt + pd.Timedelta(days=20))].copy()
    if w.empty:
        continue

    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
    axes[0].plot(w["event_date"], w["event_score"], lw=1.3, label="event_score")
    axes[0].axvline(dt, ls="--", c="red", lw=1.1, label=f"extreme={dt.date()}")
    axes[0].set_title(f"{tk} | non_split_candidate_event")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    axes[1].plot(w["event_date"], w["day_range"], lw=1.3, color="#1f77b4", label="day_range")
    axes[1].axvline(dt, ls="--", c="red", lw=1.1)
    axes[1].grid(alpha=0.2)

    axes[2].bar(w["event_date"], w["day_return"], width=1.0, color=["#d62728" if x < 0 else "#4f7ea8" for x in w["day_return"]])
    axes[2].axhline(0, c="black", lw=0.8)
    axes[2].axvline(dt, ls="--", c="red", lw=1.1)
    axes[2].grid(alpha=0.2)

    fig.tight_layout()
    chart_fp = CHARTS_DIR / f"{tk}_{dt.strftime('%Y%m%d')}_candidate.png"
    fig.savefig(chart_fp, dpi=130)
    plt.close(fig)

# -----------------------------
# Console + notebook output
# -----------------------------
print("=== STEP 16K - SPLITS VS EXTREMES AUDIT ===")
print(f"splits_source: {SPLITS_FP}")
print(f"event_source: {EVENT_FP}")
print(f"output_dir: {OUT_DIR}")
print(f"thresholds -> TH_RET={TH_RET:.6f}, TH_RNG={TH_RNG:.6f}, TH_SCR={TH_SCR:.6f}")
print(f"n_event_rows: {ev.height}")
print(f"n_extremes: {ext.height}")
print(f"n_split_related: {ext.filter(pl.col('extreme_class')=='split_related').height}")
print(f"n_non_split_likely_error: {ext.filter(pl.col('extreme_class')=='non_split_likely_error').height}")
print(f"n_non_split_candidate_event: {ext.filter(pl.col('extreme_class')=='non_split_candidate_event').height}")
print(f"global_chart: {global_fp}")
print(f"candidate_charts_dir: {CHARTS_DIR}")

display(Markdown(f"### Step 16K output dir\n`{OUT_DIR}`"))

display(Markdown("#### Histograma global (splits vs extremos no-split)"))
display(IPyImage(filename=str(global_fp)))

# Widget: revisi?n uno-por-uno de candidatos no-split
chart_files = sorted(CHARTS_DIR.glob("*.png"))

# Resumen visual r?pido (sin tabla): barra de conteos por clase
cnt = ext.group_by("extreme_class").agg(pl.len().alias("n")).sort("n", descending=True).to_pandas()
plt.figure(figsize=(7,4))
plt.bar(cnt["extreme_class"], cnt["n"], color=["#2ca02c", "#4f7ea8", "#d62728"][:len(cnt)])
plt.title("Extremos por clase")
plt.ylabel("n")
plt.xticks(rotation=15)
plt.grid(alpha=0.2)
plt.tight_layout()
bar_fp = OUT_DIR / "step16k_counts_by_class.png"
plt.savefig(bar_fp, dpi=140)
plt.show()
plt.close()

if len(chart_files) == 0:
    display(Markdown("No se generaron charts de candidatos no-split."))
else:
    try:
        import ipywidgets as widgets
    except Exception:
        widgets = None

    display(Markdown("#### Revisi?n de candidatos no-split (uno por uno)"))

    if widgets is None:
        display(Markdown("ipywidgets no disponible. Mostrando solo el primer caso."))
        p0 = chart_files[0]
        display(Markdown(f"**1/{len(chart_files)} | {p0.name}**"))
        display(IPyImage(filename=str(p0)))
    else:
        options = [(p.name, i) for i, p in enumerate(chart_files)]
        dd = widgets.Dropdown(options=options, value=0, description="Caso")
        prev_btn = widgets.Button(description="Prev")
        next_btn = widgets.Button(description="Next")
        render_btn = widgets.Button(description="Render")
        out = widgets.Output()

        def _render_idx(i: int):
            i = max(0, min(i, len(chart_files)-1))
            dd.value = i
            with out:
                out.clear_output(wait=True)
                psel = chart_files[i]
                display(Markdown(f"**{i+1}/{len(chart_files)} | {psel.name}**"))
                display(IPyImage(filename=str(psel)))

        def _on_prev(_):
            _render_idx(int(dd.value) - 1)

        def _on_next(_):
            _render_idx(int(dd.value) + 1)

        def _on_render(_):
            _render_idx(int(dd.value))

        def _on_dd_change(change):
            if change.get("name") == "value":
                _render_idx(int(change["new"]))

        prev_btn.on_click(_on_prev)
        next_btn.on_click(_on_next)
        render_btn.on_click(_on_render)
        dd.observe(_on_dd_change, names="value")

        display(widgets.HBox([dd, prev_btn, next_btn, render_btn]))
        display(out)
        _render_idx(0)
