# -*- coding: utf-8 -*-
# PASO 16L - SPLIT REVERSE ENGINEERING (iterative) + visual comparisons
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown, Image as IPyImage

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")

EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
OUT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "split_reverse_engineering"
OUT_DIR = OUT_ROOT / f"step16l_split_reverse_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR = OUT_DIR / "ticker_charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

SPLITS_FP = DATA_ROOT / "additional" / "corporate_actions" / "splits.parquet"
if not SPLITS_FP.exists():
    raise FileNotFoundError(f"No existe {SPLITS_FP}")

runs = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)
if not runs:
    raise FileNotFoundError("No hay step16_event_index_*")
run_dir = runs[-1]
EV_FP = run_dir / "step16_event_index_quotes_only.parquet"
if not EV_FP.exists():
    raise FileNotFoundError(f"No existe {EV_FP}")

# --------------------
# Load and prep
# --------------------
ev = pl.read_parquet(EV_FP)
sp = pl.read_parquet(SPLITS_FP)

# keep necessary cols, dedupe ticker-date by highest ticks
ev = (
    ev.select(["ticker", "date", "n_ticks", "mid_open", "mid_close", "mid_min", "mid_max", "day_return", "day_range", "event_score"])
      .sort(["ticker", "date", "n_ticks"], descending=[False, False, True])
      .unique(subset=["ticker", "date"], keep="first")
      .sort(["ticker", "date"])
)

ev_pd = ev.to_pandas()
ev_pd["date"] = pd.to_datetime(ev_pd["date"])

sp_pd = sp.select(["ticker", "execution_date", "split_from", "split_to"]).to_pandas()
sp_pd["execution_date"] = pd.to_datetime(sp_pd["execution_date"])
sp_pd["split_ratio"] = sp_pd["split_to"] / sp_pd["split_from"]
sp_pd["canon_factor"] = sp_pd["split_ratio"].apply(lambda x: max(x, 1.0/x) if x and x > 0 else np.nan)
sp_pd = sp_pd.dropna(subset=["canon_factor"]) 

# candidate factors from data (canonical > 1)
factors = sorted(sp_pd["canon_factor"].dropna().unique().tolist())
# sanitize huge list: keep plausible factors [1.05, 200]
factors = [f for f in factors if 1.01 <= f <= 200]
if 2.0 not in factors:
    factors.append(2.0)
if 3.0 not in factors:
    factors.append(3.0)
if 4.0 not in factors:
    factors.append(4.0)
factors = sorted(set(factors))


def nearest_factor(obs: float):
    arr = np.array(factors, dtype=float)
    idx = int(np.argmin(np.abs(arr - obs)))
    nf = float(arr[idx])
    rel_err = abs(obs - nf) / max(nf, 1e-9)
    return nf, rel_err

# --------------------
# Detect raw jumps
# --------------------
rows = []
for tk, g in ev_pd.groupby("ticker", sort=False):
    g = g.sort_values("date").copy()
    g["prev_close"] = g["mid_close"].shift(1)
    g = g.dropna(subset=["prev_close"])
    if g.empty:
        continue
    g["jump_ratio"] = g["mid_close"] / g["prev_close"]
    g["jump_factor_abs"] = g["jump_ratio"].apply(lambda x: max(x, 1.0/x) if x > 0 else np.nan)
    g["jump_dir"] = np.where(g["jump_ratio"] >= 1.0, "up", "down")

    # extreme jump candidates
    g["is_jump_extreme"] = (g["jump_factor_abs"] >= 1.5) | (g["day_range"].abs() >= 0.5) | (g["day_return"].abs() >= 0.5)
    for _, r in g.iterrows():
        if not bool(r["is_jump_extreme"]):
            continue
        nf, rel = nearest_factor(float(r["jump_factor_abs"]))
        rows.append({
            "ticker": tk,
            "event_date": r["date"].strftime("%Y-%m-%d"),
            "jump_ratio": float(r["jump_ratio"]),
            "jump_factor_abs": float(r["jump_factor_abs"]),
            "jump_dir": r["jump_dir"],
            "n_ticks": int(r["n_ticks"]),
            "day_return": float(r["day_return"]),
            "day_range": float(r["day_range"]),
            "nearest_split_factor": nf,
            "factor_rel_error": rel,
            "split_like": bool(rel <= 0.12),
        })

jumps = pd.DataFrame(rows)
if jumps.empty:
    raise RuntimeError("No se detectaron jumps extremos")

# --------------------
# Match official splits around detected jumps
# --------------------
sp_lookup = sp_pd.copy()
sp_lookup["split_date"] = sp_lookup["execution_date"].dt.strftime("%Y-%m-%d")

# exact / near windows
jumps["event_dt"] = pd.to_datetime(jumps["event_date"])

match_rows = []
for _, r in jumps.iterrows():
    tk = r["ticker"]
    dt = r["event_dt"]
    sub = sp_lookup[sp_lookup["ticker"] == tk]
    if sub.empty:
        match_rows.append({"match_exact": False, "match_near_3d": False, "match_near_5d": False, "official_factor": np.nan, "official_date": None})
        continue
    sub = sub.copy()
    sub["abs_days"] = (sub["execution_date"] - dt).abs().dt.days
    near = sub.sort_values("abs_days").head(1)
    if near.empty:
        match_rows.append({"match_exact": False, "match_near_3d": False, "match_near_5d": False, "official_factor": np.nan, "official_date": None})
        continue
    n = near.iloc[0]
    d = int(n["abs_days"])
    match_rows.append({
        "match_exact": bool(d == 0),
        "match_near_3d": bool(d <= 3),
        "match_near_5d": bool(d <= 5),
        "official_factor": float(n["canon_factor"]),
        "official_date": n["execution_date"].strftime("%Y-%m-%d"),
    })

match_df = pd.DataFrame(match_rows)
jumps = pd.concat([jumps.reset_index(drop=True), match_df], axis=1)

# class
jumps["jump_class"] = np.where(
    jumps["match_near_5d"],
    "split_matched",
    np.where(jumps["split_like"], "split_like_unmatched", "extreme_non_split")
)

# --------------------
# Iterative mathematical adjustment per ticker
# --------------------
MAX_ITER = 6
iter_events = []
adj_series_rows = []

for tk, g in ev_pd.groupby("ticker", sort=False):
    g = g.sort_values("date").copy()
    g = g.reset_index(drop=True)
    g["adj_close"] = g["mid_close"].astype(float)

    for it in range(1, MAX_ITER + 1):
        g["adj_prev"] = g["adj_close"].shift(1)
        tmp = g.dropna(subset=["adj_prev"]).copy()
        if tmp.empty:
            break
        tmp["adj_ratio"] = tmp["adj_close"] / tmp["adj_prev"]
        tmp["adj_factor_abs"] = tmp["adj_ratio"].apply(lambda x: max(x, 1.0/x) if x > 0 else np.nan)
        tmp["nearest_factor"] = tmp["adj_factor_abs"].apply(lambda x: nearest_factor(float(x))[0])
        tmp["factor_rel_err"] = (tmp["adj_factor_abs"] - tmp["nearest_factor"]).abs() / tmp["nearest_factor"].replace(0, np.nan)
        tmp = tmp[(tmp["adj_factor_abs"] >= 1.5) & (tmp["factor_rel_err"] <= 0.12)]
        if tmp.empty:
            break

        # pick strongest unresolved jump
        pick = tmp.sort_values("adj_factor_abs", ascending=False).iloc[0]
        i = int(pick.name)
        f = float(pick["nearest_factor"])
        ratio = float(pick["adj_ratio"])

        # if down jump (ratio<1): forward split -> multiply future by f
        # if up jump (ratio>1): reverse split -> divide future by f
        if ratio < 1.0:
            g.loc[i:, "adj_close"] = g.loc[i:, "adj_close"] * f
            action = "multiply_future"
        else:
            g.loc[i:, "adj_close"] = g.loc[i:, "adj_close"] / f
            action = "divide_future"

        iter_events.append({
            "ticker": tk,
            "iter": it,
            "event_date": g.loc[i, "date"].strftime("%Y-%m-%d"),
            "adj_ratio": ratio,
            "applied_factor": f,
            "action": action,
        })

    # store adjusted series
    for _, r in g.iterrows():
        adj_series_rows.append({
            "ticker": tk,
            "date": r["date"].strftime("%Y-%m-%d"),
            "mid_close_raw": float(r["mid_close"]),
            "mid_close_adjusted": float(r["adj_close"]),
        })

adj_events = pd.DataFrame(iter_events)
adj_series = pd.DataFrame(adj_series_rows)

# --------------------
# Save tables
# --------------------
jumps_pl = pl.from_pandas(jumps)
adj_events_pl = pl.from_pandas(adj_events) if not adj_events.empty else pl.DataFrame({"ticker":[],"iter":[],"event_date":[],"adj_ratio":[],"applied_factor":[],"action":[]})
adj_series_pl = pl.from_pandas(adj_series)

jumps_fp = OUT_DIR / "step16l_jumps_labeled.parquet"
adj_events_fp = OUT_DIR / "step16l_iterative_adjustments.parquet"
adj_series_fp = OUT_DIR / "step16l_adjusted_series.parquet"

jumps_pl.write_parquet(jumps_fp)
adj_events_pl.write_parquet(adj_events_fp)
adj_series_pl.write_parquet(adj_series_fp)

jumps_pl.write_csv(OUT_DIR / "step16l_jumps_labeled.csv")
if not adj_events.empty:
    adj_events_pl.write_csv(OUT_DIR / "step16l_iterative_adjustments.csv")
adj_series_pl.write_csv(OUT_DIR / "step16l_adjusted_series.csv")

# --------------------
# Visuals
# --------------------
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# hist factors by class (zoom-friendly): log-x + geometric bins
max_factor = float(max(2.0, jumps["jump_factor_abs"].max()))
min_factor = 1.0
bins_geo = np.geomspace(min_factor, max_factor, 45)
for cls, color in [("split_matched", "#4f7ea8"), ("split_like_unmatched", "#ff7f0e"), ("extreme_non_split", "#d62728")]:
    s = jumps[jumps["jump_class"] == cls]["jump_factor_abs"]
    if len(s):
        axes[0,0].hist(s, bins=bins_geo, alpha=0.6, label=cls, color=color)
axes[0,0].set_xscale("log")
axes[0,0].set_title("jump_factor_abs by class (log x)")
axes[0,0].set_xlabel("jump_factor_abs (log scale)")
axes[0,0].legend(loc="best")
axes[0,0].grid(alpha=0.2)

# scatter observed vs nearest
sample_sc = jumps.copy()
axes[0,1].scatter(sample_sc["jump_factor_abs"], sample_sc["nearest_split_factor"], s=20, alpha=0.65)
mn = min(sample_sc["jump_factor_abs"].min(), sample_sc["nearest_split_factor"].min())
mx = max(sample_sc["jump_factor_abs"].max(), sample_sc["nearest_split_factor"].max())
axes[0,1].plot([mn, mx], [mn, mx], "r--", lw=1.0)
axes[0,1].set_title("observed factor vs nearest split factor")
axes[0,1].set_xlabel("observed")
axes[0,1].set_ylabel("nearest")
axes[0,1].grid(alpha=0.2)

# counts
cnt = jumps.groupby("jump_class").size().reset_index(name="n")
axes[1,0].bar(cnt["jump_class"], cnt["n"], color=["#4f7ea8", "#ff7f0e", "#d62728"][:len(cnt)])
axes[1,0].set_title("count by jump_class")
axes[1,0].tick_params(axis="x", rotation=15)
axes[1,0].grid(alpha=0.2)

# relative error histogram
axes[1,1].hist(jumps["factor_rel_error"].clip(upper=1.0), bins=40, color="#2ca02c", alpha=0.85)
axes[1,1].set_title("factor relative error (capped 1.0)")
axes[1,1].grid(alpha=0.2)

fig.tight_layout()
global_fp = OUT_DIR / "step16l_global_comparison.png"
fig.savefig(global_fp, dpi=140)
plt.close(fig)

# per-ticker charts for top priority dates/tickers
top_tickers = (
    jumps.sort_values(["jump_factor_abs", "factor_rel_error"], ascending=[False, True])
         .drop_duplicates(["ticker"]) 
         .head(12)["ticker"].tolist()
)

for tk in top_tickers:
    raw = ev_pd[ev_pd["ticker"] == tk].copy().sort_values("date")
    adj = adj_series[adj_series["ticker"] == tk].copy().sort_values("date")
    if raw.empty or adj.empty:
        continue
    raw["date"] = pd.to_datetime(raw["date"])
    adj["date"] = pd.to_datetime(adj["date"])

    tk_jumps = jumps[jumps["ticker"] == tk].copy()
    tk_jumps["event_dt"] = pd.to_datetime(tk_jumps["event_date"])

    tk_splits = sp_pd[sp_pd["ticker"] == tk].copy()

    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)

    axes[0].plot(raw["date"], raw["mid_close"], lw=1.2, label="mid_close_raw")
    axes[0].plot(adj["date"], adj["mid_close_adjusted"], lw=1.2, linestyle=":", label="mid_close_adjusted")

    for _, r in tk_jumps.head(8).iterrows():
        c = "#ff7f0e" if r["jump_class"] == "split_like_unmatched" else ("#4f7ea8" if r["jump_class"] == "split_matched" else "#d62728")
        axes[0].axvline(r["event_dt"], color=c, ls="--", lw=0.8, alpha=0.8)

    for _, srow in tk_splits.iterrows():
        axes[0].axvline(srow["execution_date"], color="green", ls="-.", lw=0.9, alpha=0.7)

    axes[0].set_title(f"{tk} | raw vs adjusted + split/jump markers")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    axes[1].bar(raw["date"], raw["day_return"], width=1.0, color=["#d62728" if x < 0 else "#4f7ea8" for x in raw["day_return"]])
    axes[1].axhline(0, c="black", lw=0.8)
    axes[1].set_title("day_return")
    axes[1].grid(alpha=0.2)

    fig.tight_layout()
    out_fp = CHARTS_DIR / f"{tk}_split_reverse_chart.png"
    fig.savefig(out_fp, dpi=130)
    plt.close(fig)

# --------------------
# Output
# --------------------
n_matched = int((jumps["jump_class"] == "split_matched").sum())
n_split_like = int((jumps["jump_class"] == "split_like_unmatched").sum())
n_non_split = int((jumps["jump_class"] == "extreme_non_split").sum())

print("=== STEP 16L - SPLIT REVERSE ENGINEERING ===")
print(f"splits_source: {SPLITS_FP}")
print(f"event_source: {EV_FP}")
print(f"output_dir: {OUT_DIR}")
print(f"n_rows_event_dedup: {len(ev_pd)}")
print(f"n_extreme_jumps: {len(jumps)}")
print(f"split_matched: {n_matched}")
print(f"split_like_unmatched: {n_split_like}")
print(f"extreme_non_split: {n_non_split}")
print(f"global_chart: {global_fp}")
print(f"ticker_charts_dir: {CHARTS_DIR}")

# visual-only in notebook
display(Markdown(f"### Step 16L output dir`{OUT_DIR}`"))
display(Markdown("#### Comparativa global (splits data vs tick moves extremos)"))
display(IPyImage(filename=str(global_fp)))

# Widget ticker review (sin listado fijo)
all_tickers = sorted(jumps["ticker"].dropna().unique().tolist())

def _render_ticker_chart(tk: str):
    raw = ev_pd[ev_pd["ticker"] == tk].copy().sort_values("date")
    adj = adj_series[adj_series["ticker"] == tk].copy().sort_values("date")
    if raw.empty or adj.empty:
        display(Markdown(f"Sin datos para `{tk}`"))
        return

    raw["date"] = pd.to_datetime(raw["date"])
    adj["date"] = pd.to_datetime(adj["date"])
    tk_jumps = jumps[jumps["ticker"] == tk].copy()
    tk_jumps["event_dt"] = pd.to_datetime(tk_jumps["event_date"])
    tk_splits = sp_pd[sp_pd["ticker"] == tk].copy()

    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    axes[0].plot(raw["date"], raw["mid_close"], lw=1.2, label="mid_close_raw")
    axes[0].plot(adj["date"], adj["mid_close_adjusted"], lw=1.2, linestyle=":", label="mid_close_adjusted")

    for _, r in tk_jumps.iterrows():
        c = "#ff7f0e" if r["jump_class"] == "split_like_unmatched" else ("#4f7ea8" if r["jump_class"] == "split_matched" else "#d62728")
        axes[0].axvline(r["event_dt"], color=c, ls="--", lw=0.8, alpha=0.8)

    for _, srow in tk_splits.iterrows():
        axes[0].axvline(srow["execution_date"], color="green", ls="-.", lw=0.9, alpha=0.7)

    axes[0].set_title(f"{tk} | raw vs adjusted + split/jump markers")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    axes[1].bar(raw["date"], raw["day_return"], width=1.0, color=["#d62728" if x < 0 else "#4f7ea8" for x in raw["day_return"]])
    axes[1].axhline(0, c="black", lw=0.8)
    axes[1].set_title("day_return")
    axes[1].grid(alpha=0.2)

    fig.tight_layout()
    plt.show()

    c = tk_jumps["jump_class"].value_counts().to_dict()
    display(Markdown(
        f"**{tk}**  \
"
        f"split_matched={c.get('split_matched', 0)} | "
        f"split_like_unmatched={c.get('split_like_unmatched', 0)} | "
        f"extreme_non_split={c.get('extreme_non_split', 0)}"
    ))

if len(all_tickers) == 0:
    display(Markdown("No hay tickers extremos para revisar."))
elif widgets is None:
    display(Markdown("ipywidgets no disponible; render de un ticker ejemplo."))
    _render_ticker_chart(all_tickers[0])
else:
    ticker_dd = widgets.Dropdown(options=all_tickers, value=all_tickers[0], description="Ticker")
    btn = widgets.Button(description="Render")
    out = widgets.Output()

    def _run(_):
        with out:
            out.clear_output(wait=True)
            _render_ticker_chart(ticker_dd.value)

    btn.on_click(_run)

    display(Markdown("#### Revision interactiva por ticker"))
    display(widgets.HBox([ticker_dd, btn]))
    display(out)
    with out:
        _render_ticker_chart(ticker_dd.value)
