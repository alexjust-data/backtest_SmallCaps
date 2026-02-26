# -*- coding: utf-8 -*-
# PASO 16G-W - EVENT_SCORE DECOMPOSITION (WIDGET)
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

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

base = (
    df.with_columns([
        (W_RANGE * pl.col("z_range")).alias("c_range"),
        (W_TICKS * pl.col("z_ticks")).alias("c_ticks"),
        (W_RET * pl.col("return_scale")).alias("c_ret"),
    ])
    .with_columns((pl.col("c_range") + pl.col("c_ticks") + pl.col("c_ret")).alias("event_score_rebuilt"))
)

all_tickers = sorted(base["ticker"].unique().to_list())

def available_dates(ticker: str):
    return (
        base.filter(pl.col("ticker") == ticker)
            .sort("date")
            .select("date")
            .to_series()
            .to_list()
    )

def render_case(ticker: str, target_date: str, tail_days: int = 50):
    t = base.filter(pl.col("ticker") == ticker).sort("date")
    row = t.filter(pl.col("date") == target_date)
    if row.height == 0:
        display(Markdown(f"No existe `{ticker}` `{target_date}` en run `{run_dir.name}`"))
        return

    r = row.to_dicts()[0]
    pd_t = t.to_pandas()
    pd_t["date"] = pd.to_datetime(pd_t["date"])
    target_dt = pd.to_datetime(target_date)
    win = pd_t[(pd_t["date"] >= target_dt - pd.Timedelta(days=tail_days)) &
               (pd_t["date"] <= target_dt + pd.Timedelta(days=tail_days))].copy()

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    axes[0].plot(win["date"], win["z_range"], label="z_range", linewidth=1.3)
    axes[0].plot(win["date"], win["z_ticks"], label="z_ticks", linewidth=1.3)
    axes[0].plot(win["date"], win["return_scale"], label="return_scale", linewidth=1.3)
    axes[0].axvline(target_dt, linestyle="--", color="black", linewidth=1.0, label=f"target={target_date}")
    axes[0].set_title(f"{ticker} | Componentes normalizados")
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

    labels = ["0.5*z_range", "0.3*z_ticks", "0.2*return_scale"]
    vals = [float(r["c_range"]), float(r["c_ticks"]), float(r["c_ret"])]

    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(labels, vals, color=["#1f77b4", "#ff7f0e", "#2ca02c"])
    plt.axhline(0, color="black", linewidth=0.8)
    plt.title(f"{ticker} {target_date} | Contribuciones a event_score")
    plt.ylabel("contribution")
    plt.grid(alpha=0.2, axis="y")
    for b, v in zip(bars, vals):
        plt.text(b.get_x() + b.get_width()/2, b.get_height(), f"{v:.4f}", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.show()

    orig = float(r["event_score"])
    reb = float(r["event_score_rebuilt"])
    display(Markdown(
        f"**{ticker} {target_date}**  \
"
        f"event_score={orig:.6f} | rebuilt={reb:.6f} | delta={abs(orig-reb):.12f}  \
"
        f"is_event_day={bool(r['is_event_day'])} (threshold={TH_SCORE})"
    ))

if widgets is None:
    tk = all_tickers[0]
    dt = available_dates(tk)[0]
    render_case(tk, dt, tail_days=50)
else:
    ticker_dd = widgets.Dropdown(options=all_tickers, value=("AABA" if "AABA" in all_tickers else all_tickers[0]), description="Ticker")
    date_dd = widgets.Dropdown(options=available_dates(ticker_dd.value), description="Date")
    tail_slider = widgets.IntSlider(value=50, min=20, max=200, step=5, description="Window")
    btn = widgets.Button(description="Render")
    out = widgets.Output()

    def _refresh_dates(*_):
        d = available_dates(ticker_dd.value)
        date_dd.options = d
        if "2019-01-04" in d and ticker_dd.value == "AABA":
            date_dd.value = "2019-01-04"
        else:
            date_dd.value = d[-1] if d else None

    def _run(_):
        with out:
            out.clear_output(wait=True)
            if date_dd.value is None:
                print("Sin fechas para este ticker")
                return
            render_case(ticker_dd.value, date_dd.value, tail_days=tail_slider.value)

    ticker_dd.observe(_refresh_dates, names="value")
    btn.on_click(_run)

    display(Markdown(f"### Paso 16G-W | run `{run_dir.name}`"))
    display(widgets.HBox([ticker_dd, date_dd, tail_slider, btn]))
    display(out)
    with out:
        render_case(ticker_dd.value, date_dd.value, tail_days=tail_slider.value)
