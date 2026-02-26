# -*- coding: utf-8 -*-
# PASO 16B - WIDGET VISUAL DE IS_EVENT_DAY (TICKER + MULTI)
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
from IPython.display import display, Markdown

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"

run_dirs = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)
if not run_dirs:
    raise FileNotFoundError("No hay salidas step16_event_index_*. Ejecuta Paso 16 primero.")
run_dir = run_dirs[-1]
fp = run_dir / "step16_event_index_quotes_only.parquet"
if not fp.exists():
    raise FileNotFoundError(f"No existe {fp}")

df = pl.read_parquet(fp)
if df.height == 0:
    raise RuntimeError("event_index vac?o")

# Umbrales usados en Paso 16
TH_SCORE = 2.0
TH_RANGE = 0.12
TH_RET = 0.15

# Causa explicita de activacion
plot_df = (
    df.with_columns([
        (pl.col("event_score") >= TH_SCORE).alias("trigger_score"),
        (pl.col("day_range") >= TH_RANGE).alias("trigger_range"),
        (pl.col("day_return").abs() >= TH_RET).alias("trigger_return"),
    ])
    .with_columns([
        (
            pl.when(pl.col("trigger_score") & ~pl.col("trigger_range") & ~pl.col("trigger_return")).then(pl.lit("score_only"))
            .when(~pl.col("trigger_score") & pl.col("trigger_range") & ~pl.col("trigger_return")).then(pl.lit("range_only"))
            .when(~pl.col("trigger_score") & ~pl.col("trigger_range") & pl.col("trigger_return")).then(pl.lit("return_only"))
            .when(pl.col("trigger_score") & pl.col("trigger_range") & ~pl.col("trigger_return")).then(pl.lit("score+range"))
            .when(pl.col("trigger_score") & ~pl.col("trigger_range") & pl.col("trigger_return")).then(pl.lit("score+return"))
            .when(~pl.col("trigger_score") & pl.col("trigger_range") & pl.col("trigger_return")).then(pl.lit("range+return"))
            .when(pl.col("trigger_score") & pl.col("trigger_range") & pl.col("trigger_return")).then(pl.lit("all_three"))
            .otherwise(pl.lit("none"))
        ).alias("trigger_cause")
    ])
)

# tickers ordenados por numero de event days
ticker_rank = (
    plot_df.group_by("ticker")
    .agg([
        pl.len().alias("n_days"),
        pl.col("is_event_day").sum().alias("n_event_days"),
        pl.col("event_score").max().alias("max_event_score"),
        pl.col("priority_bucket").first().alias("priority_bucket"),
    ])
    .sort(["n_event_days", "max_event_score"], descending=True)
)

all_tickers = ticker_rank["ticker"].to_list()

def render_single_ticker(ticker: str, tail_days: int = 260):
    t = (
        plot_df.filter(pl.col("ticker") == ticker)
        .sort("date")
        .tail(tail_days)
    )
    if t.height == 0:
        display(Markdown(f"Sin datos para `{ticker}`"))
        return

    pd = t.to_pandas()
    meta = ticker_rank.filter(pl.col("ticker") == ticker).to_dicts()[0]

    display(Markdown(
        f"### {ticker} | bucket={meta['priority_bucket']} | n_days={meta['n_days']} | n_event_days={meta['n_event_days']}"
    ))

    fig, axes = plt.subplots(3, 1, figsize=(14, 9), sharex=True)

    # 1) Event score
    axes[0].plot(pd["date"], pd["event_score"], linewidth=1.2, label="event_score")
    axes[0].axhline(TH_SCORE, linestyle="--", linewidth=1.0, color="red", label=f"score_th={TH_SCORE}")
    ev = pd[pd["is_event_day"] == True]
    axes[0].scatter(ev["date"], ev["event_score"], s=18, alpha=0.8, color="#d62728", label="is_event_day")
    axes[0].set_title(f"{ticker} | event_score y d?as marcados")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    # 2) day_range
    axes[1].plot(pd["date"], pd["day_range"], linewidth=1.2, color="#1f77b4", label="day_range")
    axes[1].axhline(TH_RANGE, linestyle="--", linewidth=1.0, color="red", label=f"range_th={TH_RANGE}")
    axes[1].scatter(ev["date"], ev["day_range"], s=18, alpha=0.8, color="#d62728")
    axes[1].set_title("day_range")
    axes[1].legend(loc="best")
    axes[1].grid(alpha=0.2)

    # 3) abs(day_return)
    axes[2].plot(pd["date"], pd["day_return"].abs(), linewidth=1.2, color="#2ca02c", label="abs(day_return)")
    axes[2].axhline(TH_RET, linestyle="--", linewidth=1.0, color="red", label=f"ret_th={TH_RET}")
    axes[2].scatter(ev["date"], ev["day_return"].abs(), s=18, alpha=0.8, color="#d62728")
    axes[2].set_title("abs(day_return)")
    axes[2].legend(loc="best")
    axes[2].grid(alpha=0.2)

    fig.tight_layout()
    plt.show()

    cause_tbl = (
        t.filter(pl.col("is_event_day") == True)
         .group_by("trigger_cause")
         .agg(pl.len().alias("n"))
         .sort("n", descending=True)
    )
    display(Markdown("#### Causa de activaci?n de `is_event_day`"))
    display(cause_tbl)


def render_multi_top(n_tickers: int = 12):
    top = ticker_rank.head(n_tickers)
    top_tickers = top["ticker"].to_list()

    m = (
        plot_df.filter(pl.col("ticker").is_in(top_tickers))
        .group_by("ticker")
        .agg([
            pl.len().alias("n_days"),
            pl.col("is_event_day").sum().alias("n_event_days"),
            pl.col("event_score").max().alias("max_event_score"),
        ])
        .with_columns((pl.col("n_event_days") / pl.col("n_days")).alias("event_day_ratio"))
        .sort("event_day_ratio", descending=True)
    )

    mpd = m.to_pandas()
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8))

    axes[0].bar(mpd["ticker"], mpd["event_day_ratio"], color="#4f7ea8")
    axes[0].set_title("Top tickers por ratio de event_day")
    axes[0].set_ylabel("event_day_ratio")
    axes[0].tick_params(axis='x', rotation=75)
    axes[0].grid(alpha=0.2)

    axes[1].bar(mpd["ticker"], mpd["max_event_score"], color="#e07a1f")
    axes[1].set_title("Top tickers por max event_score")
    axes[1].set_ylabel("max_event_score")
    axes[1].tick_params(axis='x', rotation=75)
    axes[1].grid(alpha=0.2)

    fig.tight_layout()
    plt.show()
    display(m)


if widgets is None:
    display(Markdown("ipywidgets no disponible. Render fijo de ejemplo."))
    render_single_ticker(all_tickers[0], tail_days=260)
    render_multi_top(n_tickers=12)
else:
    ticker_dd = widgets.Dropdown(options=all_tickers[:500], value=all_tickers[0], description="Ticker")
    tail_slider = widgets.IntSlider(value=260, min=60, max=1500, step=20, description="Tail days")
    n_multi = widgets.IntSlider(value=12, min=5, max=40, step=1, description="Top N")
    btn_single = widgets.Button(description="Render ticker")
    btn_multi = widgets.Button(description="Render multi")
    out = widgets.Output()

    def _run_single(_):
        with out:
            out.clear_output(wait=True)
            render_single_ticker(ticker_dd.value, tail_days=tail_slider.value)

    def _run_multi(_):
        with out:
            out.clear_output(wait=True)
            render_multi_top(n_tickers=n_multi.value)

    btn_single.on_click(_run_single)
    btn_multi.on_click(_run_multi)

    def _auto_single(*_):
        with out:
            out.clear_output(wait=True)
            render_single_ticker(ticker_dd.value, tail_days=tail_slider.value)

    ticker_dd.observe(_auto_single, names="value")
    tail_slider.observe(_auto_single, names="value")

    display(Markdown(f"### Paso 16B | run: `{run_dir}`"))
    display(widgets.HBox([ticker_dd, tail_slider, btn_single]))
    display(widgets.HBox([n_multi, btn_multi]))
    display(out)

    with out:
        render_single_ticker(ticker_dd.value, tail_days=tail_slider.value)

