from pathlib import Path
import random

import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")

EVENT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
run_dir = sorted(EVENT_ROOT.glob("step16_event_index_*"), key=lambda p: p.stat().st_mtime)[-1]
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


def build_sample(mode: str, n_sample: int, ticker: str | None, event_only: bool):
    d = df
    if event_only:
        d = d.filter(pl.col("is_event_day") == True)
    if mode == "ticker" and ticker:
        d = d.filter(pl.col("ticker") == ticker)

    rows = d.select(["ticker", "date", "n_ticks", "day_return", "day_range", "is_event_day"]).to_dicts()
    if not rows:
        return []

    if mode == "all" or (mode == "ticker" and ticker):
        return rows

    random.seed(42)
    return random.sample(rows, min(n_sample, len(rows)))


def run_qa(rows):
    idx_ticks, calc_ticks = [], []
    idx_ret, calc_ret = [], []
    idx_rng, calc_rng = [], []
    audit_rows = []

    for r in rows:
        fp = find_quote_file(r["ticker"], r["date"])
        if fp is None:
            continue
        n2, ret2, rng2 = recompute_from_quotes(fp)

        idx_ticks.append(r["n_ticks"])
        calc_ticks.append(n2)
        idx_ret.append(r["day_return"])
        calc_ret.append(ret2)
        idx_rng.append(r["day_range"])
        calc_rng.append(rng2)

        audit_rows.append({
            "ticker": r["ticker"],
            "date": r["date"],
            "is_event_day": bool(r["is_event_day"]),
            "n_ticks_idx": r["n_ticks"],
            "n_ticks_re": n2,
            "day_return_idx": r["day_return"],
            "day_return_re": ret2,
            "day_range_idx": r["day_range"],
            "day_range_re": rng2,
            "err_n_ticks": n2 - r["n_ticks"],
            "err_day_return": ret2 - r["day_return"],
            "err_day_range": rng2 - r["day_range"],
        })

    if not audit_rows:
        return None, None

    adf = pl.DataFrame(audit_rows)

    idx_ticks = np.array(idx_ticks)
    calc_ticks = np.array(calc_ticks)
    idx_ret = np.array(idx_ret)
    calc_ret = np.array(calc_ret)
    idx_rng = np.array(idx_rng)
    calc_rng = np.array(calc_rng)

    err_ticks = calc_ticks - idx_ticks
    err_ret = calc_ret - idx_ret
    err_rng = calc_rng - idx_rng

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))

    axes[0, 0].scatter(idx_ticks, calc_ticks, s=12, alpha=0.65)
    mn, mx = min(idx_ticks.min(), calc_ticks.min()), max(idx_ticks.max(), calc_ticks.max())
    axes[0, 0].plot([mn, mx], [mn, mx], "r--", lw=1.2)
    axes[0, 0].set_title("n_ticks: index vs recomputed")
    axes[0, 0].set_xlabel("index")
    axes[0, 0].set_ylabel("recomputed")
    axes[0, 0].grid(alpha=0.2)

    axes[0, 1].scatter(idx_ret, calc_ret, s=12, alpha=0.65)
    mn, mx = min(idx_ret.min(), calc_ret.min()), max(idx_ret.max(), calc_ret.max())
    axes[0, 1].plot([mn, mx], [mn, mx], "r--", lw=1.2)
    axes[0, 1].set_title("day_return: index vs recomputed")
    axes[0, 1].set_xlabel("index")
    axes[0, 1].set_ylabel("recomputed")
    axes[0, 1].grid(alpha=0.2)

    axes[0, 2].scatter(idx_rng, calc_rng, s=12, alpha=0.65)
    mn, mx = min(idx_rng.min(), calc_rng.min()), max(idx_rng.max(), calc_rng.max())
    axes[0, 2].plot([mn, mx], [mn, mx], "r--", lw=1.2)
    axes[0, 2].set_title("day_range: index vs recomputed")
    axes[0, 2].set_xlabel("index")
    axes[0, 2].set_ylabel("recomputed")
    axes[0, 2].grid(alpha=0.2)

    axes[1, 0].hist(err_ticks, bins=30, color="#4f7ea8", alpha=0.9)
    axes[1, 0].set_title("Error n_ticks")
    axes[1, 0].grid(alpha=0.2)

    axes[1, 1].hist(err_ret, bins=30, color="#e07a1f", alpha=0.9)
    axes[1, 1].set_title("Error day_return")
    axes[1, 1].grid(alpha=0.2)

    axes[1, 2].hist(err_rng, bins=30, color="#5b8f5b", alpha=0.9)
    axes[1, 2].set_title("Error day_range")
    axes[1, 2].grid(alpha=0.2)

    fig.suptitle(f"Step16 QA visual | run={run_dir.name} | sample={len(adf)}", y=1.02)
    fig.tight_layout()

    return fig, adf


def render(mode: str, n_sample: int, ticker: str | None, event_only: bool):
    rows = build_sample(mode=mode, n_sample=n_sample, ticker=ticker, event_only=event_only)
    fig, adf = run_qa(rows)

    if adf is None:
        display(Markdown("Sin filas para auditar con esa selección."))
        return

    display(Markdown(
        f"**Modo:** `{mode}` | **event_only:** `{event_only}` | **ticker:** `{ticker}` | "
        f"**filas_auditadas:** `{adf.height}`"
    ))
    display(fig)

    top_err = adf.with_columns([
        pl.max_horizontal(pl.col("err_day_return").abs(), pl.col("err_day_range").abs()).alias("err_price_max")
    ]).sort(["err_price_max", "err_n_ticks"], descending=True).head(20)
    display(Markdown("#### Top 20 errores"))
    display(top_err)


all_tickers = sorted(df.select("ticker").unique().to_series().to_list())

if widgets is None:
    display(Markdown("ipywidgets no disponible. Ejecutando modo aleatorio (300)."))
    render(mode="random", n_sample=300, ticker=None, event_only=False)
else:
    mode_dd = widgets.Dropdown(
        options=[
            ("Aleatorio", "random"),
            ("Todos", "all"),
            ("Ticker", "ticker"),
            ("Evento (is_event_day)", "event"),
        ],
        value="random",
        description="Modo",
    )
    n_slider = widgets.IntSlider(value=300, min=20, max=3000, step=20, description="N")
    ticker_dd = widgets.Dropdown(options=all_tickers[:1000], value=(all_tickers[0] if all_tickers else None), description="Ticker")
    event_chk = widgets.Checkbox(value=False, description="Solo eventos")
    btn = widgets.Button(description="Ejecutar QA")
    out = widgets.Output()

    def _sync(*_):
        is_ticker = mode_dd.value == "ticker"
        is_event = mode_dd.value == "event"
        ticker_dd.disabled = not is_ticker
        event_chk.disabled = is_event
        if is_event:
            event_chk.value = True

    def _run(_=None):
        with out:
            out.clear_output(wait=True)
            mode = mode_dd.value
            tk = ticker_dd.value if mode == "ticker" else None
            ev = True if mode == "event" else bool(event_chk.value)
            render(mode=mode, n_sample=n_slider.value, ticker=tk, event_only=ev)

    mode_dd.observe(_sync, names="value")
    btn.on_click(_run)

    _sync()
    display(Markdown(f"### Paso 16C QA Widget | run: `{run_dir}`"))
    display(widgets.HBox([mode_dd, n_slider, ticker_dd]))
    display(widgets.HBox([event_chk, btn]))
    display(out)

    _run()