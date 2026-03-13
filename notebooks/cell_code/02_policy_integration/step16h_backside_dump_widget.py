# -*- coding: utf-8 -*-
# PASO 16H - BACKSIDE DUMP WIDGET (stable v3)
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown, HTML

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

print('16H stable v3 loaded')

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
LIFE_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"

TH_EVENT_DAY_DUMP = -0.08
TH_FWD_CUM_DUMP = -0.15
TH_SCORE_DECAY = 1.2

life_runs = sorted(LIFE_ROOT.glob("step16e_event_lifecycle_*"), key=lambda p: p.stat().st_mtime)
if not life_runs:
    raise FileNotFoundError("No hay step16e_event_lifecycle_*. Ejecuta Paso 16E primero.")
life_dir = life_runs[-1]
fp_life = life_dir / "step16e_event_lifecycle_scores.parquet"
if not fp_life.exists():
    raise FileNotFoundError(f"No existe {fp_life}")

df = pl.read_parquet(fp_life).sort(["ticker", "date"])
if df.height == 0:
    raise RuntimeError("Lifecycle scores vacio")

cand = (
    df.filter(pl.col("is_lifecycle_event") == True)
      .select(["ticker", "date", "event_lifecycle_score"])
      .sort(["event_lifecycle_score"], descending=True)
)
if cand.height == 0:
    raise RuntimeError("No hay is_lifecycle_event=True.")

rows = cand.head(500).to_dicts()
seen = set()
by_ticker = {}
for r in rows:
    tk = str(r["ticker"])
    dt = str(r["date"])
    if (tk, dt) in seen:
        continue
    seen.add((tk, dt))
    score = float(r.get("event_lifecycle_score", 0.0))
    by_ticker.setdefault(tk, []).append(f"{dt} | score={score:.3f}")

tickers = sorted(by_ticker.keys())
for tk in tickers:
    by_ticker[tk] = sorted(by_ticker[tk])


def detect_backside_start(pd_ticker: pd.DataFrame, event_dt: pd.Timestamp):
    ev = pd_ticker[pd_ticker["date"] == event_dt]
    if ev.empty:
        return None, "event_not_found", None
    ev = ev.iloc[0]
    if float(ev.get("day_return", 0.0)) <= TH_EVENT_DAY_DUMP:
        return event_dt, "event_day_dump", 0

    post = pd_ticker[pd_ticker["date"] > event_dt].copy()
    if post.empty:
        return None, "no_post_data", None

    post["cum_post_ret"] = (1.0 + post["day_return"].fillna(0.0)).cumprod() - 1.0
    cond = (
        (post["day_return"] <= TH_EVENT_DAY_DUMP)
        | (post["cum_post_ret"] <= TH_FWD_CUM_DUMP)
        | (post["score_decay"].fillna(0.0) >= TH_SCORE_DECAY)
    )
    hit = post[cond]
    if hit.empty:
        return None, "no_backside_trigger", None

    row = hit.iloc[0]
    start_dt = row["date"]
    lag_days = int((start_dt - event_dt).days)
    reason = "post_day_dump" if row["day_return"] <= TH_EVENT_DAY_DUMP else ("cum_post_dump" if row["cum_post_ret"] <= TH_FWD_CUM_DUMP else "score_decay_trigger")
    return start_dt, reason, lag_days


def _tv_html(symbol: str, interval: str = "D", height: int = 620, theme: str = "light"):
    safe = symbol.replace(":", "_").replace(".", "_").replace("-", "_")
    cid = f"tv_{safe}_{abs(hash((symbol, interval, height, theme))) % 10_000_000}"
    return f"""
    <div class=\"tradingview-widget-container\" style=\"width:100%;height:{height}px;\">
      <div id=\"{cid}\" style=\"width:100%;height:{height}px;\"></div>
      <script type=\"text/javascript\" src=\"https://s3.tradingview.com/tv.js\"></script>
      <script type=\"text/javascript\">
      if (typeof TradingView !== \"undefined\") {{
        new TradingView.widget({{
          \"autosize\": true,
          \"symbol\": \"{symbol}\",
          \"interval\": \"{interval}\",
          \"timezone\": \"Etc/UTC\",
          \"theme\": \"{theme}\",
          \"style\": \"1\",
          \"locale\": \"en\",
          \"toolbar_bg\": \"#f1f3f6\",
          \"enable_publishing\": false,
          \"allow_symbol_change\": true,
          \"container_id\": \"{cid}\"
        }});
      }}
      </script>
    </div>
    """


def render_backside(ticker: str, event_date: str, pre_days: int, post_days: int, exchange: str, tv_theme: str):
    t = df.filter(pl.col("ticker") == ticker).sort("date")
    t2 = t.with_columns(pl.col("date").cast(pl.Utf8).alias("date_key"))

    r = t2.filter(pl.col("date_key") == str(event_date))
    if r.height == 0:
        r = t2.filter(pl.col("date_key").str.starts_with(str(event_date).split(" ")[0]))
    if r.height == 0:
        display(Markdown(f"No existe evento {ticker} {event_date}"))
        return

    pd_t = t.to_pandas()
    pd_t["date"] = pd.to_datetime(pd_t["date"])
    event_dt = pd.to_datetime(str(r.to_dicts()[0]["date_key"]))

    pd_t["price_idx"] = (1.0 + pd_t["day_return"].fillna(0.0)).cumprod()
    backside_start_dt, backside_reason, lag_days = detect_backside_start(pd_t, event_dt)

    win = pd_t[(pd_t["date"] >= event_dt - pd.Timedelta(days=pre_days)) & (pd_t["date"] <= event_dt + pd.Timedelta(days=post_days))].copy()
    if win.empty:
        display(Markdown("Ventana vacia para el evento seleccionado"))
        return

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    axes[0].plot(win["date"], win["price_idx"], lw=1.6, label="price_idx (cum returns)")
    axes[0].axvline(event_dt, ls="--", c="red", lw=1.2, label=f"event_anchor={event_date}")
    if backside_start_dt is not None:
        axes[0].axvline(backside_start_dt, ls="--", c="purple", lw=1.2, label=f"backside_start={backside_start_dt.date()}")
    axes[0].set_title(f"{ticker} | Backside dump view | bars_in_window={len(win)}")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    axes[1].bar(win["date"], win["day_return"], width=1.0, alpha=0.8, color="#4f7ea8", label="day_return")
    axes[1].axvline(event_dt, ls="--", c="red", lw=1.2)
    if backside_start_dt is not None:
        axes[1].axvline(backside_start_dt, ls="--", c="purple", lw=1.2)
    axes[1].axhline(0, c="black", lw=0.8)
    axes[1].set_title("day_return around event (negative bars = dump)")
    axes[1].legend(loc="best")
    axes[1].grid(alpha=0.2)
    fig.tight_layout()
    plt.show()

    ev_row = r.to_dicts()[0]
    diag = pl.DataFrame({
        "ticker": [ticker],
        "event_date": [event_date],
        "backside_start_date": [str(backside_start_dt.date()) if backside_start_dt is not None else None],
        "backside_reason": [backside_reason],
        "backside_lag_days": [lag_days],
        "score_explosion": [float(ev_row["score_explosion"])],
        "score_decay": [float(ev_row["score_decay"])],
        "event_lifecycle_score": [float(ev_row["event_lifecycle_score"])],
    })
    display(diag)

    symbol = f"{exchange}:{ticker}"
    display(Markdown(f"#### TradingView oficial | `{symbol}`"))
    display(HTML(_tv_html(symbol=symbol, interval="D", height=620, theme=tv_theme)))


if widgets is None:
    tk = tickers[0]
    dt = by_ticker[tk][0].split(" | ")[0]
    render_backside(tk, dt, 20, 20, "NASDAQ", "light")
else:
    ticker_dd = widgets.Dropdown(options=tickers, value=tickers[0], description="Ticker")
    event_dd = widgets.Dropdown(options=by_ticker[ticker_dd.value], value=by_ticker[ticker_dd.value][0], description="Event")
    pre_slider = widgets.IntSlider(value=20, min=5, max=60, step=1, description="Pre days")
    post_slider = widgets.IntSlider(value=20, min=5, max=60, step=1, description="Post days")
    ex_dd = widgets.Dropdown(options=["NASDAQ", "NYSE", "AMEX", "OTC"], value="NASDAQ", description="Exchange")
    theme_dd = widgets.Dropdown(options=["light", "dark"], value="light", description="TV theme")
    btn = widgets.Button(description="Render dump")
    out = widgets.Output()

    def _on_ticker_change(change):
        if change.get("name") != "value":
            return
        tk = str(change["new"])
        opts = by_ticker.get(tk, [])
        event_dd.options = opts
        event_dd.value = opts[0] if opts else None

    def _run(_=None):
        with out:
            out.clear_output(wait=True)
            tk = str(ticker_dd.value)
            evs = by_ticker.get(tk, [])
            if not evs:
                print(f"[16H] sin eventos para {tk}")
                return
            if event_dd.value not in evs:
                event_dd.value = evs[0]
            dt = str(event_dd.value).split(" | ")[0]
            print(f"[16H] ticker_dd={tk} | event={event_dd.value}")
            render_backside(tk, dt, pre_slider.value, post_slider.value, str(ex_dd.value), str(theme_dd.value))

    ticker_dd.observe(_on_ticker_change, names="value")
    btn.on_click(_run)

    display(Markdown(f"### Paso 16H | run `{life_dir.name}`"))
    display(widgets.VBox([ticker_dd, event_dd, pre_slider, post_slider, ex_dd, theme_dd, btn]))
    display(out)
    _run()
