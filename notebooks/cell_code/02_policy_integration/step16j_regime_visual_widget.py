# -*- coding: utf-8 -*-
# PASO 16J - REGIME VISUAL WIDGET (graficos + filtros)
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from IPython.display import display, Markdown, Image as IPyImage

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")
LIFE_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"
REG_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle_regime"
QUOTE_ROOTS = [
    DATA_ROOT / "quotes_p95_2004_2018",
    DATA_ROOT / "quotes_p95_2019_2025",
    DATA_ROOT / "quotes_p95",
]

life_run = sorted(LIFE_ROOT.glob("step16e_event_lifecycle_*"), key=lambda p: p.stat().st_mtime)[-1]
reg_run = sorted(REG_ROOT.glob("step16i_regime_*"), key=lambda p: p.stat().st_mtime)[-1]

life_fp = life_run / "step16e_event_lifecycle_scores.parquet"
reg_fp = reg_run / "step16i_event_regime_labels.parquet"

if not life_fp.exists() or not reg_fp.exists():
    raise FileNotFoundError("Faltan artefactos de 16E o 16I")

life = pl.read_parquet(life_fp)
reg = pl.read_parquet(reg_fp)

if "date" not in life.columns:
    raise ValueError(f"16E sin columna 'date'. Columnas: {life.columns}")
if "event_date" not in reg.columns:
    raise ValueError(f"16I sin columna 'event_date'. Columnas: {reg.columns}")

life = life.with_columns(pl.col("date").cast(pl.Utf8).alias("event_date_key"))
reg = reg.with_columns(pl.col("event_date").cast(pl.Utf8).alias("event_date_key"))
reg = reg.unique(subset=["ticker", "event_date", "regime_label"])

regimes = [
    "pump_then_dump_confirmed",
    "pump_then_shakeout",
    "pump_continuation",
]

# Deteccion simetrica de inicios
TH_PUMP_DAY = 0.08
TH_PRE_CUM_PUMP = 0.15
TH_SCORE_EXPLOSION = 2.0

TH_EVENT_DAY_DUMP = -0.08
TH_POST_CUM_DUMP = -0.15
TH_SCORE_DECAY = 1.2
_TV_CACHE = {}


def get_events_by_regime(regime: str):
    return (
        reg.filter(pl.col("regime_label") == regime)
           .sort(["event_lifecycle_score"], descending=True)
           .select(["ticker", "event_date", "event_lifecycle_score", "dd10", "rebound_ratio_10d", "regime_label"])
    )


def detect_pump_start(pd_ticker: pd.DataFrame, event_dt: pd.Timestamp):
    # pre-window including event day
    pre = pd_ticker[pd_ticker["date"] <= event_dt].copy().sort_values("date")
    if pre.empty:
        return None, "no_pre_data", None

    # 1) pump ya en el evento
    ev = pre[pre["date"] == event_dt]
    if not ev.empty and float(ev.iloc[0].get("day_return", 0.0)) >= TH_PUMP_DAY:
        return event_dt, "event_day_pump", 0

    # 2) condiciones pre-evento (simetrico a dump)
    pre_only = pre[pre["date"] < event_dt].copy()
    if pre_only.empty:
        return None, "no_pre_only", None

    # cum return desde cada fecha pre hasta event_dt
    pre_only = pre_only.reset_index(drop=True)
    full = pre.reset_index(drop=True)
    i_event = int(full[full["date"] == event_dt].index[0])

    # map index in full for each pre row
    full_idx_map = {d: i for i, d in enumerate(full["date"].tolist())}

    starts = []
    for _, r in pre_only.iterrows():
        d0 = r["date"]
        i0 = full_idx_map.get(d0)
        if i0 is None or i0 > i_event:
            continue
        seg = full.iloc[i0:i_event+1]
        cum_to_event = float((1.0 + seg["day_return"].fillna(0.0)).prod() - 1.0)
        cond = (
            (float(r.get("day_return", 0.0)) >= TH_PUMP_DAY)
            or (cum_to_event >= TH_PRE_CUM_PUMP)
            or (float(r.get("score_explosion", 0.0)) >= TH_SCORE_EXPLOSION)
        )
        if cond:
            starts.append((d0, float(r.get("day_return", 0.0)), cum_to_event, float(r.get("score_explosion", 0.0))))

    if not starts:
        return None, "no_pump_trigger", None

    # inicio = primer trigger cronologico
    starts = sorted(starts, key=lambda x: x[0])
    pump_start_dt, day_ret, cum_to_event, sx = starts[0]

    if day_ret >= TH_PUMP_DAY:
        reason = "pre_day_pump"
    elif cum_to_event >= TH_PRE_CUM_PUMP:
        reason = "cum_pre_pump"
    else:
        reason = "score_explosion_trigger"

    lag_days = int((event_dt - pump_start_dt).days)
    return pump_start_dt, reason, lag_days


def detect_backside_start(pd_ticker: pd.DataFrame, event_dt: pd.Timestamp):
    ev = pd_ticker[pd_ticker["date"] == event_dt]
    if ev.empty:
        return None, "event_not_found", None
    ev = ev.iloc[0]

    if float(ev.get("day_return", 0.0)) <= TH_EVENT_DAY_DUMP:
        return event_dt, "event_day_dump", 0

    post = pd_ticker[pd_ticker["date"] > event_dt].copy().sort_values("date")
    if post.empty:
        return None, "no_post_data", None

    post["cum_post_ret"] = (1.0 + post["day_return"].fillna(0.0)).cumprod() - 1.0
    cond = (
        (post["day_return"] <= TH_EVENT_DAY_DUMP)
        | (post["cum_post_ret"] <= TH_POST_CUM_DUMP)
        | (post["score_decay"].fillna(0.0) >= TH_SCORE_DECAY)
    )
    hit = post[cond]
    if hit.empty:
        return None, "no_backside_trigger", None

    row = hit.iloc[0]
    start_dt = row["date"]
    lag_days = int((start_dt - event_dt).days)

    if row["day_return"] <= TH_EVENT_DAY_DUMP:
        reason = "post_day_dump"
    elif row["cum_post_ret"] <= TH_POST_CUM_DUMP:
        reason = "cum_post_dump"
    else:
        reason = "score_decay_trigger"

    return start_dt, reason, lag_days


def _find_quote_fp(ticker: str, dt: pd.Timestamp):
    y, m, d = int(dt.year), int(dt.month), int(dt.day)
    for root in QUOTE_ROOTS:
        fp = root / ticker / f"year={y}" / f"month={m:02d}" / f"day={d:02d}" / "quotes.parquet"
        if fp.exists():
            return fp
    return None


def _day_ohlc_from_quotes(fp: Path):
    try:
        q = pl.read_parquet(fp, columns=["bid_price", "ask_price"])
    except Exception:
        return None
    if not {"bid_price", "ask_price"}.issubset(set(q.columns)):
        return None
    q = q.with_columns(((pl.col("bid_price") + pl.col("ask_price")) / 2.0).alias("mid")).filter(pl.col("mid").is_finite())
    if q.height == 0:
        return None
    s = q.select([
        pl.col("mid").first().alias("open"),
        pl.col("mid").max().alias("high"),
        pl.col("mid").min().alias("low"),
        pl.col("mid").last().alias("close"),
        pl.len().alias("n_ticks"),
    ]).to_dicts()[0]
    return s


def _tv_window_df(ticker: str, event_dt: pd.Timestamp, pre_days: int, post_days: int):
    key = (ticker, str(event_dt.date()), int(pre_days), int(post_days))
    if key in _TV_CACHE:
        return _TV_CACHE[key]

    left = (event_dt - pd.Timedelta(days=pre_days)).normalize()
    right = (event_dt + pd.Timedelta(days=post_days)).normalize()
    dates = pd.date_range(left, right, freq="D")

    rows = []
    prev_close = None
    for d in dates:
        fp = _find_quote_fp(ticker, d)
        if fp is None:
            rows.append({
                "date": d,
                "open": prev_close,
                "high": prev_close,
                "low": prev_close,
                "close": prev_close,
                "n_ticks": 0,
                "has_quotes": False,
            })
            continue
        m = _day_ohlc_from_quotes(fp)
        if m is None:
            rows.append({
                "date": d,
                "open": prev_close,
                "high": prev_close,
                "low": prev_close,
                "close": prev_close,
                "n_ticks": 0,
                "has_quotes": False,
            })
            continue
        prev_close = float(m["close"]) if m["close"] is not None else prev_close
        rows.append({
            "date": d,
            "open": float(m["open"]),
            "high": float(m["high"]),
            "low": float(m["low"]),
            "close": float(m["close"]),
            "n_ticks": int(m["n_ticks"]),
            "has_quotes": True,
        })

    out = pd.DataFrame(rows)
    _TV_CACHE[key] = out
    return out


def _plot_tv_like(ax_price, ax_vol, tvdf: pd.DataFrame, event_dt: pd.Timestamp, ticker: str):
    x = mdates.date2num(pd.to_datetime(tvdf["date"]).dt.to_pydatetime())
    width = 0.7

    for xi, row in zip(x, tvdf.itertuples(index=False)):
        if row.open is None:
            continue
        o, h, l, c = row.open, row.high, row.low, row.close
        has_q = bool(row.has_quotes)
        up = c >= o

        if has_q:
            body_color = "#26a69a" if up else "#ef5350"
            edge_color = body_color
            wick_color = body_color
            alpha = 0.95
        else:
            body_color = "none"
            edge_color = "#9e9e9e"
            wick_color = "#bdbdbd"
            alpha = 0.8

        ax_price.vlines(xi, l, h, color=wick_color, linewidth=1.0, alpha=alpha, zorder=2)
        y0 = min(o, c)
        hh = abs(c - o)
        if hh < 1e-9:
            hh = max(1e-4, (h - l) * 0.02 if h is not None and l is not None else 1e-4)
        rect = Rectangle((xi - width / 2.0, y0), width, hh, facecolor=body_color, edgecolor=edge_color, linewidth=1.0, alpha=alpha, zorder=3)
        ax_price.add_patch(rect)

        vcolor = "#26a69a" if up else "#ef5350"
        if not has_q:
            vcolor = "#c7c7c7"
        ax_vol.bar(xi, row.n_ticks, width=0.7, color=vcolor, alpha=0.8)

    ax_price.axvline(mdates.date2num(event_dt), ls="--", c="#1e88e5", lw=1.2, label=f"event={event_dt.date()}")
    ax_price.set_title(f"{ticker} | TV-like OHLC (quotes=filled, no-quotes=hollow)")
    ax_price.grid(alpha=0.18)
    ax_price.legend(loc="best")

    ax_vol.set_title("Daily ticks volume proxy")
    ax_vol.grid(alpha=0.18)
    ax_vol.xaxis_date()
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))


def render_event(ticker: str, event_date_key: str, pre_days: int = 30, post_days: int = 30, tv_image_path: str = ""):
    t = life.filter(pl.col("ticker") == ticker).sort("date")
    if t.height == 0:
        display(Markdown(f"Sin serie para `{ticker}`"))
        return

    # Defensive normalization to avoid key mismatch between widget value and parquet type.
    event_key = str(event_date_key)
    ev = reg.filter(
        (pl.col("ticker") == ticker)
        & (pl.col("event_date").cast(pl.Utf8) == event_key)
    ).head(1)
    if ev.height == 0:
        event_key2 = event_key.split(" ")[0]
        ev = reg.filter(
            (pl.col("ticker") == ticker)
            & (pl.col("event_date").cast(pl.Utf8).str.starts_with(event_key2))
        ).head(1)
    if ev.height == 0:
        display(Markdown(f"Sin etiqueta 16I para `{ticker}` `{event_date_key}`"))
        return
    evd = ev.to_dicts()[0]

    pd_t = t.to_pandas()
    pd_t["date"] = pd.to_datetime(pd_t["date"])
    event_dt = pd.to_datetime(str(ev.to_dicts()[0]["event_date"]))

    pd_t["price_idx"] = (1.0 + pd_t["day_return"].fillna(0.0)).cumprod()
    win = pd_t[(pd_t["date"] >= event_dt - pd.Timedelta(days=pre_days)) &
               (pd_t["date"] <= event_dt + pd.Timedelta(days=post_days))].copy()
    if win.empty:
        display(Markdown("Ventana vacia"))
        return
    n_bars = int(len(win))
    sparse = n_bars <= 8

    pump_start_dt, pump_reason, pump_lag_days = detect_pump_start(win, event_dt)
    backside_start_dt, backside_reason, backside_lag_days = detect_backside_start(win, event_dt)

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    # 1) price proxy
    if sparse:
        axes[0].scatter(win["date"], win["price_idx"], s=28, label="price_idx (sparse points)")
    else:
        axes[0].plot(win["date"], win["price_idx"], lw=1.6, label="price_idx")
    axes[0].axvline(event_dt, ls="--", c="red", lw=1.2, label=f"event={event_date_key}")
    if pump_start_dt is not None:
        axes[0].axvline(pump_start_dt, ls="--", c="green", lw=1.2, label=f"pump_start={pump_start_dt.date()}")
    if backside_start_dt is not None:
        axes[0].axvline(backside_start_dt, ls="--", c="purple", lw=1.2, label=f"backside_start={backside_start_dt.date()}")
    axes[0].set_title(f"{ticker} | {evd['regime_label']} | bars_in_window={n_bars}")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    # 2) day return bars
    colors = ["#d62728" if x < 0 else "#4f7ea8" for x in win["day_return"].fillna(0.0)]
    axes[1].bar(win["date"], win["day_return"], color=colors, width=1.0, alpha=0.85, label="day_return")
    axes[1].axhline(0, c="black", lw=0.8)
    axes[1].axvline(event_dt, ls="--", c="red", lw=1.2)
    if pump_start_dt is not None:
        axes[1].axvline(pump_start_dt, ls="--", c="green", lw=1.2)
    if backside_start_dt is not None:
        axes[1].axvline(backside_start_dt, ls="--", c="purple", lw=1.2)
    axes[1].set_title("day_return around event")
    axes[1].grid(alpha=0.2)

    # 3) scores
    if "score_explosion" in win.columns and "score_decay" in win.columns:
        if sparse:
            axes[2].scatter(win["date"], win["score_explosion"], s=24, label="score_explosion")
            axes[2].scatter(win["date"], win["score_decay"], s=24, label="score_decay")
        else:
            axes[2].plot(win["date"], win["score_explosion"], lw=1.2, label="score_explosion")
            axes[2].plot(win["date"], win["score_decay"], lw=1.2, label="score_decay")
    if "event_lifecycle_score" in win.columns:
        if sparse:
            axes[2].scatter(win["date"], win["event_lifecycle_score"], s=24, label="event_lifecycle_score")
        else:
            axes[2].plot(win["date"], win["event_lifecycle_score"], lw=1.2, linestyle=":", label="event_lifecycle_score")
    axes[2].axvline(event_dt, ls="--", c="red", lw=1.2)
    if pump_start_dt is not None:
        axes[2].axvline(pump_start_dt, ls="--", c="green", lw=1.2)
    if backside_start_dt is not None:
        axes[2].axvline(backside_start_dt, ls="--", c="purple", lw=1.2)
    axes[2].set_title("scores around event")
    axes[2].legend(loc="best")
    axes[2].grid(alpha=0.2)

    fig.tight_layout()
    plt.show()
    # Extra window: TradingView-like candlestick panel built from quotes
    tvdf = _tv_window_df(ticker, event_dt, pre_days, post_days)
    fig2, (axp, axv) = plt.subplots(2, 1, figsize=(14, 7), sharex=True, gridspec_kw={"height_ratios": [3, 1]})
    _plot_tv_like(axp, axv, tvdf, event_dt, ticker)
    fig2.tight_layout()
    plt.show()
    if tv_image_path:
        p = Path(tv_image_path)
        if p.exists():
            display(Markdown("#### TradingView (captura aportada)"))
            display(IPyImage(filename=str(p)))
        else:
            display(Markdown(f"Ruta TradingView no encontrada: `{tv_image_path}`"))
    if sparse:
        display(Markdown(
            f"**Aviso de cobertura**: solo `{n_bars}` barras en la ventana solicitada. "
            "La forma del chart refleja disponibilidad real en `quotes_p95`, no un fallo del widget."
        ))

    diag = pl.DataFrame({
        "ticker": [ticker],
        "pump_start_date": [str(pump_start_dt.date()) if pump_start_dt is not None else None],
        "pump_reason": [pump_reason],
        "pump_lag_days": [pump_lag_days],
        "event_date": [event_date_key],
        "backside_start_date": [str(backside_start_dt.date()) if backside_start_dt is not None else None],
        "backside_reason": [backside_reason],
        "backside_lag_days": [backside_lag_days],
        "regime_label": [evd["regime_label"]],
        "event_lifecycle_score": [float(evd.get("event_lifecycle_score", float("nan")))],
        "dd10": [float(evd.get("dd10", float("nan")))],
        "rebound_ratio_10d": [float(evd.get("rebound_ratio_10d", float("nan")))],
        "score_explosion": [float(evd.get("score_explosion", float("nan")))],
        "score_decay": [float(evd.get("score_decay", float("nan")))],
    })
    display(diag)


if widgets is None:
    r = get_events_by_regime("pump_then_dump_confirmed")
    if r.height == 0:
        r = reg.sort("event_lifecycle_score", descending=True)
    first = r.head(1).to_dicts()[0]
    render_event(first["ticker"], str(first["event_date"]), 30, 30)
else:
    regime_dd = widgets.Dropdown(options=regimes, value="pump_then_dump_confirmed", description="Regime")

    def _pairs(regime):
        rr = get_events_by_regime(regime)
        items = []
        for d in rr.head(300).to_dicts():
            dkey = str(d["event_date"])
            items.append((f"{d['ticker']} | {dkey} | score={d['event_lifecycle_score']:.3f}", (d["ticker"], dkey)))
        return items

    pair_dd = widgets.Dropdown(options=_pairs(regime_dd.value), description="Event")
    pre_slider = widgets.IntSlider(value=30, min=5, max=90, step=1, description="Pre")
    post_slider = widgets.IntSlider(value=30, min=5, max=90, step=1, description="Post")
    tv_path = widgets.Text(value="", description="TV img", placeholder=r"C:\ruta\captura_tv.png")
    btn = widgets.Button(description="Render")
    out = widgets.Output()

    def _refresh(*_):
        opts = _pairs(regime_dd.value)
        pair_dd.options = opts
        if opts:
            pair_dd.value = opts[0][1]

    def _run(_=None):
        with out:
            out.clear_output(wait=True)
            if pair_dd.value is None:
                print("Sin eventos para ese regimen")
                return
            tk, dt = pair_dd.value
            print(f"[16J] selected={tk} | {dt} | regime={regime_dd.value}")
            render_event(tk, dt, pre_days=pre_slider.value, post_days=post_slider.value, tv_image_path=tv_path.value.strip())

    regime_dd.observe(_refresh, names="value")
    pair_dd.observe(lambda ch: _run(None) if ch.get("name") == "value" else None, names="value")
    btn.on_click(_run)

    display(Markdown(f"### Paso 16J | life_run=`{life_run.name}` | regime_run=`{reg_run.name}`"))
    display(widgets.HBox([regime_dd, pair_dd]))
    display(widgets.HBox([pre_slider, post_slider, btn]))
    display(tv_path)
    display(out)

    with out:
        if pair_dd.value is not None:
            tk, dt = pair_dd.value
            render_event(tk, dt, pre_days=pre_slider.value, post_days=post_slider.value)
