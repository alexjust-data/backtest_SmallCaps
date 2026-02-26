# -*- coding: utf-8 -*-
# PASO 16H - BACKSIDE DUMP WIDGET (explosion -> decay)
from pathlib import Path
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from IPython.display import display, Markdown

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
LIFE_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_lifecycle"

# Heuristica de inicio de backside
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

# Build candidate events
cand = (
    df.filter(pl.col("is_lifecycle_event") == True)
      .select(["ticker", "date", "event_lifecycle_score", "score_explosion", "score_decay"])
      .sort(["event_lifecycle_score"], descending=True)
)

if cand.height == 0:
    raise RuntimeError("No hay is_lifecycle_event=True. Ajusta umbrales de Paso 16E.")

pairs = [f"{r['ticker']} | {r['date']}" for r in cand.head(500).to_dicts()]
pair_to_data = {f"{r['ticker']} | {r['date']}": (r['ticker'], r['date']) for r in cand.head(500).to_dicts()}

def detect_backside_start(pd_ticker: pd.DataFrame, event_dt: pd.Timestamp):
    # row del evento
    ev = pd_ticker[pd_ticker["date"] == event_dt]
    if ev.empty:
        return None, "event_not_found", None
    ev = ev.iloc[0]

    # 1) dump ya en el propio evento
    if float(ev.get("day_return", 0.0)) <= TH_EVENT_DAY_DUMP:
        return event_dt, "event_day_dump", 0

    # 2) buscar inicio posterior por condiciones de decay
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

    if row["day_return"] <= TH_EVENT_DAY_DUMP:
        reason = "post_day_dump"
    elif row["cum_post_ret"] <= TH_FWD_CUM_DUMP:
        reason = "cum_post_dump"
    else:
        reason = "score_decay_trigger"

    return start_dt, reason, lag_days

def render_backside(ticker: str, event_date: str, pre_days: int = 20, post_days: int = 20):
    t = df.filter(pl.col("ticker") == ticker).sort("date")
    r = t.filter(pl.col("date") == event_date)
    if r.height == 0:
        display(Markdown(f"No existe evento {ticker} {event_date}"))
        return

    pd_t = t.to_pandas()
    pd_t["date"] = pd.to_datetime(pd_t["date"])
    event_dt = pd.to_datetime(event_date)

    # Proxy de precio desde retornos diarios
    pd_t["price_idx"] = (1.0 + pd_t["day_return"].fillna(0.0)).cumprod()

    # detectar inicio de backside
    backside_start_dt, backside_reason, lag_days = detect_backside_start(pd_t, event_dt)

    win = pd_t[(pd_t["date"] >= event_dt - pd.Timedelta(days=pre_days)) &
               (pd_t["date"] <= event_dt + pd.Timedelta(days=post_days))].copy()
    if win.empty:
        display(Markdown("Ventana vacia para el evento seleccionado"))
        return

    ev_row = r.to_dicts()[0]

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # Top: price proxy + event/backside markers
    axes[0].plot(win["date"], win["price_idx"], lw=1.6, label="price_idx (cum returns)")
    axes[0].axvline(event_dt, ls="--", c="red", lw=1.2, label=f"event_anchor={event_date}")
    if backside_start_dt is not None:
        axes[0].axvline(backside_start_dt, ls="--", c="purple", lw=1.2, label=f"backside_start={backside_start_dt.date()}")
    axes[0].set_title(f"{ticker} | Backside dump view")
    axes[0].legend(loc="best")
    axes[0].grid(alpha=0.2)

    # Bottom: day_return and forward dump evidence
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

    # Event diagnostics table
    diag = pl.DataFrame({
        "ticker": [ticker],
        "event_date": [event_date],
        "backside_start_date": [str(backside_start_dt.date()) if backside_start_dt is not None else None],
        "backside_reason": [backside_reason],
        "backside_lag_days": [lag_days],
        "score_explosion": [float(ev_row["score_explosion"])],
        "score_decay": [float(ev_row["score_decay"])],
        "event_lifecycle_score": [float(ev_row["event_lifecycle_score"])],
        "day_return": [float(ev_row["day_return"])],
        "fwd_ret_1d": [float(ev_row.get("fwd_ret_1d", float('nan')))],
        "fwd_ret_3d": [float(ev_row.get("fwd_ret_3d", float('nan')))],
        "is_explosion_day": [bool(ev_row["is_explosion_day"])],
        "is_decay_followup": [bool(ev_row["is_decay_followup"])],
        "is_lifecycle_event": [bool(ev_row["is_lifecycle_event"])],
    })
    display(diag)

if widgets is None:
    tk, dt = pair_to_data[pairs[0]]
    render_backside(tk, dt)
else:
    pair_dd = widgets.Dropdown(options=pairs, value=pairs[0], description="Event")
    pre_slider = widgets.IntSlider(value=20, min=5, max=60, step=1, description="Pre days")
    post_slider = widgets.IntSlider(value=20, min=5, max=60, step=1, description="Post days")
    btn = widgets.Button(description="Render dump")
    out = widgets.Output()

    def _run(_):
        with out:
            out.clear_output(wait=True)
            tk, dt = pair_to_data[pair_dd.value]
            render_backside(tk, dt, pre_days=pre_slider.value, post_days=post_slider.value)

    btn.on_click(_run)

    display(Markdown(f"### Paso 16H | run `{life_dir.name}`"))
    display(widgets.HBox([pair_dd, pre_slider, post_slider, btn]))
    display(out)
    with out:
        tk, dt = pair_to_data[pair_dd.value]
        render_backside(tk, dt, pre_days=pre_slider.value, post_days=post_slider.value)
