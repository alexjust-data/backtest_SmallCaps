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
ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "split_reverse_engineering"

run_dir = sorted(ROOT.glob("step16l_split_reverse_*"), key=lambda p: p.stat().st_mtime)[-1]
jumps_fp = run_dir / "step16l_jumps_labeled.parquet"
adj_fp = run_dir / "step16l_adjusted_series.parquet"

jumps = pl.read_parquet(jumps_fp).to_pandas()
adj = pl.read_parquet(adj_fp).to_pandas()

jumps["event_date"] = pd.to_datetime(jumps["event_date"])
jumps["official_date"] = pd.to_datetime(jumps["official_date"], errors="coerce")
jumps["abs_days_to_official"] = (jumps["event_date"] - jumps["official_date"]).abs().dt.days
adj["date"] = pd.to_datetime(adj["date"])

matched = jumps[jumps["jump_class"] == "split_matched"].copy().sort_values(["factor_rel_error", "jump_factor_abs"], ascending=[True, False])
unmatched = jumps[jumps["jump_class"] == "split_like_unmatched"].copy().sort_values(["factor_rel_error", "jump_factor_abs"], ascending=[True, False])

if matched.empty:
    raise RuntimeError("No hay casos split_matched en la corrida actual")
if unmatched.empty:
    raise RuntimeError("No hay casos split_like_unmatched en la corrida actual")


def _label(r):
    od = r["official_date"].date().isoformat() if pd.notna(r["official_date"]) else "NA"
    return (
        f"{r['ticker']} | ev={r['event_date'].date()} | off={od} | "
        f"jf={r['jump_factor_abs']:.2f} | err={r['factor_rel_error']:.3f}"
    )


def _plot_one(ax_top, ax_bot, r):
    tk = r["ticker"]
    ev_dt = r["event_date"]
    off_dt = r["official_date"]

    s = adj[adj["ticker"] == tk].sort_values("date").copy()
    if s.empty:
        ax_top.text(0.5, 0.5, f"Sin serie para {tk}", ha="center", va="center")
        return

    w = s[(s["date"] >= ev_dt - pd.Timedelta(days=45)) & (s["date"] <= ev_dt + pd.Timedelta(days=45))].copy()
    if w.empty:
        w = s.tail(120).copy()

    ax_top.plot(w["date"], w["mid_close_raw"], lw=1.4, label="mid_close_raw")
    ax_top.plot(w["date"], w["mid_close_adjusted"], lw=1.2, ls=":", label="mid_close_adjusted")
    ax_top.axvline(ev_dt, color="red", ls="--", lw=1.3, label=f"detected_jump={ev_dt.date()}")
    if pd.notna(off_dt):
        ax_top.axvline(off_dt, color="green", ls="-.", lw=1.3, label=f"official_split={off_dt.date()}")

    ax_top.set_title(f"{tk} | {r['jump_class']}")
    ax_top.grid(alpha=0.22)
    ax_top.legend(loc="best")

    txt = (
        f"jump_factor_abs={r['jump_factor_abs']:.4f} | nearest_split_factor={r['nearest_split_factor']:.4f}\n"
        f"factor_rel_error={r['factor_rel_error']:.4f} | match_near_5d={bool(r['match_near_5d'])}\n"
        f"abs_days_to_official={r['abs_days_to_official'] if pd.notna(r['abs_days_to_official']) else 'NA'}"
    )
    ax_top.text(0.01, 0.03, txt, transform=ax_top.transAxes, fontsize=9,
                bbox=dict(facecolor="white", alpha=0.75, edgecolor="gray"))

    rw = w.copy()
    rw["ret_raw"] = rw["mid_close_raw"].pct_change()
    colors = ["#d62728" if x < 0 else "#4f7ea8" for x in rw["ret_raw"].fillna(0.0)]
    ax_bot.bar(rw["date"], rw["ret_raw"], width=1.0, color=colors, alpha=0.85)
    ax_bot.axhline(0, color="black", lw=0.8)
    ax_bot.axvline(ev_dt, color="red", ls="--", lw=1.2)
    if pd.notna(off_dt):
        ax_bot.axvline(off_dt, color="green", ls="-.", lw=1.2)
    ax_bot.set_title("ret_raw diario")
    ax_bot.grid(alpha=0.22)


def render(m_idx=0, u_idx=0):
    r_m = matched.iloc[int(m_idx)]
    r_u = unmatched.iloc[int(u_idx)]

    fig, axes = plt.subplots(2, 2, figsize=(16, 9), sharex=False)
    _plot_one(axes[0, 0], axes[1, 0], r_m)
    _plot_one(axes[0, 1], axes[1, 1], r_u)
    fig.suptitle("Split oficial que matchea vs split-like no matcheado", y=1.02)
    fig.tight_layout()
    plt.show()

    display(Markdown(
        f"**Run:** `{run_dir.name}`  \n"
        f"**Matched:** `{_label(r_m)}`  \n"
        f"**Unmatched:** `{_label(r_u)}`"
    ))


if widgets is None:
    display(Markdown("ipywidgets no disponible. Mostrando primer par."))
    render(0, 0)
else:
    m_opts = [( _label(r), i) for i, (_, r) in enumerate(matched.head(300).iterrows())]
    u_opts = [( _label(r), i) for i, (_, r) in enumerate(unmatched.head(300).iterrows())]

    m_dd = widgets.Dropdown(options=m_opts, value=0, description="Matched")
    u_dd = widgets.Dropdown(options=u_opts, value=0, description="Unmatched")
    btn = widgets.Button(description="Renderizar")
    out = widgets.Output()

    def _run(_=None):
        with out:
            out.clear_output(wait=True)
            render(m_dd.value, u_dd.value)

    m_dd.observe(lambda ch: _run() if ch.get("name") == "value" else None, names="value")
    u_dd.observe(lambda ch: _run() if ch.get("name") == "value" else None, names="value")
    btn.on_click(_run)

    display(Markdown("### Step16L | Comparativa visual matched vs unmatched"))
    display(widgets.HBox([m_dd, u_dd, btn]))
    display(out)
    _run()