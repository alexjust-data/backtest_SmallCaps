from pathlib import Path
from datetime import timedelta, datetime, timezone
import random

import matplotlib.pyplot as plt
import polars as pl
from IPython.display import display, Markdown, Image as IPyImage

try:
    import ipywidgets as widgets
except Exception:
    widgets = None

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")

REPAIR_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "repair_queue"
queue_candidates = sorted(REPAIR_ROOT.glob("step15_repair_queue_*/repair_queue_v1.parquet"), key=lambda p: p.stat().st_mtime)
if not queue_candidates:
    raise FileNotFoundError("No existe repair_queue_v1.parquet (ejecuta Paso 15)")
queue_fp = queue_candidates[-1]
queue = pl.read_parquet(queue_fp)

queue_p = queue.filter(pl.col("priority_bucket").is_in(["P2", "P3"]))
if queue_p.height == 0:
    raise RuntimeError("No hay tickers P2/P3 en la cola actual")

out_dir = REPAIR_ROOT / f"step15_visual_evidence_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
out_dir.mkdir(parents=True, exist_ok=True)


def _quote_month_days(ticker: str, year: int, month: int):
    roots = [
        DATA_ROOT / "quotes_p95_2004_2018",
        DATA_ROOT / "quotes_p95_2019_2025",
        DATA_ROOT / "quotes_p95",
    ]
    best_days = 0
    best_root = None
    for r in roots:
        p = r / ticker / f"year={year}" / f"month={month:02d}"
        if p.exists():
            n_days = sum(1 for _ in p.glob("day=*/quotes.parquet"))
            if n_days > best_days:
                best_days = n_days
                best_root = r
    return best_days, best_root


def _ohlcv_month_path(ticker: str, year: int, month: int):
    for e in [DATA_ROOT / "ohlcv_intraday_1m" / "2004_2018", DATA_ROOT / "ohlcv_intraday_1m" / "2019_2025"]:
        p = e / ticker / f"year={year}" / f"month={month:02d}" / "minute.parquet"
        if p.exists():
            return p
    return None


def overlap_month_table(ticker: str) -> pl.DataFrame:
    rows = []
    year_candidates = set()
    for r in [DATA_ROOT / "quotes_p95_2004_2018", DATA_ROOT / "quotes_p95_2019_2025", DATA_ROOT / "quotes_p95"]:
        tp = r / ticker
        if tp.exists():
            for y in tp.glob("year=*"):
                if y.is_dir():
                    try:
                        year_candidates.add(int(y.name.split("=", 1)[1]))
                    except Exception:
                        pass

    for y in sorted(year_candidates):
        for m in range(1, 13):
            q_days, q_root = _quote_month_days(ticker, y, m)
            o_fp = _ohlcv_month_path(ticker, y, m)
            has_ohlcv = o_fp is not None
            overlap = (q_days > 0) and has_ohlcv
            if overlap or q_days > 0 or has_ohlcv:
                rows.append({
                    "ticker": ticker,
                    "year": y,
                    "month": m,
                    "quote_days": q_days,
                    "has_ohlcv": has_ohlcv,
                    "overlap": overlap,
                    "quote_root": str(q_root) if q_root is not None else None,
                    "ohlcv_fp": str(o_fp) if o_fp is not None else None,
                })
    if not rows:
        return pl.DataFrame({
            "ticker": [],
            "year": [],
            "month": [],
            "quote_days": [],
            "has_ohlcv": [],
            "overlap": [],
            "quote_root": [],
            "ohlcv_fp": [],
        })
    return pl.DataFrame(rows).sort(["year", "month"])


def _build_intraday_for_month_day(ticker: str, year: int, month: int, day: int, quote_root: Path, ohlcv_fp: Path):
    q_fp = quote_root / ticker / f"year={year}" / f"month={month:02d}" / f"day={day:02d}" / "quotes.parquet"
    if not q_fp.exists():
        return None

    o = (
        pl.read_parquet(ohlcv_fp)
        .filter(pl.col("date") == pl.date(year, month, day))
        .select(["timestamp", "low", "high", "close"])
        .with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
        .sort("timestamp")
    )
    if o.height == 0:
        return None

    q0 = pl.read_parquet(q_fp)
    if "participant_timestamp" in q0.columns:
        ts_expr = pl.from_epoch("participant_timestamp", time_unit="ns").dt.cast_time_unit("ms")
    elif "timestamp" in q0.columns:
        ts_expr = pl.from_epoch("timestamp", time_unit="ns").dt.cast_time_unit("ms")
    else:
        return None

    q = (
        q0.with_columns([
            ts_expr.alias("ts_ms"),
            ((pl.col("bid_price") + pl.col("ask_price")) / 2.0).alias("mid"),
        ])
        .filter(pl.col("mid").is_finite())
    )
    if q.height == 0:
        return None

    q1 = (
        q.with_columns(pl.col("ts_ms").dt.truncate("1m").alias("minute_ts"))
        .group_by("minute_ts")
        .agg([
            pl.col("mid").last().alias("last_mid_1m"),
            pl.col("mid").min().alias("q_low"),
            pl.col("mid").max().alias("q_high"),
            pl.len().alias("n_ticks"),
        ])
        .sort("minute_ts")
    )

    j = o.join(q1, left_on="timestamp", right_on="minute_ts", how="inner")
    if j.height < 30:
        return None

    j = j.with_columns((pl.col("close") - pl.col("last_mid_1m")).abs().alias("abs_err"))
    rep = j.sort("abs_err", descending=True).head(1)
    return {"joined": j, "ticks": q.select(["ts_ms", "mid"]).sort("ts_ms"), "rep_ts": rep["timestamp"][0]}


def render_ticker(group_label: str, ticker: str):
    row = queue_p.filter((pl.col("priority_bucket") == group_label) & (pl.col("ticker") == ticker)).head(1)
    if row.height == 0:
        display(Markdown(f"No se encontró ticker `{ticker}` en `{group_label}`"))
        return

    r = row.to_dicts()[0]
    summary_txt = (
        f"- overlap_months_total: `{r.get('overlap_months_total')}`  \n"
        f"- overlap_quote_days_total: `{r.get('overlap_quote_days_total')}`  \n"
        f"- year_gap_o_first_minus_q_last: `{r.get('year_gap_o_first_minus_q_last')}`"
    )
    display(Markdown(f"### {ticker} | {group_label} | cause={r.get('repair_cause')}"))
    display(Markdown(summary_txt))

    mdf = overlap_month_table(ticker)
    ov = mdf.filter(pl.col("overlap") == True)
    if ov.height == 0:
        display(Markdown("No hay meses de solape quotes+OHLCV para este ticker."))
        return

    ov_pd = ov.to_pandas()
    ov_pd["ym"] = ov_pd["year"].astype(str) + "-" + ov_pd["month"].astype(str).str.zfill(2)

    plt.figure(figsize=(12, 4.2))
    plt.bar(ov_pd["ym"], ov_pd["quote_days"], color="#4f7ea8")
    plt.xticks(rotation=75, fontsize=8)
    plt.ylabel("quote_days in overlap month")
    plt.title(f"{ticker} | Overlap months evidence ({group_label})")
    plt.tight_layout()
    fp_months = out_dir / f"{ticker}_{group_label}_overlap_months.png"
    plt.savefig(fp_months, dpi=130)
    plt.close()
    display(Markdown("#### Evidencia 1: meses con solape y días efectivos"))
    display(IPyImage(filename=str(fp_months)))

    target = ov.sort("quote_days").head(1).to_dicts()[0]
    year = int(target["year"])
    month = int(target["month"])
    quote_root = Path(target["quote_root"]) if target.get("quote_root") else None
    ohlcv_fp = Path(target["ohlcv_fp"]) if target.get("ohlcv_fp") else None
    if quote_root is None or ohlcv_fp is None:
        return

    q_month = quote_root / ticker / f"year={year}" / f"month={month:02d}"
    days = sorted([int(p.name.split("=", 1)[1]) for p in q_month.glob("day=*") if p.is_dir()])

    intraday = None
    chosen_day = None
    for d in days:
        intraday = _build_intraday_for_month_day(ticker, year, month, d, quote_root, ohlcv_fp)
        if intraday is not None:
            chosen_day = d
            break

    if intraday is None:
        display(Markdown("Mes con solape encontrado, pero sin día utilizable para chart intradía."))
        return

    rep_ts = intraday["rep_ts"]
    joined = intraday["joined"].to_pandas()

    fig, ax = plt.subplots(figsize=(12, 4.2))
    ax.plot(joined["timestamp"], joined["last_mid_1m"], label="last_mid_1m", linewidth=1.3)
    ax.fill_between(joined["timestamp"], joined["low"], joined["high"], alpha=0.22, label="OHLCV [low,high]")
    ax.axvline(rep_ts, color="red", linestyle="--", linewidth=1.2, label="representative_minute")
    ax.set_title(f"{ticker} | {group_label} | {year}-{month:02d}-{chosen_day:02d} full-day")
    ax.legend(loc="best")
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fp_full = out_dir / f"{ticker}_{group_label}_full_day.png"
    fig.savefig(fp_full, dpi=130)
    plt.close(fig)

    left = rep_ts - timedelta(minutes=8)
    right = rep_ts + timedelta(minutes=8)
    zj = intraday["joined"].filter((pl.col("timestamp") >= left) & (pl.col("timestamp") <= right)).to_pandas()
    zt = intraday["ticks"].filter((pl.col("ts_ms") >= left) & (pl.col("ts_ms") <= right)).to_pandas()

    fig, ax = plt.subplots(figsize=(12, 4.2))
    ax.plot(zj["timestamp"], zj["last_mid_1m"], label="last_mid_1m", linewidth=1.5)
    ax.fill_between(zj["timestamp"], zj["low"], zj["high"], alpha=0.22, label="OHLCV [low,high]")
    ax.scatter(zt["ts_ms"], zt["mid"], s=8, alpha=0.75, label="mid ticks")
    ax.axvline(rep_ts, color="red", linestyle="--", linewidth=1.2, label="representative_minute")
    ax.set_title(f"{ticker} | {group_label} | zoom around representative minute")
    ax.legend(loc="best")
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fp_zoom = out_dir / f"{ticker}_{group_label}_zoom_ticks.png"
    fig.savefig(fp_zoom, dpi=130)
    plt.close(fig)

    display(Markdown("#### Evidencia 2: comparación intradía (full day + zoom)"))
    display(IPyImage(filename=str(fp_full)))
    display(IPyImage(filename=str(fp_zoom)))


p2_tickers = queue_p.filter(pl.col("priority_bucket") == "P2").select("ticker").to_series().to_list()
p3_tickers = queue_p.filter(pl.col("priority_bucket") == "P3").select("ticker").to_series().to_list()

if widgets is None:
    print("ipywidgets no disponible. Muestra aleatoria fija.")
    random.seed(42)
    if p2_tickers:
        render_ticker("P2", random.choice(p2_tickers))
    if p3_tickers:
        render_ticker("P3", random.choice(p3_tickers))
else:
    group_dd = widgets.Dropdown(
        options=[("P2 - low_overlap_months", "P2"), ("P3 - low_overlap_days", "P3")],
        value="P2",
        description="Grupo",
    )
    ticker_dd = widgets.Dropdown(
        options=p2_tickers[:500],
        value=(p2_tickers[0] if p2_tickers else None),
        description="Ticker",
    )
    btn = widgets.Button(description="Renderizar")
    out = widgets.Output()

    def _refresh_ticker_options(*_):
        opts = p2_tickers if group_dd.value == "P2" else p3_tickers
        ticker_dd.options = opts[:500]
        ticker_dd.value = opts[0] if opts else None

    def _run(_):
        with out:
            out.clear_output(wait=True)
            if ticker_dd.value is None:
                print("Sin ticker disponible")
                return
            render_ticker(group_dd.value, ticker_dd.value)

    group_dd.observe(_refresh_ticker_options, names="value")
    btn.on_click(_run)

    display(Markdown(f"### Paso 15B - Visual evidence widget\nSalida de imágenes: `{out_dir}`"))
    display(widgets.HBox([group_dd, ticker_dd, btn]))
    display(out)

    with out:
        if ticker_dd.value is not None:
            render_ticker(group_dd.value, ticker_dd.value)