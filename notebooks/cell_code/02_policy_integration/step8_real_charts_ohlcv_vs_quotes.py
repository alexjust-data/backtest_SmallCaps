from pathlib import Path
from datetime import timedelta

import matplotlib.pyplot as plt
import polars as pl
from IPython.display import display, Markdown, Image

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")
OUT_DIR = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "visual_ohlcv_vs_quotes" / "step8_real_charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = ["AABA", "CRMD"]
CASE_HINTS = {
    "AABA": {"year": "2019", "month": "01", "day": "02"},
    "CRMD": {"year": "2019", "month": "02", "day": "14"},
}
MAX_SCAN_DAYS_PER_TICKER = 30

QUOTE_ROOTS = [
    DATA_ROOT / "quotes_p95",
    DATA_ROOT / "quotes_p95_2019_2025",
    DATA_ROOT / "quotes_p95_2004_2018",
]


def _find_quote_file(ticker: str, year: str, month: str, day: str):
    for qroot in QUOTE_ROOTS:
        q_fp = qroot / ticker / f"year={year}" / f"month={month}" / f"day={day}" / "quotes.parquet"
        if q_fp.exists():
            return q_fp
    return None


def _find_ohlcv_file(ticker: str, year: str, month: str):
    for era in ["2019_2025", "2004_2018"]:
        fp = DATA_ROOT / "ohlcv_intraday_1m" / era / ticker / f"year={year}" / f"month={month}" / "minute.parquet"
        if fp.exists():
            return fp
    return None


def _build_case_for_day(ticker: str, year: str, month: str, day: str):
    q_fp = _find_quote_file(ticker, year, month, day)
    ohlcv_fp = _find_ohlcv_file(ticker, year, month)
    if q_fp is None or ohlcv_fp is None:
        return None

    # timestamp can be participant_timestamp (ns) or timestamp (ns/us/ms)
    q0 = pl.read_parquet(q_fp)
    if "participant_timestamp" in q0.columns:
        ts_expr = pl.from_epoch("participant_timestamp", time_unit="ns").dt.cast_time_unit("ms")
    elif "timestamp" in q0.columns:
        ts_expr = pl.from_epoch("timestamp", time_unit="ns").dt.cast_time_unit("ms")
    else:
        return None

    o = (
        pl.read_parquet(ohlcv_fp)
        .filter(pl.col("date") == pl.date(int(year), int(month), int(day)))
        .select(["timestamp", "low", "high", "close"])
        .with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
        .sort("timestamp")
    )
    if o.height == 0:
        return None

    q = (
        q0.with_columns([
            ts_expr.alias("ts_ms"),
            ((pl.col("bid_price") + pl.col("ask_price")) / 2.0).alias("mid"),
        ])
        .filter(pl.col("mid").is_finite())
        .select(["ts_ms", "mid"])
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
    if j.height < 120:
        return None

    j = j.with_columns((pl.col("close") - pl.col("last_mid_1m")).abs().alias("abs_err"))
    rep = j.sort("abs_err", descending=True).head(1)

    return {
        "ticker": ticker,
        "year": year,
        "month": month,
        "day": day,
        "joined": j,
        "ticks": q.sort("ts_ms"),
        "rep_ts": rep["timestamp"][0],
        "rep_err": float(rep["abs_err"][0]),
    }


def find_overlap_day(ticker: str):
    hint = CASE_HINTS.get(ticker)
    if hint is not None:
        c = _build_case_for_day(ticker, hint["year"], hint["month"], hint["day"])
        if c is not None:
            print(f"{ticker}: usando CASE_HINTS {hint['year']}-{hint['month']}-{hint['day']}")
            return c
        print(f"{ticker}: CASE_HINTS no util, fallback a escaneo")

    scanned = 0
    for q_root in QUOTE_ROOTS:
        q_ticker_root = q_root / ticker
        if not q_ticker_root.exists():
            continue

        for y in sorted([p for p in q_ticker_root.glob("year=*") if p.is_dir()]):
            year = y.name.split("=", 1)[1]
            for m in sorted([p for p in y.glob("month=*") if p.is_dir()]):
                month = m.name.split("=", 1)[1]
                for d in sorted([p for p in m.glob("day=*") if p.is_dir()]):
                    day = d.name.split("=", 1)[1]
                    scanned += 1
                    if scanned > MAX_SCAN_DAYS_PER_TICKER:
                        print(f"{ticker}: limite de escaneo alcanzado ({MAX_SCAN_DAYS_PER_TICKER})")
                        return None
                    c = _build_case_for_day(ticker, year, month, day)
                    if c is not None:
                        return c
    return None


def render_case(case):
    ticker = case["ticker"]
    joined = case["joined"].to_pandas()
    rep_ts = case["rep_ts"]

    fig, ax = plt.subplots(figsize=(12, 4.6))
    ax.plot(joined["timestamp"], joined["last_mid_1m"], label="last_mid_1m", linewidth=1.3)
    ax.fill_between(joined["timestamp"], joined["low"], joined["high"], alpha=0.22, label="OHLCV [low,high]")
    ax.axvline(rep_ts, color="red", linestyle="--", linewidth=1.3, label="representative_minute")
    ax.set_title(f"{ticker} | Intraday example (full day)")
    ax.legend(loc="lower right")
    ax.grid(alpha=0.22)
    fp1 = OUT_DIR / f"{ticker}_full_day.png"
    fig.tight_layout()
    fig.savefig(fp1, dpi=130)
    plt.close(fig)

    left = rep_ts - timedelta(minutes=8)
    right = rep_ts + timedelta(minutes=8)
    zj = case["joined"].filter((pl.col("timestamp") >= left) & (pl.col("timestamp") <= right)).to_pandas()
    zt = case["ticks"].filter((pl.col("ts_ms") >= left) & (pl.col("ts_ms") <= right)).to_pandas()

    fig, ax = plt.subplots(figsize=(12, 4.6))
    ax.plot(zj["timestamp"], zj["last_mid_1m"], label="last_mid_1m", linewidth=1.5)
    ax.fill_between(zj["timestamp"], zj["low"], zj["high"], alpha=0.22, label="OHLCV [low,high]")
    ax.scatter(zt["ts_ms"], zt["mid"], s=8, alpha=0.8, label="mid ticks")
    ax.axvline(rep_ts, color="red", linestyle="--", linewidth=1.3, label="representative_minute")
    ax.set_title(f"{ticker} | Tick-level zoom around representative minute")
    ax.legend(loc="upper right")
    ax.grid(alpha=0.22)
    fp2 = OUT_DIR / f"{ticker}_zoom_ticks.png"
    fig.tight_layout()
    fig.savefig(fp2, dpi=130)
    plt.close(fig)

    display(Markdown(f"### {ticker} | {case['year']}-{case['month']}-{case['day']} | rep_minute={rep_ts} | abs_err={case['rep_err']:.6f}"))
    display(Image(filename=str(fp1)))
    display(Image(filename=str(fp2)))


cases = []
for t in TICKERS:
    c = find_overlap_day(t)
    if c is None:
        print(f"[WARN] sin dia util para {t}")
    else:
        cases.append(c)

print(f"OUT_DIR: {OUT_DIR}")
print(f"cases_found: {len(cases)} / {len(TICKERS)}")
for c in cases:
    render_case(c)
