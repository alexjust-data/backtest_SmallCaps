from pathlib import Path
import ast
import json
import re

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    import ipywidgets as widgets
    from IPython.display import display, clear_output
except Exception:
    widgets = None
    def display(*args, **kwargs):
        for x in args:
            print(x)
    def clear_output(*args, **kwargs):
        return None

RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", RUN_DIR / "retry_queue_quotes_strict_current.csv"))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", RUN_DIR / "retry_frozen_quotes_strict.csv"))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", RUN_DIR / "run_config_quotes_strict.json"))
OUT_DIR = Path(globals().get("OUT_DIR", RUN_DIR / "agent03_outputs"))
QUOTES_ROOT = Path(globals().get("QUOTES_ROOT", r"D:\quotes\__pruebas__\final_preprod"))
REF_SPLITS_ROOT = Path(globals().get("REF_SPLITS_ROOT", r"D:\reference\splits"))
REF_EVENTS_ROOT = Path(globals().get("REF_EVENTS_ROOT", r"D:\reference\events"))
REF_OVERVIEW_ROOT = Path(globals().get("REF_OVERVIEW_ROOT", r"D:\reference\overview"))
OHLCV_DAILY_ROOT = Path(globals().get("OHLCV_DAILY_ROOT", r"D:\ohlcv_daily"))
OHLCV_1M_ROOT = Path(globals().get("OHLCV_1M_ROOT", r"D:\ohlcv_1m"))
CAUSAL_OUT_DIR = Path(globals().get("CAUSAL_OUT_DIR", OUT_DIR / "causal_hypotheses"))
CAUSAL_OUT_DIR.mkdir(parents=True, exist_ok=True)
TOP_FILES = int(globals().get("TOP_FILES", 20))
TOP_TICKERS = int(globals().get("TOP_TICKERS", 15))
TOP_PATHOLOGY_FILES = int(globals().get("TOP_PATHOLOGY_FILES", 150))
QUOTE_VIEW_MAX_ROWS = int(globals().get("QUOTE_VIEW_MAX_ROWS", 20000))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))
_QUOTE_CACHE = {}

def _show(df, rows=200):
    with pd.option_context("display.max_rows", rows, "display.max_columns", 200, "display.width", 220):
        display(df)

def _safe_csv(path):
    if not Path(path).exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()

def _parts(series):
    pat = re.compile(r"[/\\](?P<ticker>[^/\\]+)[/\\]year=(?P<year>\d{4})[/\\]month=(?P<month>\d{2})[/\\]day=(?P<day>\d{2})[/\\]quotes\.parquet$")
    out = series.astype(str).str.extract(pat)
    out["date"] = pd.to_datetime(out["year"] + "-" + out["month"] + "-" + out["day"], errors="coerce")
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["month"] = pd.to_numeric(out["month"], errors="coerce")
    return out

def _read_parquet(path, cols=None):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    pf = pq.ParquetFile(p)
    use = None if cols is None else [c for c in cols if c in pf.schema_arrow.names]
    try:
        return pf.read(columns=use).to_pandas()
    except Exception:
        pdf = pf.read().to_pandas()
        return pdf if use is None else pdf[[c for c in use if c in pdf.columns]]

def _load_splits(ticker):
    df = _read_parquet(REF_SPLITS_ROOT / f"ticker={ticker}" / f"splits_{ticker}.parquet")
    if df.empty or "execution_date" not in df.columns:
        return pd.DataFrame(columns=["ticker","execution_date","split_from","split_to","split_ratio"])
    df["ticker"] = ticker
    df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce")
    df["split_from"] = pd.to_numeric(df.get("split_from"), errors="coerce")
    df["split_to"] = pd.to_numeric(df.get("split_to"), errors="coerce")
    df["split_ratio"] = df["split_from"] / df["split_to"]
    return df[["ticker","execution_date","split_from","split_to","split_ratio"]].dropna(subset=["execution_date"])

def _load_events(ticker):
    df = _read_parquet(REF_EVENTS_ROOT / f"ticker={ticker}" / f"events_{ticker}.parquet")
    rows = []
    for items in ([] if df.empty else df.get("events", [])):
        if not isinstance(items, list):
            continue
        for ev in items:
            if isinstance(ev, dict):
                rows.append({
                    "ticker": ticker,
                    "event_date": pd.to_datetime(ev.get("date"), errors="coerce"),
                    "event_type": str(ev.get("type")) if ev.get("type") is not None else None,
                    "event_payload": json.dumps(ev, ensure_ascii=False),
                })
    out = pd.DataFrame(rows)
    return out if out.empty else out.dropna(subset=["event_date"])

def _load_overview(ticker):
    d = REF_OVERVIEW_ROOT / f"ticker={ticker}"
    files = sorted(d.glob("*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ticker"])
    df = _read_parquet(files[-1])
    keep = ["ticker","market_cap","weighted_shares_outstanding","share_class_shares_outstanding","round_lot","list_date","primary_exchange","market","type","active","name"]
    if df.empty:
        return pd.DataFrame(columns=["ticker"])
    out = df[[c for c in keep if c in df.columns]].copy()
    if "ticker" not in out.columns:
        out["ticker"] = ticker
    out["ticker"] = out["ticker"].astype(str)
    if "list_date" in out.columns:
        out["list_date"] = pd.to_datetime(out["list_date"], errors="coerce")
    return out.head(1)

def _load_daily(ticker, year):
    p = OHLCV_DAILY_ROOT / f"ticker={ticker}" / f"year={year}" / f"day_aggs_{ticker}_{year}.parquet"
    df = _read_parquet(p)
    if df.empty:
        return pd.DataFrame(columns=["ticker","date"])
    df["ticker"] = ticker
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def _load_minute(ticker, year, month):
    p = OHLCV_1M_ROOT / f"ticker={ticker}" / f"year={year}" / f"month={month:02d}" / f"minute_aggs_{ticker}_{year}_{month:02d}.parquet"
    df = _read_parquet(p)
    if df.empty:
        return pd.DataFrame(columns=["ticker","date"])
    df["ticker"] = ticker
    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    else:
        df["ts_utc"] = pd.to_datetime(df["t"], unit="ms", utc=True, errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce") if "date" in df.columns else df["ts_utc"].dt.tz_convert("America/New_York").dt.floor("D").dt.tz_localize(None)
    return df

def _dist(target, candidates):
    if pd.isna(target) or len(candidates) == 0:
        return np.nan
    c = pd.to_datetime(pd.Series(candidates), errors="coerce").dropna()
    if c.empty:
        return np.nan
    target = pd.Timestamp(target)
    d = (c - target).dt.days.abs()
    return float(d.min()) if len(d) else np.nan

def _session(ts_utc):
    ts_local = ts_utc.dt.tz_convert("America/New_York")
    mins = ts_local.dt.hour * 60 + ts_local.dt.minute
    return pd.Series(np.select([mins < 570, mins < 960, mins <= 1200], ["premarket", "market", "afterhours"], default="outside"), index=ts_utc.index)

def _quote_metrics(path):
    df = _read_parquet(path, ["timestamp","bid_price","ask_price","bid_exchange","ask_exchange","tape"])
    if df.empty:
        return {}
    df["bid_price"] = pd.to_numeric(df["bid_price"], errors="coerce")
    df["ask_price"] = pd.to_numeric(df["ask_price"], errors="coerce")
    df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="ns", utc=True, errors="coerce")
    df["crossed"] = (df["bid_price"] > df["ask_price"]) & df["bid_price"].notna() & df["ask_price"].notna()
    df["ask_zero"] = df["ask_price"].fillna(np.nan) <= 0
    df["bid_zero"] = df["bid_price"].fillna(np.nan) <= 0
    df["ask_absurd"] = df["ask_price"].fillna(np.nan) >= 100000
    df["bid_absurd"] = df["bid_price"].fillna(np.nan) >= 100000
    df["any_placeholder"] = df[["ask_zero","bid_zero","ask_absurd","bid_absurd"]].any(axis=1)
    df["session"] = _session(df["ts_utc"]) if df["ts_utc"].notna().any() else "unknown"
    return {
        "rows_scanned": int(len(df)),
        "cross_rows_scanned": int(df["crossed"].sum()),
        "placeholder_rows": int(df["any_placeholder"].sum()),
        "ask_zero_rows": int(df["ask_zero"].sum()),
        "bid_zero_rows": int(df["bid_zero"].sum()),
        "ask_absurd_rows": int(df["ask_absurd"].sum()),
        "bid_absurd_rows": int(df["bid_absurd"].sum()),
    }

events = _safe_csv(EVENTS_CSV)
retry_df = _safe_csv(RETRY_QUEUE_CSV)
retry_frozen_df = _safe_csv(RETRY_FROZEN_CSV)
if events.empty:
    raise RuntimeError(f"No events found: {EVENTS_CSV}")
cfg = json.loads(RUN_CONFIG_JSON.read_text(encoding="utf-8")) if RUN_CONFIG_JSON.exists() else {}
max_cross = float(cfg.get("max_crossed_ratio_pct", 0.8))
hard_cross = float(cfg.get("hard_fail_crossed_pct", 5.0))
parts = _parts(events["file"])
events = pd.concat([events.reset_index(drop=True), parts], axis=1)
events = events[events["ticker"].notna() & events["date"].notna()].copy()
events["ticker"] = events["ticker"].astype(str)
events["severity"] = events.get("severity", "UNKNOWN").astype(str)
events["rows"] = pd.to_numeric(events.get("rows"), errors="coerce").fillna(0.0)
events["crossed_rows"] = pd.to_numeric(events.get("crossed_rows"), errors="coerce").fillna(0.0)
events["crossed_ratio_pct"] = pd.to_numeric(events.get("crossed_ratio_pct"), errors="coerce").fillna(0.0)
events["issue_list"] = events.get("issues", pd.Series([None] * len(events))).apply(lambda v: [] if pd.isna(v) else ast.literal_eval(v) if isinstance(v, str) and v.startswith("[") else ([v] if pd.notna(v) else []))
events["warn_list"] = events.get("warns", pd.Series([None] * len(events))).apply(lambda v: [] if pd.isna(v) else ast.literal_eval(v) if isinstance(v, str) and v.startswith("[") else ([v] if pd.notna(v) else []))
events["soft_cross"] = events["warn_list"].apply(lambda xs: "crossed_rows_present_but_under_threshold" in set(map(str, xs)))
events["hard_threshold"] = events["issue_list"].apply(lambda xs: "crossed_ratio_gt_threshold" in set(map(str, xs)))
events["hard_cap"] = events["issue_list"].apply(lambda xs: "crossed_ratio_gt_hard_cap" in set(map(str, xs)))
events["retry_pending_flag"] = events["file"].astype(str).isin(set(retry_df.get("file", pd.Series(dtype=str)).dropna().astype(str)))
events["retry_frozen_flag"] = events["file"].astype(str).isin(set(retry_frozen_df.get("file", pd.Series(dtype=str)).dropna().astype(str)))

tickers = sorted(events["ticker"].unique().tolist())
splits_df = pd.concat([_load_splits(t) for t in tickers], ignore_index=True)
ref_events_df = pd.concat([_load_events(t) for t in tickers], ignore_index=True)
overview_df = pd.concat([_load_overview(t) for t in tickers], ignore_index=True)
for c in ["nearest_split_days","nearest_ticker_change_days","nearest_any_event_days"]:
    events[c] = np.nan
for ticker in tickers:
    m = events["ticker"] == ticker
    s = splits_df.loc[splits_df["ticker"] == ticker, "execution_date"] if not splits_df.empty else pd.Series(dtype="datetime64[ns]")
    e = ref_events_df.loc[ref_events_df["ticker"] == ticker] if not ref_events_df.empty else pd.DataFrame()
    tc = e.loc[e["event_type"].eq("ticker_change"), "event_date"] if not e.empty else pd.Series(dtype="datetime64[ns]")
    ae = e["event_date"] if not e.empty else pd.Series(dtype="datetime64[ns]")
    events.loc[m, "nearest_split_days"] = events.loc[m, "date"].apply(lambda d: _dist(d, s))
    events.loc[m, "nearest_ticker_change_days"] = events.loc[m, "date"].apply(lambda d: _dist(d, tc))
    events.loc[m, "nearest_any_event_days"] = events.loc[m, "date"].apply(lambda d: _dist(d, ae))
for d in [5,20,60]:
    events[f"split_win_{d}d"] = events["nearest_split_days"].le(d).fillna(False)
    events[f"ticker_change_win_{d}d"] = events["nearest_ticker_change_days"].le(d).fillna(False)
    events[f"any_event_win_{d}d"] = events["nearest_any_event_days"].le(d).fillna(False)
if not overview_df.empty:
    events = events.merge(overview_df, on="ticker", how="left")

daily_df = pd.concat([_load_daily(t, int(y)) for t, y in events[["ticker","year"]].dropna().drop_duplicates().itertuples(index=False)], ignore_index=True)
events["date"] = pd.to_datetime(events["date"], errors="coerce")
if not daily_df.empty:
    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    events = events.merge(daily_df[[c for c in ["ticker","date","o","h","l","c","v","vw","n"] if c in daily_df.columns]], on=["ticker","date"], how="left")
minute_daily_parts = []
needed_dates = events[["ticker", "date", "year", "month"]].dropna().copy()
needed_dates["date"] = pd.to_datetime(needed_dates["date"], errors="coerce")
needed_dates = needed_dates.dropna(subset=["date"])
for t, y, m in needed_dates[["ticker", "year", "month"]].drop_duplicates().itertuples(index=False):
    mdf = _load_minute(t, int(y), int(m))
    if mdf.empty:
        continue
    mdf["date"] = pd.to_datetime(mdf["date"], errors="coerce")
    wanted = set(needed_dates.loc[(needed_dates["ticker"] == t) & (needed_dates["year"] == y) & (needed_dates["month"] == m), "date"].dropna().tolist())
    if wanted:
        mdf = mdf[mdf["date"].isin(wanted)].copy()
    if mdf.empty:
        continue
    daily_min = mdf.groupby(["ticker","date"], dropna=False).agg(minute_active=("ts_utc", "size"), minute_v=("v","sum"), minute_n=("n","sum")).reset_index()
    minute_daily_parts.append(daily_min)
if minute_daily_parts:
    daily_min = pd.concat(minute_daily_parts, ignore_index=True)
    daily_min["date"] = pd.to_datetime(daily_min["date"], errors="coerce")
    events = events.merge(daily_min, on=["ticker","date"], how="left")

# H3 session decomposition on top severe/material files
session_rows = []
for _, r in events.sort_values(["hard_cap","hard_threshold","crossed_ratio_pct","rows"], ascending=[False,False,False,False]).head(TOP_PATHOLOGY_FILES).iterrows():
    q = _read_parquet(Path(str(r["file"])), ["timestamp","bid_price","ask_price"])
    if q.empty or "timestamp" not in q.columns:
        continue
    q["bid_price"] = pd.to_numeric(q.get("bid_price"), errors="coerce")
    q["ask_price"] = pd.to_numeric(q.get("ask_price"), errors="coerce")
    q["ts_utc"] = pd.to_datetime(q["timestamp"], unit="ns", utc=True, errors="coerce")
    q = q[q["ts_utc"].notna()].copy()
    if q.empty:
        continue
    q["session"] = _session(q["ts_utc"])
    q["crossed"] = (q["bid_price"] > q["ask_price"]) & q["bid_price"].notna() & q["ask_price"].notna()
    s = q.groupby("session", dropna=False).agg(rows_scanned=("session","size"), cross_rows=("crossed","sum")).reset_index()
    s["ticker"] = r["ticker"]
    s["date"] = r["date"]
    s["severity"] = r["severity"]
    s["crossed_ratio_pct_file_session"] = 100 * s["cross_rows"] / s["rows_scanned"].clip(lower=1)
    session_rows.append(s)
severity_by_session = pd.concat(session_rows, ignore_index=True) if session_rows else pd.DataFrame(columns=["ticker","date","severity","session","rows_scanned","cross_rows","crossed_ratio_pct_file_session"])

# H4 exchange/tape decomposition on top severe/material files
exchange_rows = []
for _, r in events.sort_values(["hard_cap","hard_threshold","crossed_ratio_pct","rows"], ascending=[False,False,False,False]).head(TOP_PATHOLOGY_FILES).iterrows():
    q = _read_parquet(Path(str(r["file"])), ["bid_exchange","ask_exchange","tape","bid_price","ask_price"])
    if q.empty:
        continue
    q["bid_price"] = pd.to_numeric(q.get("bid_price"), errors="coerce")
    q["ask_price"] = pd.to_numeric(q.get("ask_price"), errors="coerce")
    q["crossed"] = (q["bid_price"] > q["ask_price"]) & q["bid_price"].notna() & q["ask_price"].notna()
    q = q[q["crossed"]].copy()
    if q.empty:
        continue
    q["exchange_pair"] = q["bid_exchange"].astype("string") + "->" + q["ask_exchange"].astype("string")
    g = q.groupby(["exchange_pair","tape"], dropna=False).size().reset_index(name="cross_rows")
    g["ticker"] = r["ticker"]
    g["date"] = r["date"]
    g["severity"] = r["severity"]
    exchange_rows.append(g)
cross_exchange_pairs_global = pd.concat(exchange_rows, ignore_index=True) if exchange_rows else pd.DataFrame(columns=["exchange_pair","tape","cross_rows","ticker","date","severity"])

events["daily_close"] = pd.to_numeric(events.get("c"), errors="coerce")
events["daily_volume"] = pd.to_numeric(events.get("v"), errors="coerce")
events["daily_trades"] = pd.to_numeric(events.get("n"), errors="coerce")
events["minute_active"] = pd.to_numeric(events.get("minute_active"), errors="coerce")
events["rows_bin"] = pd.cut(events["rows"], [-0.1,20,100,500,np.inf], labels=["rows<20","20<=rows<100","100<=rows<500","rows>=500"])
events["price_bin"] = pd.cut(events["daily_close"], [-np.inf,1,5,10,20,np.inf], labels=["<1","1-5","5-10","10-20",">20"])
for col in ["daily_volume","daily_trades","minute_active"]:
    ser = pd.to_numeric(events[col], errors="coerce")
    try:
        events[f"{col}_quartile"] = pd.qcut(ser.rank(method="first"), 4, labels=["Q1_low","Q2","Q3","Q4_high"])
    except Exception:
        events[f"{col}_quartile"] = pd.Series([pd.NA] * len(events), dtype="string")

def _rate(df, gcol, label):
    g = df.groupby(gcol, dropna=False).agg(files=("file","size"), tickers=("ticker","nunique"), hard_threshold=("hard_threshold","sum"), hard_cap=("hard_cap","sum"), soft_cross=("soft_cross","sum"), retry_pending=("retry_pending_flag","sum"), retry_frozen=("retry_frozen_flag","sum"), median_cross=("crossed_ratio_pct","median"), p95_cross=("crossed_ratio_pct", lambda s: s.quantile(0.95))).reset_index().rename(columns={gcol: label})
    g["hard_threshold_rate_pct"] = 100 * g["hard_threshold"] / g["files"].clip(lower=1)
    g["hard_cap_rate_pct"] = 100 * g["hard_cap"] / g["files"].clip(lower=1)
    return g.sort_values(["hard_cap_rate_pct","hard_threshold_rate_pct","files"], ascending=[False,False,False])

window_rows = []
for base, name in [("split","split"),("ticker_change","ticker_change"),("any_event","any_event")]:
    for d in [5,20,60]:
        col = f"{base}_win_{d}d"
        for val, scope in [(True,"inside"),(False,"outside")]:
            sub = events[events[col].fillna(False) == val]
            if sub.empty:
                continue
            window_rows.append({"hypothesis": name, "window_days": d, "scope": scope, "files": len(sub), "tickers": sub["ticker"].nunique(), "hard_threshold": int(sub["hard_threshold"].sum()), "hard_cap": int(sub["hard_cap"].sum()), "soft_cross": int(sub["soft_cross"].sum()), "hard_threshold_rate_pct": 100 * sub["hard_threshold"].mean(), "hard_cap_rate_pct": 100 * sub["hard_cap"].mean(), "median_crossed_ratio_pct": float(sub["crossed_ratio_pct"].median()), "p95_crossed_ratio_pct": float(sub["crossed_ratio_pct"].quantile(0.95))})
window_df = pd.DataFrame(window_rows)
liq_price_summary = {
    "rows_bin": _rate(events, "rows_bin", "rows_bin"),
    "price_bin": _rate(events.dropna(subset=["price_bin"]), "price_bin", "price_bin"),
    "daily_volume_quartile": _rate(events.dropna(subset=["daily_volume_quartile"]), "daily_volume_quartile", "daily_volume_quartile"),
    "daily_trades_quartile": _rate(events.dropna(subset=["daily_trades_quartile"]), "daily_trades_quartile", "daily_trades_quartile"),
    "minute_active_quartile": _rate(events.dropna(subset=["minute_active_quartile"]), "minute_active_quartile", "minute_active_quartile"),
}

top_tickers = events.groupby("ticker", dropna=False).agg(files=("file","size"), retry_pending=("retry_pending_flag","sum"), retry_frozen=("retry_frozen_flag","sum"), hard_fail=("hard_threshold","sum"), hard_cap=("hard_cap","sum"), soft_cross=("soft_cross","sum"), max_crossed_ratio_pct=("crossed_ratio_pct","max"), p95_crossed_ratio_pct=("crossed_ratio_pct", lambda s: s.quantile(0.95)), median_daily_close=("daily_close","median"), median_daily_volume=("daily_volume","median"), median_daily_trades=("daily_trades","median"), median_minute_active=("minute_active","median"), nearest_split_days_min=("nearest_split_days","min"), nearest_ticker_change_days_min=("nearest_ticker_change_days","min"), market_cap=("market_cap","max")).reset_index()
coverage_csv = OUT_DIR / "coverage_by_ticker.csv"
if coverage_csv.exists():
    cov = _safe_csv(coverage_csv)
    if not cov.empty:
        cov["ticker"] = cov["ticker"].astype(str)
        top_tickers = top_tickers.merge(cov[["ticker","coverage_ratio_ok","missing_days_ok"]], on="ticker", how="left")
top_tickers = top_tickers.sort_values(["retry_frozen","hard_fail","hard_cap","max_crossed_ratio_pct","retry_pending"], ascending=[False,False,False,False,False]).reset_index(drop=True)
top_files = events.sort_values(["hard_cap","hard_threshold","crossed_ratio_pct","rows"], ascending=[False,False,False,False]).head(TOP_FILES).copy()
pathology_rows = []
for _, r in events.sort_values(["hard_cap","hard_threshold","crossed_ratio_pct","rows"], ascending=[False,False,False,False]).head(TOP_PATHOLOGY_FILES).iterrows():
    m = _quote_metrics(Path(str(r["file"])))
    if m:
        pathology_rows.append({"ticker": r["ticker"], "date": r["date"], "severity": r["severity"], "crossed_ratio_pct": r["crossed_ratio_pct"], **m})
pathology_df = pd.DataFrame(pathology_rows)

events.to_csv(CAUSAL_OUT_DIR / "causal_file_features.csv", index=False)
window_df.to_csv(CAUSAL_OUT_DIR / "causal_window_tests.csv", index=False)
for name, df in liq_price_summary.items():
    df.to_csv(CAUSAL_OUT_DIR / f"{name}.csv", index=False)
top_tickers.to_csv(CAUSAL_OUT_DIR / "causal_top_tickers.csv", index=False)
top_files.to_csv(CAUSAL_OUT_DIR / "causal_top_files.csv", index=False)
if not pathology_df.empty:
    pathology_df.to_csv(CAUSAL_OUT_DIR / "causal_quote_pathology_top_files.csv", index=False)
severity_by_session.to_csv(CAUSAL_OUT_DIR / "severity_by_session.csv", index=False)
cross_exchange_pairs_global.to_csv(CAUSAL_OUT_DIR / "cross_exchange_pairs_global.csv", index=False)

print("=== CAUSAL HYPOTHESES REVIEW ===")
print("Artefactos escritos en:", CAUSAL_OUT_DIR)
_show(pd.DataFrame([
    {"metric": "files_total", "value": len(events)},
    {"metric": "tickers_total", "value": events["ticker"].nunique()},
    {"metric": "hard_threshold_files", "value": int(events["hard_threshold"].sum())},
    {"metric": "hard_cap_files", "value": int(events["hard_cap"].sum())},
    {"metric": "soft_cross_files", "value": int(events["soft_cross"].sum())},
    {"metric": "near_split_20d_files", "value": int(events["split_win_20d"].sum())},
    {"metric": "near_ticker_change_20d_files", "value": int(events["ticker_change_win_20d"].sum())},
    {"metric": "pathology_files_scanned", "value": int(len(pathology_df))},
]))
print("\nH1 / corporate action windows:")
_show(window_df.sort_values(["hypothesis","window_days","scope"]))
print("\nH2 / H5 / rows, liquidez y precio:")
for label, df in liq_price_summary.items():
    print(f"\n{label}:")
    _show(df)
print(f"\nTop {TOP_TICKERS} tickers para estudio profundo causal:")
_show(top_tickers.head(TOP_TICKERS))
print(f"\nTop {TOP_FILES} files para estudio profundo causal:")
_show(top_files[["ticker","date","severity","crossed_ratio_pct","crossed_rows","rows","retry_pending_flag","retry_frozen_flag","nearest_split_days","nearest_ticker_change_days","daily_close","daily_volume","daily_trades","minute_active","file"]].head(TOP_FILES))
if not pathology_df.empty:
    print("\nH7 / placeholders o quotes absurdos en top files severos:")
    _show(pathology_df.sort_values(["placeholder_rows","crossed_ratio_pct"], ascending=[False,False]).head(30))

print("\nH3 / severidad agregada por session en top files:")
if not severity_by_session.empty:
    sess_view = severity_by_session.groupby("session", dropna=False).agg(files=("date","count"), tickers=("ticker","nunique"), rows_scanned=("rows_scanned","sum"), cross_rows=("cross_rows","sum")).reset_index()
    sess_view["crossed_ratio_pct"] = 100 * sess_view["cross_rows"] / sess_view["rows_scanned"].clip(lower=1)
    _show(sess_view.sort_values("crossed_ratio_pct", ascending=False))
else:
    print("Sin datos de session decomposition.")

print("\nH4 / exchange pairs globales en top files:")
if not cross_exchange_pairs_global.empty:
    pair_view = cross_exchange_pairs_global.groupby(["exchange_pair","tape"], dropna=False).agg(cross_rows=("cross_rows","sum"), files=("date","count"), tickers=("ticker","nunique")).reset_index().sort_values("cross_rows", ascending=False).head(30)
    _show(pair_view)
else:
    print("Sin datos de exchange pairs.")

fig, axes = plt.subplots(2, 2, figsize=(FIGSIZE[0] * 2, FIGSIZE[1] * 2))
tmp = window_df[(window_df["scope"] == "inside") & (window_df["window_days"] == 20)].copy()
if not tmp.empty:
    axes[0,0].bar(tmp["hypothesis"], tmp["hard_threshold_rate_pct"], color="#c44e52", alpha=0.85, label="hard_threshold")
    axes[0,0].bar(tmp["hypothesis"], tmp["hard_cap_rate_pct"], color="#dd8452", alpha=0.85, label="hard_cap")
    axes[0,0].legend()
    axes[0,0].set_title("Tasa de severidad dentro de ventana 20d")
rows_df = liq_price_summary["rows_bin"]
axes[0,1].bar(rows_df["rows_bin"].astype(str), rows_df["hard_threshold_rate_pct"], color="#4c72b0")
axes[0,1].set_title("Hard threshold rate por rows_bin")
axes[0,1].tick_params(axis="x", rotation=25)
price_df = liq_price_summary["price_bin"]
if not price_df.empty:
    axes[1,0].bar(price_df["price_bin"].astype(str), price_df["hard_threshold_rate_pct"], color="#55a868")
    axes[1,0].set_title("Hard threshold rate por price_bin")
vol_df = liq_price_summary["daily_volume_quartile"]
if not vol_df.empty:
    axes[1,1].bar(vol_df["daily_volume_quartile"].astype(str), vol_df["hard_threshold_rate_pct"], color="#8172b3")
    axes[1,1].set_title("Hard threshold rate por quartil de volumen diario")
plt.tight_layout(); plt.show()

if not severity_by_session.empty:
    sess_plot = severity_by_session.groupby("session", dropna=False).agg(rows_scanned=("rows_scanned","sum"), cross_rows=("cross_rows","sum")).reset_index()
    sess_plot["crossed_ratio_pct"] = 100 * sess_plot["cross_rows"] / sess_plot["rows_scanned"].clip(lower=1)
    plt.figure(figsize=FIGSIZE)
    plt.bar(sess_plot["session"].astype(str), sess_plot["crossed_ratio_pct"], color="#4c72b0")
    plt.title("H3: crossed_ratio_pct agregado por session (top files)")
    plt.ylabel("% crossed rows")
    plt.tight_layout(); plt.show()

if not cross_exchange_pairs_global.empty:
    pair_plot = cross_exchange_pairs_global.groupby(["exchange_pair","tape"], dropna=False).agg(cross_rows=("cross_rows","sum")).reset_index().sort_values("cross_rows", ascending=False).head(15)
    plt.figure(figsize=(FIGSIZE[0], max(4, FIGSIZE[1] + 2)))
    labels = pair_plot["exchange_pair"].astype(str) + " | tape=" + pair_plot["tape"].astype(str)
    plt.barh(labels, pair_plot["cross_rows"], color="#dd8452")
    plt.gca().invert_yaxis()
    plt.title("H4: exchange pairs globales con mas cross_rows (top files)")
    plt.xlabel("cross_rows")
    plt.tight_layout(); plt.show()

if not pathology_df.empty:
    path_view = pathology_df.copy()
    path_view["pathology_rate_pct"] = 100 * path_view["placeholder_rows"] / path_view["rows_scanned"].clip(lower=1)
    plt.figure(figsize=FIGSIZE)
    top_path = path_view.sort_values(["pathology_rate_pct","crossed_ratio_pct"], ascending=[False,False]).head(20)
    plt.barh(top_path["ticker"] + " " + top_path["date"].astype(str), top_path["pathology_rate_pct"], color="#c44e52")
    plt.gca().invert_yaxis(); plt.title("Top files: placeholders/quotes absurdos"); plt.xlabel("% filas con ask/bid 0 o absurdos"); plt.tight_layout(); plt.show()

def _get_quote_df(file_path):
    if file_path in _QUOTE_CACHE:
        return _QUOTE_CACHE[file_path].copy()
    df = _read_parquet(file_path, ["timestamp","participant_timestamp","bid_price","ask_price","bid_exchange","ask_exchange","bid_size","ask_size","tape","sequence_number"])
    if df.empty:
        _QUOTE_CACHE[file_path] = df
        return df.copy()
    df["bid_price"] = pd.to_numeric(df["bid_price"], errors="coerce")
    df["ask_price"] = pd.to_numeric(df["ask_price"], errors="coerce")
    df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="ns", utc=True, errors="coerce")
    df["ts_local"] = df["ts_utc"].dt.tz_convert("America/New_York")
    df["crossed"] = (df["bid_price"] > df["ask_price"]) & df["bid_price"].notna() & df["ask_price"].notna()
    df["ask_zero"] = df["ask_price"].fillna(np.nan) <= 0
    df["bid_zero"] = df["bid_price"].fillna(np.nan) <= 0
    df["ask_absurd"] = df["ask_price"].fillna(np.nan) >= 100000
    df["bid_absurd"] = df["bid_price"].fillna(np.nan) >= 100000
    df["session"] = _session(df["ts_utc"])
    _QUOTE_CACHE[file_path] = df
    return df.copy()

def _ticker_case_table(ticker):
    cols = ["ticker","date","severity","crossed_ratio_pct","crossed_rows","rows","retry_pending_flag","retry_frozen_flag","hard_threshold","hard_cap","nearest_split_days","nearest_ticker_change_days","daily_close","daily_volume","daily_trades","minute_active","file"]
    return events.loc[events["ticker"] == ticker, cols].sort_values(["hard_cap","hard_threshold","crossed_ratio_pct","rows"], ascending=[False,False,False,False])

def _plot_case(ticker, date_str, session_mode="all", compare_days=30):
    case_df = _ticker_case_table(ticker)
    if case_df.empty:
        print("Ticker sin casos en el run.")
        return
    row = case_df.loc[case_df["date"].astype(str) == date_str].head(1)
    row = case_df.head(1) if row.empty else row
    row = row.iloc[0]
    q = _get_quote_df(str(row["file"]))
    if q.empty:
        print("No se pudo leer quotes del caso.")
        return
    if session_mode != "all":
        q = q[q["session"] == session_mode].copy()
    if len(q) > QUOTE_VIEW_MAX_ROWS:
        q = q.iloc[:QUOTE_VIEW_MAX_ROWS].copy()
    daily_hist = daily_df[daily_df["ticker"] == ticker].copy() if not daily_df.empty else pd.DataFrame()
    if not daily_hist.empty:
        daily_hist["date"] = pd.to_datetime(daily_hist["date"], errors="coerce")
    d0 = pd.to_datetime(row["date"])
    if not daily_hist.empty:
        daily_hist = daily_hist[(daily_hist["date"] >= d0 - pd.Timedelta(days=compare_days)) & (daily_hist["date"] <= d0 + pd.Timedelta(days=compare_days))].copy()
    split_dates = splits_df.loc[splits_df["ticker"] == ticker, "execution_date"].dropna().tolist() if not splits_df.empty else []
    change_dates = ref_events_df.loc[(ref_events_df["ticker"] == ticker) & (ref_events_df["event_type"] == "ticker_change"), "event_date"].dropna().tolist() if not ref_events_df.empty else []
    fig = plt.figure(figsize=(16, 10)); gs = fig.add_gridspec(3, 2, height_ratios=[2.2,1.4,1.2]); ax1 = fig.add_subplot(gs[0,:]); ax2 = fig.add_subplot(gs[1,0]); ax3 = fig.add_subplot(gs[1,1]); ax4 = fig.add_subplot(gs[2,:])
    ax1.plot(q["ts_local"], q["bid_price"], label="bid_price", linewidth=0.9, color="#4c72b0")
    ax1.plot(q["ts_local"], q["ask_price"], label="ask_price", linewidth=0.9, color="#dd8452")
    crossed = q[q["crossed"]]
    if not crossed.empty:
        ax1.scatter(crossed["ts_local"], crossed["bid_price"], s=10, color="#c44e52", alpha=0.8, label="crossed rows")
    placeholder = q[q["ask_zero"] | q["bid_zero"] | q["ask_absurd"] | q["bid_absurd"]]
    if not placeholder.empty:
        ax1.scatter(placeholder["ts_local"], placeholder["ask_price"].clip(lower=0), s=14, color="#8172b3", alpha=0.8, label="placeholder/absurd")
    ax1.set_title(f"{ticker} {pd.to_datetime(row['date']).date()} | severity={row['severity']} | crossed_ratio_pct={row['crossed_ratio_pct']:.4f}% | rows={int(row['rows'])}")
    ax1.legend(loc="best"); ax1.set_ylabel("price")
    if not daily_hist.empty:
        ax2.plot(daily_hist["date"], daily_hist["c"], color="#55a868", marker="o", linewidth=1); ax2.axvline(d0, color="#c44e52", linestyle="--", linewidth=1.2, label="problem day")
        for sd in split_dates:
            if d0 - pd.Timedelta(days=compare_days) <= sd <= d0 + pd.Timedelta(days=compare_days):
                ax2.axvline(sd, color="#8172b3", linestyle=":", linewidth=1.0, label="split")
        for ed in change_dates:
            if d0 - pd.Timedelta(days=compare_days) <= ed <= d0 + pd.Timedelta(days=compare_days):
                ax2.axvline(ed, color="#937860", linestyle="-.", linewidth=1.0, label="ticker change")
        handles, labels = ax2.get_legend_handles_labels()
        if labels:
            dd = dict(zip(labels, handles)); ax2.legend(dd.values(), dd.keys(), loc="best")
        ax2.set_title("Close diario alrededor del caso"); ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d")); ax2.tick_params(axis="x", rotation=30)
        ax3.bar(daily_hist["date"], daily_hist["v"], color="#64b5cd"); ax3.axvline(d0, color="#c44e52", linestyle="--", linewidth=1.2); ax3.set_title("Volumen diario alrededor del caso"); ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d")); ax3.tick_params(axis="x", rotation=30)
    pair_df = crossed.assign(exchange_pair=crossed["bid_exchange"].astype("string") + "->" + crossed["ask_exchange"].astype("string")).groupby("exchange_pair", dropna=False).size().reset_index(name="cross_rows").sort_values("cross_rows", ascending=False).head(12)
    if not pair_df.empty:
        ax4.barh(pair_df["exchange_pair"], pair_df["cross_rows"], color="#c44e52"); ax4.invert_yaxis(); ax4.set_title("Pares bid_exchange -> ask_exchange que dominan los cruces")
    else:
        ax4.text(0.5, 0.5, "Sin filas crossed en el filtro actual", ha="center", va="center"); ax4.set_axis_off()
    plt.tight_layout(); plt.show()
    _show(pd.DataFrame([
        {"field": "ticker", "value": ticker}, {"field": "date", "value": str(pd.to_datetime(row["date"]).date())}, {"field": "severity", "value": row["severity"]},
        {"field": "crossed_ratio_pct", "value": row["crossed_ratio_pct"]}, {"field": "crossed_rows", "value": row["crossed_rows"]}, {"field": "rows", "value": row["rows"]},
        {"field": "retry_pending_flag", "value": row["retry_pending_flag"]}, {"field": "retry_frozen_flag", "value": row["retry_frozen_flag"]},
        {"field": "nearest_split_days", "value": row["nearest_split_days"]}, {"field": "nearest_ticker_change_days", "value": row["nearest_ticker_change_days"]},
        {"field": "daily_close", "value": row.get("daily_close")}, {"field": "daily_volume", "value": row.get("daily_volume")}, {"field": "daily_trades", "value": row.get("daily_trades")}, {"field": "minute_active", "value": row.get("minute_active")},
    ]), 50)

if widgets is not None:
    print("\nWidget causal interactivo:")
    all_widget_tickers = sorted([str(t) for t in events["ticker"].dropna().astype(str).unique().tolist()])
    ticker_dropdown = widgets.Dropdown(options=all_widget_tickers, description="Ticker:", layout=widgets.Layout(width="260px"))
    session_dropdown = widgets.Dropdown(options=[("Todo","all"),("Premarket","premarket"),("Market","market"),("Afterhours","afterhours")], value="all", description="Sesion:", layout=widgets.Layout(width="260px"))
    compare_slider = widgets.IntSlider(value=30, min=5, max=120, step=5, description="Ventana d:", layout=widgets.Layout(width="320px"))
    date_dropdown = widgets.Dropdown(description="Fecha:", layout=widgets.Layout(width="260px"))
    output = widgets.Output()
    print(f"Tickers disponibles en widget: {len(all_widget_tickers)}")
    def _update_dates(*args):
        vals = _ticker_case_table(ticker_dropdown.value)["date"].astype(str).tolist()
        date_dropdown.options = vals
        if vals:
            date_dropdown.value = vals[0]
    def _render(*args):
        with output:
            clear_output(wait=True)
            _plot_case(ticker_dropdown.value, date_dropdown.value, session_dropdown.value, int(compare_slider.value))
    ticker_dropdown.observe(_update_dates, names="value")
    ticker_dropdown.observe(_render, names="value")
    date_dropdown.observe(_render, names="value")
    session_dropdown.observe(_render, names="value")
    compare_slider.observe(_render, names="value")
    _update_dates()
    display(widgets.HBox([ticker_dropdown, date_dropdown, session_dropdown, compare_slider]))
    display(output)
    _render()
else:
    print("\nipywidgets no disponible; usa _plot_case('CSBR', '2015-09-02').")
