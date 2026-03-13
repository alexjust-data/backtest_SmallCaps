from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

try:
    from IPython.display import display
except Exception:
    display = print

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
HALTS_PARQUET = Path(globals().get("HALTS_PARQUET", r"D:\Halts\processed\halts_master_nasdaq_for_run_dates.parquet"))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))
TOP_CASES = int(globals().get("TOP_CASES", 50))
WINDOWS = list(globals().get("HALT_WINDOWS", [0, 1, 5, 20]))
PRE_HALT_MINUTES = int(globals().get("PRE_HALT_MINUTES", 30))
REOPEN_MINUTES = int(globals().get("REOPEN_MINUTES", 30))
INTRADAY_TOP_CASES = int(globals().get("INTRADAY_TOP_CASES", 100))


def _show(df):
    with pd.option_context("display.max_rows", 200, "display.max_columns", 80, "display.width", 240):
        display(df)


def _parse_listlike(x):
    if pd.isna(x):
        return []
    s = str(x).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    try:
        import ast
        v = ast.literal_eval(s)
        if isinstance(v, list):
            return [str(i) for i in v]
        return [str(v)]
    except Exception:
        return [s]


def _safe_div(a, b):
    return float(a) / float(b) if b not in (0, 0.0, None) else np.nan


def _load_quote_df(file_path: str) -> pd.DataFrame:
    if pq is None:
        return pd.DataFrame()
    fp = Path(file_path)
    if not fp.exists():
        return pd.DataFrame()
    cols = ["timestamp", "bid_price", "ask_price", "bid_exchange", "ask_exchange", "tape", "sequence_number"]
    pf = pq.ParquetFile(fp)
    avail = set(pf.schema_arrow.names)
    use_cols = [c for c in cols if c in avail]
    if not use_cols:
        return pd.DataFrame()
    df = pf.read(columns=use_cols).to_pandas()
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["bid_price"] = pd.to_numeric(df.get("bid_price"), errors="coerce")
    df["ask_price"] = pd.to_numeric(df.get("ask_price"), errors="coerce")
    df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="ns", utc=True, errors="coerce")
    df = df[df["ts_utc"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
    df["ts_et"] = df["ts_utc"].dt.tz_convert("America/New_York")
    df["crossed"] = (df["bid_price"] > df["ask_price"]) & df["bid_price"].notna() & df["ask_price"].notna()
    df["placeholder"] = (df["ask_price"].fillna(np.nan) <= 0) | (df["bid_price"].fillna(np.nan) <= 0) | (df["ask_price"].fillna(np.nan) >= 100000) | (df["bid_price"].fillna(np.nan) >= 100000)
    return df


ev = pd.read_csv(EVENTS_CSV)
halts = pd.read_parquet(HALTS_PARQUET)

parts = ev["file"].astype(str).str.extract(
    r"[/\\](?P<ticker>[^/\\]+)[/\\]year=(?P<year>\d{4})[/\\]month=(?P<month>\d{2})[/\\]day=(?P<day>\d{2})[/\\]quotes\.parquet$"
)
ev["ticker"] = parts["ticker"].astype(str).str.upper().str.strip()
ev["date"] = pd.to_datetime(parts["year"] + "-" + parts["month"] + "-" + parts["day"], errors="coerce")
ev["crossed_ratio_pct"] = pd.to_numeric(ev.get("crossed_ratio_pct"), errors="coerce").fillna(0.0)
ev["rows"] = pd.to_numeric(ev.get("rows"), errors="coerce").fillna(0.0)
ev["crossed_rows"] = pd.to_numeric(ev.get("crossed_rows"), errors="coerce").fillna(0.0)
ev["severity"] = ev.get("severity", pd.Series(["UNKNOWN"] * len(ev))).astype(str)
ev["issues_list"] = ev.get("issues", pd.Series([None] * len(ev))).apply(_parse_listlike)
ev["warns_list"] = ev.get("warns", pd.Series([None] * len(ev))).apply(_parse_listlike)
ev["hard_threshold"] = ev["issues_list"].apply(lambda xs: "crossed_ratio_gt_threshold" in set(xs))
ev["hard_cap"] = ev["issues_list"].apply(lambda xs: "crossed_ratio_gt_hard_cap" in set(xs))
ev["soft_cross"] = ev["warns_list"].apply(lambda xs: "crossed_rows_present_but_under_threshold" in set(xs))
ev = ev[ev["ticker"].notna() & ev["date"].notna()].copy()

halts["ticker"] = halts["ticker"].astype(str).str.upper().str.strip()
halts["halt_date"] = pd.to_datetime(halts["halt_date"], errors="coerce")
halts["halt_start_et"] = pd.to_datetime(halts.get("halt_start_et"), errors="coerce")
halts["resume_trade_et"] = pd.to_datetime(halts.get("resume_trade_et"), errors="coerce")
halts["resume_quote_et"] = pd.to_datetime(halts.get("resume_quote_et"), errors="coerce")
for _c in ["halt_start_et", "resume_trade_et", "resume_quote_et"]:
    if _c in halts.columns:
        halts[_c] = pd.to_datetime(halts[_c], errors="coerce")
        if getattr(halts[_c].dt, "tz", None) is None:
            halts[_c] = halts[_c].dt.tz_localize("America/New_York", nonexistent="NaT", ambiguous="NaT")
        else:
            halts[_c] = halts[_c].dt.tz_convert("America/New_York")
halts = halts[halts["ticker"].notna() & halts["halt_date"].notna()].copy()

ev["nearest_halt_days"] = np.nan
ev["halt_count_same_day"] = 0
ev["halt_type_same_day"] = pd.NA
ev["halt_code_same_day"] = pd.NA

for ticker, grp in ev.groupby("ticker"):
    h = halts[halts["ticker"] == ticker].copy()
    if h.empty:
        continue
    halt_dates = h["halt_date"].dropna().sort_values().reset_index(drop=True)
    idx = ev["ticker"] == ticker
    dates = ev.loc[idx, "date"]

    def nearest_days(d):
        diffs = (halt_dates - d).dt.days.abs()
        return float(diffs.min()) if len(diffs) else np.nan

    ev.loc[idx, "nearest_halt_days"] = dates.apply(nearest_days)

    same_day = ev.loc[idx, ["date"]].merge(
        h[["halt_date", "halt_type", "halt_code"]],
        left_on="date",
        right_on="halt_date",
        how="left",
    )
    same_day_counts = same_day.groupby("date").size().rename("halt_count_same_day")
    same_day_type = same_day.groupby("date")["halt_type"].agg(lambda s: ",".join(sorted(set([str(x) for x in s.dropna()]))) if s.notna().any() else pd.NA)
    same_day_code = same_day.groupby("date")["halt_code"].agg(lambda s: ",".join(sorted(set([str(x) for x in s.dropna()]))) if s.notna().any() else pd.NA)

    ev.loc[idx, "halt_count_same_day"] = ev.loc[idx, "date"].map(same_day_counts).fillna(0).astype(int)
    ev.loc[idx, "halt_type_same_day"] = ev.loc[idx, "date"].map(same_day_type)
    ev.loc[idx, "halt_code_same_day"] = ev.loc[idx, "date"].map(same_day_code)

for w in WINDOWS:
    ev[f"within_{w}d_halt"] = ev["nearest_halt_days"].le(w).fillna(False)

rows = []
for w in WINDOWS:
    for label, mask in [("inside", ev[f"within_{w}d_halt"]), ("outside", ~ev[f"within_{w}d_halt"])]:
        d = ev[mask].copy()
        if d.empty:
            continue
        rows.append({
            "window_days": w,
            "scope": label,
            "files": len(d),
            "tickers": d["ticker"].nunique(),
            "hard_threshold_files": int(d["hard_threshold"].sum()),
            "hard_cap_files": int(d["hard_cap"].sum()),
            "soft_cross_files": int(d["soft_cross"].sum()),
            "hard_threshold_rate_pct": 100 * d["hard_threshold"].mean(),
            "hard_cap_rate_pct": 100 * d["hard_cap"].mean(),
            "soft_cross_rate_pct": 100 * d["soft_cross"].mean(),
            "median_crossed_ratio_pct": d["crossed_ratio_pct"].median(),
            "p95_crossed_ratio_pct": d["crossed_ratio_pct"].quantile(0.95),
            "mean_rows": d["rows"].mean(),
            "median_rows": d["rows"].median(),
        })
contrast = pd.DataFrame(rows)

same_day = ev[ev["within_0d_halt"]].copy()
same_day_by_type = (
    same_day.groupby("halt_type_same_day", dropna=False)
    .agg(
        files=("file", "size"),
        tickers=("ticker", "nunique"),
        hard_threshold_files=("hard_threshold", "sum"),
        hard_cap_files=("hard_cap", "sum"),
        soft_cross_files=("soft_cross", "sum"),
        median_crossed_ratio_pct=("crossed_ratio_pct", "median"),
        p95_crossed_ratio_pct=("crossed_ratio_pct", lambda s: s.quantile(0.95)),
    )
    .reset_index()
    .sort_values("files", ascending=False)
)

top_halt_cases = ev[ev["within_5d_halt"]].sort_values(
    ["hard_cap", "hard_threshold", "crossed_ratio_pct", "rows"],
    ascending=[False, False, False, False],
).head(TOP_CASES)[[
    "ticker", "date", "severity", "crossed_ratio_pct", "crossed_rows", "rows",
    "nearest_halt_days", "halt_count_same_day", "halt_type_same_day", "halt_code_same_day", "file"
]]

intraday_candidates = (
    ev.merge(
        halts[["ticker", "halt_date", "halt_start_et", "resume_trade_et", "resume_quote_et", "halt_type", "halt_code"]],
        left_on=["ticker", "date"],
        right_on=["ticker", "halt_date"],
        how="inner",
    )
    .copy()
)
intraday_candidates["resume_effective_et"] = intraday_candidates["resume_trade_et"].fillna(intraday_candidates["resume_quote_et"])
intraday_candidates = intraday_candidates[
    intraday_candidates["halt_start_et"].notna() & intraday_candidates["resume_effective_et"].notna()
].copy()
intraday_candidates = intraday_candidates.sort_values(
    ["hard_cap", "hard_threshold", "crossed_ratio_pct", "rows"],
    ascending=[False, False, False, False],
).head(INTRADAY_TOP_CASES)

intraday_rows = []
for _, r in intraday_candidates.iterrows():
    q = _load_quote_df(str(r["file"]))
    if q.empty:
        continue
    halt_start = pd.Timestamp(r["halt_start_et"])
    resume_et = pd.Timestamp(r["resume_effective_et"])
    if halt_start.tzinfo is None:
        halt_start = halt_start.tz_localize("America/New_York")
    else:
        halt_start = halt_start.tz_convert("America/New_York")
    if resume_et.tzinfo is None:
        resume_et = resume_et.tz_localize("America/New_York")
    else:
        resume_et = resume_et.tz_convert("America/New_York")
    pre_start = halt_start - pd.Timedelta(minutes=PRE_HALT_MINUTES)
    reopen_end = resume_et + pd.Timedelta(minutes=REOPEN_MINUTES)

    window_masks = {
        "pre_halt_30m": (q["ts_et"] >= pre_start) & (q["ts_et"] < halt_start),
        "halt_window": (q["ts_et"] >= halt_start) & (q["ts_et"] < resume_et),
        "reopen_30m": (q["ts_et"] >= resume_et) & (q["ts_et"] < reopen_end),
        "same_day_all": pd.Series(True, index=q.index),
    }
    for window_name, mask in window_masks.items():
        s = q[mask].copy()
        rows_n = int(len(s))
        cross_rows = int(s["crossed"].sum()) if rows_n else 0
        placeholder_rows = int(s["placeholder"].sum()) if rows_n else 0
        intraday_rows.append({
            "ticker": r["ticker"],
            "date": r["date"],
            "window_name": window_name,
            "halt_type": r["halt_type"],
            "halt_code": r["halt_code"],
            "rows": rows_n,
            "cross_rows": cross_rows,
            "placeholder_rows": placeholder_rows,
            "crossed_ratio_pct": 100 * _safe_div(cross_rows, rows_n) if rows_n else np.nan,
            "placeholder_ratio_pct": 100 * _safe_div(placeholder_rows, rows_n) if rows_n else np.nan,
            "halt_minutes": (resume_et - halt_start).total_seconds() / 60.0,
            "file_crossed_ratio_pct": r["crossed_ratio_pct"],
            "severity": r["severity"],
            "file": r["file"],
        })

intraday_df = pd.DataFrame(intraday_rows)
intraday_summary = pd.DataFrame()
same_day_vs_nonhalt = pd.DataFrame()
if not intraday_df.empty:
    intraday_summary = (
        intraday_df.groupby("window_name", dropna=False)
        .agg(
            files=("file", "nunique"),
            tickers=("ticker", "nunique"),
            rows=("rows", "sum"),
            cross_rows=("cross_rows", "sum"),
            placeholder_rows=("placeholder_rows", "sum"),
            median_crossed_ratio_pct=("crossed_ratio_pct", "median"),
            p95_crossed_ratio_pct=("crossed_ratio_pct", lambda s: s.quantile(0.95)),
            median_placeholder_ratio_pct=("placeholder_ratio_pct", "median"),
        )
        .reset_index()
    )
    intraday_summary["weighted_crossed_ratio_pct"] = 100 * intraday_summary["cross_rows"] / intraday_summary["rows"].clip(lower=1)
    intraday_summary["weighted_placeholder_ratio_pct"] = 100 * intraday_summary["placeholder_rows"] / intraday_summary["rows"].clip(lower=1)

    halt_days = set(zip(intraday_candidates["ticker"].astype(str), pd.to_datetime(intraday_candidates["date"]).dt.strftime("%Y-%m-%d")))
    nonhalt_same_day = ev.copy()
    nonhalt_same_day["date_key"] = nonhalt_same_day["date"].dt.strftime("%Y-%m-%d")
    nonhalt_same_day = nonhalt_same_day[~nonhalt_same_day[["ticker", "date_key"]].apply(tuple, axis=1).isin(halt_days)].copy()
    same_day_vs_nonhalt = pd.DataFrame([
        {
            "scope": "same_day_halt_files",
            "files": int(len(intraday_candidates)),
            "hard_threshold_rate_pct": 100 * intraday_candidates["hard_threshold"].mean() if len(intraday_candidates) else np.nan,
            "hard_cap_rate_pct": 100 * intraday_candidates["hard_cap"].mean() if len(intraday_candidates) else np.nan,
            "soft_cross_rate_pct": 100 * intraday_candidates["soft_cross"].mean() if len(intraday_candidates) else np.nan,
            "median_crossed_ratio_pct": intraday_candidates["crossed_ratio_pct"].median() if len(intraday_candidates) else np.nan,
        },
        {
            "scope": "non_halt_day_files",
            "files": int(len(nonhalt_same_day)),
            "hard_threshold_rate_pct": 100 * nonhalt_same_day["hard_threshold"].mean() if len(nonhalt_same_day) else np.nan,
            "hard_cap_rate_pct": 100 * nonhalt_same_day["hard_cap"].mean() if len(nonhalt_same_day) else np.nan,
            "soft_cross_rate_pct": 100 * nonhalt_same_day["soft_cross"].mean() if len(nonhalt_same_day) else np.nan,
            "median_crossed_ratio_pct": nonhalt_same_day["crossed_ratio_pct"].median() if len(nonhalt_same_day) else np.nan,
        },
    ])

print("=== CONTRASTE HALT VS NO HALT ===")
_show(contrast)
print("\n=== MISMO DIA DE HALT: DESGLOSE POR HALT_TYPE ===")
_show(same_day_by_type)
print(f"\n=== TOP {TOP_CASES} CASOS CERCA DE HALTS (<=5d) ===")
_show(top_halt_cases)
print("\n=== INTRADIA: PRE-HALT / HALT / REOPEN ===")
if intraday_summary.empty:
    print("Sin casos con halt_start_et y resume_trade_et/resume_quote_et utilizables.")
else:
    _show(intraday_summary)
    print("\n=== SAME-DAY HALT VS NON-HALT DAY FILES ===")
    _show(same_day_vs_nonhalt)

fig, axes = plt.subplots(1, 3, figsize=(16, 4))
plot_df = contrast[contrast["scope"] == "inside"].copy()
axes[0].bar(plot_df["window_days"].astype(str), plot_df["hard_threshold_rate_pct"], color="#c44e52")
axes[0].set_title("Hard threshold rate dentro de ventana halt")
axes[0].set_xlabel("window_days")
axes[0].set_ylabel("% files")
axes[1].bar(plot_df["window_days"].astype(str), plot_df["hard_cap_rate_pct"], color="#dd8452")
axes[1].set_title("Hard cap rate dentro de ventana halt")
axes[1].set_xlabel("window_days")
axes[1].set_ylabel("% files")
axes[2].bar(plot_df["window_days"].astype(str), plot_df["soft_cross_rate_pct"], color="#4c72b0")
axes[2].set_title("Soft cross rate dentro de ventana halt")
axes[2].set_xlabel("window_days")
axes[2].set_ylabel("% files")
plt.tight_layout()
plt.show()

cmp5 = contrast[contrast["window_days"] == 5].copy()
if not cmp5.empty:
    plt.figure(figsize=(8, 4))
    x = np.arange(len(cmp5))
    width = 0.35
    plt.bar(x - width/2, cmp5["hard_threshold_rate_pct"], width=width, label="hard_threshold")
    plt.bar(x + width/2, cmp5["hard_cap_rate_pct"], width=width, label="hard_cap")
    plt.xticks(x, cmp5["scope"])
    plt.title("Ventana 5d: inside vs outside halt")
    plt.ylabel("% files")
    plt.legend()
    plt.tight_layout()
    plt.show()

if not intraday_summary.empty:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    order = [w for w in ["pre_halt_30m", "halt_window", "reopen_30m", "same_day_all"] if w in intraday_summary["window_name"].tolist()]
    p = intraday_summary.set_index("window_name").loc[order].reset_index()
    axes[0].bar(p["window_name"], p["weighted_crossed_ratio_pct"], color="#c44e52")
    axes[0].set_title("Intradia: weighted crossed ratio")
    axes[0].set_ylabel("% rows")
    axes[0].tick_params(axis="x", rotation=20)
    axes[1].bar(p["window_name"], p["weighted_placeholder_ratio_pct"], color="#8172b3")
    axes[1].set_title("Intradia: weighted placeholder ratio")
    axes[1].set_ylabel("% rows")
    axes[1].tick_params(axis="x", rotation=20)
    plt.tight_layout()
    plt.show()
