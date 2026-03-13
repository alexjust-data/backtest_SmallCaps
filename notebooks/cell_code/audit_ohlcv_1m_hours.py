from __future__ import annotations

import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_1m"))
CALENDAR_CSV = Path(
    globals().get(
        "CALENDAR_CSV",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\market_calendar_official_XNYS_20050101_20251231.csv",
    )
)
AUDIT_DIR = Path(
    globals().get(
        "AUDIT_DIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\debug\audit_ohlcv_1m_hours",
    )
)
TIMEZONE = str(globals().get("TIMEZONE", "America/New_York"))
PREMARKET_START = str(globals().get("PREMARKET_START", "04:00:00"))
MARKET_START = str(globals().get("MARKET_START", "09:30:00"))
MARKET_END_REGULAR = str(globals().get("MARKET_END_REGULAR", "16:00:00"))
POSTMARKET_END_REGULAR = str(globals().get("POSTMARKET_END_REGULAR", "20:00:00"))
POSTMARKET_END_EARLY = str(globals().get("POSTMARKET_END_EARLY", "17:00:00"))
MAX_TICKERS = globals().get("MAX_TICKERS", None)
UNIVERSE_PARQUET = Path(
    globals().get(
        "UNIVERSE_PARQUET",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet",
    )
)
WORKERS = int(globals().get("WORKERS", max(1, (os.cpu_count() or 1) - 1)))
PROGRESS_EVERY = int(globals().get("PROGRESS_EVERY", 250))

CACHE_PARQUET_NAME = "month_file_audit_cache.parquet"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_ts(day: str, hhmmss: str) -> pd.Timestamp:
    return pd.Timestamp(f"{day} {hhmmss}", tz=TIMEZONE)


def _load_calendar(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CALENDAR_CSV no existe: {path}")
    cal = pd.read_csv(path).copy()
    if "session_date" not in cal.columns:
        raise ValueError(f"CALENDAR_CSV no contiene session_date: {path}")
    cal["session_date"] = cal["session_date"].astype("string")
    if "is_early_close" not in cal.columns:
        cal["is_early_close"] = False
    cal["is_early_close"] = cal["is_early_close"].astype(str).str.lower().isin(["true", "1", "yes"])
    return cal[["session_date", "is_early_close"]].drop_duplicates(subset=["session_date"], keep="last")


def _load_universe_tickers(path: Path) -> list[str] | None:
    if not path.exists():
        return None
    tickers = pd.read_parquet(path, columns=["ticker"])["ticker"].astype("string").dropna().str.strip().str.upper()
    tickers = tickers[tickers.ne("")]
    return sorted(tickers.drop_duplicates().tolist())


def _iter_month_files(root: Path, universe_tickers: list[str] | None = None):
    discovered = 0
    ticker_dirs = []

    if universe_tickers:
        ticker_dirs = [(ticker, root / f"ticker={ticker}") for ticker in universe_tickers]
    else:
        ticker_dirs = [
            (p.name.split("=", 1)[1].strip().upper(), p)
            for p in root.glob("ticker=*")
            if p.is_dir() and "=" in p.name
        ]

    for ticker, ticker_dir in ticker_dirs:
        if not ticker_dir.exists():
            continue
        for year_dir in ticker_dir.glob("year=*"):
            if not year_dir.is_dir() or "=" not in year_dir.name:
                continue
            year = int(year_dir.name.split("=", 1)[1])
            for month_dir in year_dir.glob("month=*"):
                if not month_dir.is_dir() or "=" not in month_dir.name:
                    continue
                month = int(month_dir.name.split("=", 1)[1])
                for fp in month_dir.glob("minute_aggs_*.parquet"):
                    if not fp.is_file():
                        continue
                    stat = fp.stat()
                    discovered += 1
                    if discovered % max(1000, PROGRESS_EVERY * 5) == 0:
                        print(f"[discover] month_files_candidate={discovered}")
                    yield fp, ticker, year, month, int(stat.st_size), int(stat.st_mtime_ns)


def _collect_month_files(root: Path, max_tickers, universe_tickers: list[str] | None) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()
    for fp, ticker, year, month, file_size, file_mtime_ns in _iter_month_files(root, universe_tickers):
        if max_tickers is not None and ticker not in seen_tickers and len(seen_tickers) >= int(max_tickers):
            break
        seen_tickers.add(ticker)
        tasks.append(
            {
                "file": str(fp),
                "ticker": ticker,
                "year": year,
                "month": month,
                "file_size": file_size,
                "file_mtime_ns": file_mtime_ns,
            }
        )

    tasks.sort(key=lambda x: (x["ticker"], x["year"], x["month"], x["file"]))
    return tasks


def _audit_month_file(fp: Path, ticker: str, year: int, month: int, calendar_map: dict[str, bool]) -> list[dict]:
    df = pd.read_parquet(fp, columns=["t"]).copy()
    if df.empty:
        return [{
            "ticker": ticker,
            "year": year,
            "month": month,
            "session_date": None,
            "rows": 0,
            "first_ts_local": None,
            "last_ts_local": None,
            "first_hhmmss": None,
            "last_hhmmss": None,
            "is_early_close": None,
            "has_premarket": False,
            "has_rth": False,
            "has_postmarket": False,
            "has_out_of_policy": False,
            "premarket_rows": 0,
            "rth_rows": 0,
            "postmarket_rows": 0,
            "out_of_policy_rows": 0,
            "status": "empty_month_file",
            "file": str(fp),
        }]

    dt_utc = pd.to_datetime(pd.to_numeric(df["t"], errors="coerce"), unit="ms", utc=True, errors="coerce")
    dt_local = dt_utc.dt.tz_convert(TIMEZONE)
    valid = dt_local.notna()
    if not valid.any():
        return [{
            "ticker": ticker,
            "year": year,
            "month": month,
            "session_date": None,
            "rows": int(len(df)),
            "first_ts_local": None,
            "last_ts_local": None,
            "first_hhmmss": None,
            "last_hhmmss": None,
            "is_early_close": None,
            "has_premarket": False,
            "has_rth": False,
            "has_postmarket": False,
            "has_out_of_policy": False,
            "premarket_rows": 0,
            "rth_rows": 0,
            "postmarket_rows": 0,
            "out_of_policy_rows": 0,
            "status": "invalid_timestamps",
            "file": str(fp),
        }]

    work = pd.DataFrame({"dt_local": dt_local[valid]})
    work["session_date"] = work["dt_local"].dt.strftime("%Y-%m-%d")
    work["hhmmss"] = work["dt_local"].dt.strftime("%H:%M:%S")

    out: list[dict] = []
    for day, g in work.groupby("session_date", dropna=True, sort=False):
        is_early = bool(calendar_map.get(str(day), False))
        market_end = MARKET_END_REGULAR
        post_end = POSTMARKET_END_EARLY if is_early else POSTMARKET_END_REGULAR

        pre_start_ts = _session_ts(str(day), PREMARKET_START)
        market_start_ts = _session_ts(str(day), MARKET_START)
        market_end_ts = _session_ts(str(day), market_end)
        post_end_ts = _session_ts(str(day), post_end)

        is_pre = (g["dt_local"] >= pre_start_ts) & (g["dt_local"] < market_start_ts)
        is_rth = (g["dt_local"] >= market_start_ts) & (g["dt_local"] <= market_end_ts)
        is_post = (g["dt_local"] > market_end_ts) & (g["dt_local"] <= post_end_ts)
        is_out = (g["dt_local"] < pre_start_ts) | (g["dt_local"] > post_end_ts)

        statuses = []
        if is_out.any():
            statuses.append("out_of_policy_minutes")
        if not is_rth.any():
            statuses.append("missing_rth")
        if not statuses:
            statuses.append("ok")

        first_ts = g["dt_local"].min()
        last_ts = g["dt_local"].max()

        out.append(
            {
                "ticker": ticker,
                "year": year,
                "month": month,
                "session_date": str(day),
                "rows": int(len(g)),
                "first_ts_local": first_ts.isoformat(),
                "last_ts_local": last_ts.isoformat(),
                "first_hhmmss": g["hhmmss"].min(),
                "last_hhmmss": g["hhmmss"].max(),
                "is_early_close": is_early,
                "has_premarket": bool(is_pre.any()),
                "has_rth": bool(is_rth.any()),
                "has_postmarket": bool(is_post.any()),
                "has_out_of_policy": bool(is_out.any()),
                "premarket_rows": int(is_pre.sum()),
                "rth_rows": int(is_rth.sum()),
                "postmarket_rows": int(is_post.sum()),
                "out_of_policy_rows": int(is_out.sum()),
                "status": "|".join(statuses),
                "file": str(fp),
            }
        )
    return out


def _audit_month_task(task: dict[str, Any], calendar_map: dict[str, bool]) -> list[dict]:
    fp = Path(task["file"])
    out = _audit_month_file(fp, str(task["ticker"]), int(task["year"]), int(task["month"]), calendar_map)
    for row in out:
        row["file_size"] = int(task["file_size"])
        row["file_mtime_ns"] = int(task["file_mtime_ns"])
    return out


def _load_cache(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame()
    cache_df = pd.read_parquet(cache_path)
    required = {"file", "file_size", "file_mtime_ns"}
    if not required.issubset(cache_df.columns):
        return pd.DataFrame()
    cache_df["file"] = cache_df["file"].astype("string")
    cache_df["file_size"] = pd.to_numeric(cache_df["file_size"], errors="coerce").fillna(-1).astype("int64")
    cache_df["file_mtime_ns"] = pd.to_numeric(cache_df["file_mtime_ns"], errors="coerce").fillna(-1).astype("int64")
    return cache_df


def _split_reused_vs_pending(month_tasks: list[dict[str, Any]], cache_df: pd.DataFrame):
    if cache_df.empty:
        return pd.DataFrame(), month_tasks

    current_df = pd.DataFrame(month_tasks)
    current_df["file"] = current_df["file"].astype("string")
    current_df["file_size"] = pd.to_numeric(current_df["file_size"], errors="coerce").astype("int64")
    current_df["file_mtime_ns"] = pd.to_numeric(current_df["file_mtime_ns"], errors="coerce").astype("int64")

    unique_cache = (
        cache_df.sort_values(["file", "file_mtime_ns", "file_size"])
        .drop_duplicates(subset=["file", "file_mtime_ns", "file_size"], keep="last")
        [["file", "file_mtime_ns", "file_size"]]
        .assign(_cached=True)
    )

    merged = current_df.merge(unique_cache, on=["file", "file_mtime_ns", "file_size"], how="left")
    reused_keys = merged.loc[merged["_cached"].eq(True), ["file", "file_mtime_ns", "file_size"]].drop_duplicates()

    if reused_keys.empty:
        return pd.DataFrame(), month_tasks

    reused_df = cache_df.merge(reused_keys, on=["file", "file_mtime_ns", "file_size"], how="inner")
    reused_set = {
        (str(r.file), int(r.file_size), int(r.file_mtime_ns))
        for r in reused_keys.itertuples(index=False)
    }
    pending_tasks = [
        task for task in month_tasks
        if (task["file"], int(task["file_size"]), int(task["file_mtime_ns"])) not in reused_set
    ]
    return reused_df, pending_tasks


if not DATA_ROOT.exists():
    raise FileNotFoundError(f"DATA_ROOT no existe: {DATA_ROOT}")

AUDIT_DIR.mkdir(parents=True, exist_ok=True)

calendar_df = _load_calendar(CALENDAR_CSV)
calendar_map = dict(zip(calendar_df["session_date"].astype(str), calendar_df["is_early_close"].astype(bool)))
universe_tickers = _load_universe_tickers(UNIVERSE_PARQUET)
cache_path = AUDIT_DIR / CACHE_PARQUET_NAME

month_tasks = _collect_month_files(DATA_ROOT, MAX_TICKERS, universe_tickers)
cache_df = _load_cache(cache_path)
reused_df, pending_tasks = _split_reused_vs_pending(month_tasks, cache_df)
rows: list[dict] = []
tasks_total = len(month_tasks)
pending_total = len(pending_tasks)
reused_total = tasks_total - pending_total

print("audit_ohlcv_1m_hours starting")
print("DATA_ROOT:", DATA_ROOT)
print("CALENDAR_CSV:", CALENDAR_CSV)
print("AUDIT_DIR:", AUDIT_DIR)
print("UNIVERSE_PARQUET:", UNIVERSE_PARQUET)
print("UNIVERSE_TICKERS:", len(universe_tickers) if universe_tickers is not None else "scan_disk")
print(f"WORKERS: {WORKERS}")
print(f"MONTH_FILES_TO_AUDIT: {tasks_total}")
print(f"MONTH_FILES_REUSED_FROM_CACHE: {reused_total}")
print(f"MONTH_FILES_TO_RECOMPUTE: {pending_total}")

started_at = time.perf_counter()
completed = 0

if pending_total:
    if WORKERS <= 1:
        for task in pending_tasks:
            rows.extend(_audit_month_task(task, calendar_map))
            completed += 1
            if completed % PROGRESS_EVERY == 0 or completed == pending_total:
                elapsed = time.perf_counter() - started_at
                rate = completed / elapsed if elapsed > 0 else 0.0
                print(
                    f"[progress] month_files={completed}/{pending_total} "
                    f"elapsed_sec={elapsed:.1f} rate={rate:.2f}_files_per_sec"
                )
    else:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = [ex.submit(_audit_month_task, task, calendar_map) for task in pending_tasks]
            for fut in as_completed(futures):
                rows.extend(fut.result())
                completed += 1
                if completed % PROGRESS_EVERY == 0 or completed == pending_total:
                    elapsed = time.perf_counter() - started_at
                    rate = completed / elapsed if elapsed > 0 else 0.0
                    print(
                        f"[progress] month_files={completed}/{pending_total} "
                        f"elapsed_sec={elapsed:.1f} rate={rate:.2f}_files_per_sec"
                    )

recomputed_df = pd.DataFrame(rows)
if recomputed_df.empty:
    recomputed_df = pd.DataFrame(columns=["file", "file_size", "file_mtime_ns"])

if reused_df.empty and recomputed_df.empty:
    day_df = pd.DataFrame(
        columns=[
            "ticker", "year", "month", "session_date", "rows", "first_ts_local", "last_ts_local", "first_hhmmss",
            "last_hhmmss", "is_early_close", "has_premarket", "has_rth", "has_postmarket", "has_out_of_policy",
            "premarket_rows", "rth_rows", "postmarket_rows", "out_of_policy_rows", "status", "file", "file_size", "file_mtime_ns",
        ]
    )
else:
    day_df = pd.concat([reused_df, recomputed_df], ignore_index=True, sort=False)

ticker_df = (
    day_df.groupby("ticker", dropna=True)
    .agg(
        days_observed=("session_date", "nunique"),
        rows_total=("rows", "sum"),
        first_day=("session_date", "min"),
        last_day=("session_date", "max"),
        first_hhmmss_min=("first_hhmmss", "min"),
        last_hhmmss_max=("last_hhmmss", "max"),
        days_with_premarket=("has_premarket", "sum"),
        days_with_rth=("has_rth", "sum"),
        days_with_postmarket=("has_postmarket", "sum"),
        days_with_out_of_policy=("has_out_of_policy", "sum"),
        out_of_policy_rows_total=("out_of_policy_rows", "sum"),
    )
    .reset_index()
    if not day_df.empty
    else pd.DataFrame(
        columns=[
            "ticker", "days_observed", "rows_total", "first_day", "last_day", "first_hhmmss_min", "last_hhmmss_max",
            "days_with_premarket", "days_with_rth", "days_with_postmarket", "days_with_out_of_policy", "out_of_policy_rows_total",
        ]
    )
)

status_counts = (
    day_df["status"].value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    if not day_df.empty
    else pd.DataFrame(columns=["status", "count"])
)

summary = {
    "generated_at_utc": _now_utc(),
    "paths_used": {
        "data_root": str(DATA_ROOT),
        "calendar_csv": str(CALENDAR_CSV),
        "audit_dir": str(AUDIT_DIR),
    },
    "policy": {
        "timezone": TIMEZONE,
        "premarket_start": PREMARKET_START,
        "market_start": MARKET_START,
        "market_end_regular": MARKET_END_REGULAR,
        "postmarket_end_regular": POSTMARKET_END_REGULAR,
        "postmarket_end_early": POSTMARKET_END_EARLY,
    },
    "counts": {
        "tickers_observed": int(day_df["ticker"].nunique()) if not day_df.empty else 0,
        "days_observed": int(day_df["session_date"].nunique()) if not day_df.empty else 0,
        "ticker_day_rows": int(len(day_df)),
        "month_files_total": int(tasks_total),
        "month_files_reused_from_cache": int(reused_total),
        "month_files_recomputed": int(pending_total),
        "days_with_premarket": int(day_df["has_premarket"].sum()) if not day_df.empty else 0,
        "days_with_rth": int(day_df["has_rth"].sum()) if not day_df.empty else 0,
        "days_with_postmarket": int(day_df["has_postmarket"].sum()) if not day_df.empty else 0,
        "days_with_out_of_policy": int(day_df["has_out_of_policy"].sum()) if not day_df.empty else 0,
    },
}

summary_json = AUDIT_DIR / "summary.json"
day_csv = AUDIT_DIR / "ticker_day_hours.csv"
ticker_csv = AUDIT_DIR / "ticker_hours_summary.csv"
status_csv = AUDIT_DIR / "status_counts.csv"
day_parquet = AUDIT_DIR / "ticker_day_hours.parquet"
ticker_parquet = AUDIT_DIR / "ticker_hours_summary.parquet"

day_df.to_csv(day_csv, index=False, encoding="utf-8")
ticker_df.to_csv(ticker_csv, index=False, encoding="utf-8")
status_counts.to_csv(status_csv, index=False, encoding="utf-8")
day_df.to_parquet(day_parquet, index=False)
ticker_df.to_parquet(ticker_parquet, index=False)
day_df.to_parquet(cache_path, index=False)
summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

print("audit_ohlcv_1m_hours completed")
print("DATA_ROOT:", DATA_ROOT)
print("CALENDAR_CSV:", CALENDAR_CSV)
print("AUDIT_DIR:", AUDIT_DIR)
print("UNIVERSE_PARQUET:", UNIVERSE_PARQUET)
print(f"WORKERS: {WORKERS}")
print(f"MONTH_FILES_TO_AUDIT: {tasks_total}")
print(f"MONTH_FILES_REUSED_FROM_CACHE: {reused_total}")
print(f"MONTH_FILES_RECOMPUTED: {pending_total}")
print()
print("Counts:")
for k, v in summary["counts"].items():
    print(f"- {k}: {v}")
print()
print("Artifacts:")
print("-", summary_json)
print("-", day_csv)
print("-", ticker_csv)
print("-", status_csv)
print("-", day_parquet)
print("-", ticker_parquet)
