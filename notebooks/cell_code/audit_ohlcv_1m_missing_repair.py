from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_1m_missing_repair"))
EXPECTED_INPUT = Path(
    globals().get(
        "EXPECTED_INPUT",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_missing_in_ohlcv_1m_vs_daily.parquet",
    )
)
AUDIT_DIR = Path(globals().get("AUDIT_DIR", DATA_ROOT / "_audit_missing_repair"))
MAX_EXAMPLE_FILES = int(globals().get("MAX_EXAMPLE_FILES", 25))
WRITE_PARQUET_COPY = bool(globals().get("WRITE_PARQUET_COPY", True))
CHECKPOINT_DIR = Path(globals().get("CHECKPOINT_DIR", DATA_ROOT / "_run" / "checkpoints"))

REQUIRED_COLS = ["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"]
FINAL_PARTITION_RE = re.compile(
    r"[\\/]ticker=(?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]minute_aggs_(?P<ticker_file>[^_]+)_(?P<year_file>\d{4})_(?P<month_file>\d{2})\.parquet$",
    flags=re.IGNORECASE,
)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_ticker_series(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip().str.upper()
    out = out.dropna()
    return out[out != ""]


def _load_expected_input(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"EXPECTED_INPUT no existe: {path}")
    df = pd.read_parquet(path).copy()
    if "ticker" not in df.columns:
        raise ValueError(f"EXPECTED_INPUT no contiene columna 'ticker': {path}")
    df["ticker"] = _norm_ticker_series(df["ticker"])
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"], keep="first").sort_values("ticker")
    return df.reset_index(drop=True)


def _safe_json_load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _detect_scan_root(root: Path) -> tuple[Path, str]:
    staging = root / "_staging_flatfiles_1m"
    if staging.exists() and any(staging.glob("ticker=*")):
        return staging, "flatfiles_staging"
    if any(root.glob("ticker=*")):
        return root, "final_dataset"
    return root, "unknown"


def _path_parts_map(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in path.parts:
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.lower()] = v
    return out


def _validate_file(path: Path) -> dict:
    rec = {
        "file": str(path),
        "ticker_path": None,
        "year_path": None,
        "month_path": None,
        "rows": 0,
        "status": "ok",
        "issues": [],
        "warns": [],
        "min_date": None,
        "max_date": None,
    }

    parts = _path_parts_map(path)
    ticker_path = parts.get("ticker", "").strip().upper() or None
    year_raw = parts.get("year")
    month_raw = parts.get("month")

    if ticker_path is None or year_raw is None or month_raw is None:
        rec["status"] = "error"
        rec["issues"].append("invalid_partition_path")
        return rec

    year_path = int(year_raw)
    month_path = int(month_raw)
    rec["ticker_path"] = ticker_path
    rec["year_path"] = year_path
    rec["month_path"] = month_path

    final_m = FINAL_PARTITION_RE.search(str(path))
    if final_m:
        if ticker_path != final_m.group("ticker_file").strip().upper():
            rec["issues"].append("ticker_mismatch_filename_vs_partition")
        if year_path != int(final_m.group("year_file")):
            rec["issues"].append("year_mismatch_filename_vs_partition")
        if month_path != int(final_m.group("month_file")):
            rec["issues"].append("month_mismatch_filename_vs_partition")
    if path.stat().st_size == 0:
        rec["issues"].append("zero_byte_file")

    try:
        table = pq.read_table(path)
    except Exception as exc:
        rec["status"] = "error"
        rec["issues"].append(f"parquet_unreadable:{type(exc).__name__}")
        return rec

    df = table.to_pandas()
    rec["rows"] = int(len(df))

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        rec["issues"].append(f"missing_required_columns:{','.join(missing)}")
        rec["status"] = "error"
        return rec

    if df.empty:
        rec["issues"].append("zero_rows")
        rec["status"] = "error"
        return rec

    for col in ["ticker", "ts_utc", "date", "year", "month", "t"]:
        if df[col].isna().any():
            rec["issues"].append(f"null_in_{col}")

    ticker_vals = _norm_ticker_series(df["ticker"])
    if ticker_vals.empty or set(ticker_vals.unique()) != {ticker_path}:
        rec["issues"].append("ticker_value_mismatch_vs_path")

    year_vals = pd.to_numeric(df["year"], errors="coerce")
    month_vals = pd.to_numeric(df["month"], errors="coerce")
    if year_vals.isna().any() or not (year_vals.fillna(-1).astype(int) == year_path).all():
        rec["issues"].append("year_value_mismatch_vs_path")
    if month_vals.isna().any() or not (month_vals.fillna(-1).astype(int) == month_path).all():
        rec["issues"].append("month_value_mismatch_vs_path")

    t_num = pd.to_numeric(df["t"], errors="coerce")
    if t_num.isna().any():
        rec["issues"].append("non_numeric_t")
    else:
        if t_num.duplicated().any():
            rec["issues"].append("duplicate_t")
        if not t_num.is_monotonic_increasing:
            rec["warns"].append("timestamp_not_sorted")

        dt = pd.to_datetime(t_num, unit="ms", utc=True, errors="coerce")
        if dt.isna().any():
            rec["issues"].append("t_not_parseable_ms")
        else:
            dates_from_t = dt.dt.strftime("%Y-%m-%d")
            ts_utc_from_t = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if (dates_from_t != df["date"].astype(str)).any():
                rec["issues"].append("date_mismatch_vs_t")
            if (ts_utc_from_t != df["ts_utc"].astype(str)).any():
                rec["issues"].append("ts_utc_mismatch_vs_t")
            rec["min_date"] = str(dates_from_t.min())
            rec["max_date"] = str(dates_from_t.max())

    price_cols = ["o", "h", "l", "c", "vw"]
    vol_cols = ["v", "n"]
    for col in price_cols + vol_cols:
        vals = pd.to_numeric(df[col], errors="coerce")
        if vals.isna().any():
            rec["warns"].append(f"non_numeric_or_null_in_{col}")
        if (vals < 0).fillna(False).any():
            rec["issues"].append(f"negative_in_{col}")

    if not any(msg.startswith("missing_required_columns") for msg in rec["issues"]):
        o = pd.to_numeric(df["o"], errors="coerce")
        h = pd.to_numeric(df["h"], errors="coerce")
        l = pd.to_numeric(df["l"], errors="coerce")
        c = pd.to_numeric(df["c"], errors="coerce")
        bad_ohlc = ((h < pd.concat([o, c, l], axis=1).max(axis=1)) | (l > pd.concat([o, c, h], axis=1).min(axis=1))).fillna(False)
        bad_ohlc_n = int(bad_ohlc.sum())
        if bad_ohlc_n > 0:
            rec["issues"].append(f"ohlc_inconsistency_rows:{bad_ohlc_n}")

    if rec["issues"]:
        rec["status"] = "error"
    elif rec["warns"]:
        rec["status"] = "warn"
    return rec


def _checkpoint_status(ticker: str) -> dict:
    cp = CHECKPOINT_DIR / f"{ticker}.json"
    payload = _safe_json_load(cp)
    if payload is None:
        return {
            "ticker": ticker,
            "checkpoint_exists": False,
            "checkpoint_status": "missing",
            "checkpoint_rows": None,
            "checkpoint_files_written": None,
            "checkpoint_partial": None,
            "checkpoint_min_date": None,
            "checkpoint_max_date": None,
            "checkpoint_updated_at_utc": None,
        }
    return {
        "ticker": ticker,
        "checkpoint_exists": True,
        "checkpoint_status": str(payload.get("status", "unknown")),
        "checkpoint_rows": payload.get("rows"),
        "checkpoint_files_written": payload.get("files_written"),
        "checkpoint_partial": payload.get("partial"),
        "checkpoint_min_date": payload.get("min_date"),
        "checkpoint_max_date": payload.get("max_date"),
        "checkpoint_updated_at_utc": payload.get("updated_at_utc"),
    }


def _iter_parquet_files(root: Path, root_layout: str):
    for path in root.rglob("*.parquet"):
        if not path.is_file():
            continue
        if root_layout != "flatfiles_staging" and "_staging" in str(path).lower():
            continue
        yield path


if not DATA_ROOT.exists():
    raise FileNotFoundError(f"DATA_ROOT no existe: {DATA_ROOT}")

AUDIT_DIR.mkdir(parents=True, exist_ok=True)
SCAN_ROOT, ROOT_LAYOUT = _detect_scan_root(DATA_ROOT)

expected_df = _load_expected_input(EXPECTED_INPUT)
expected_tickers = expected_df["ticker"].tolist()
expected_set = set(expected_tickers)

file_records = [_validate_file(path) for path in _iter_parquet_files(SCAN_ROOT, ROOT_LAYOUT)]
files_df = pd.DataFrame(file_records)
if files_df.empty:
    files_df = pd.DataFrame(
        columns=["file", "ticker_path", "year_path", "month_path", "rows", "status", "issues", "warns", "min_date", "max_date"]
    )

for col in ["issues", "warns"]:
    if col in files_df.columns:
        files_df[col] = files_df[col].apply(lambda x: x if isinstance(x, list) else [])

present_tickers = set(files_df["ticker_path"].dropna().astype(str).str.upper().unique().tolist())
unexpected_tickers = sorted(present_tickers - expected_set)
missing_tickers = sorted(expected_set - present_tickers)

ticker_agg = (
    files_df.groupby("ticker_path", dropna=True)
    .agg(
        files_found=("file", "size"),
        rows_total=("rows", "sum"),
        file_errors=("status", lambda s: int((s == "error").sum())),
        file_warns=("status", lambda s: int((s == "warn").sum())),
        min_date_observed=("min_date", "min"),
        max_date_observed=("max_date", "max"),
    )
    .reset_index()
    .rename(columns={"ticker_path": "ticker"})
)
if ticker_agg.empty:
    ticker_agg = pd.DataFrame(
        columns=["ticker", "files_found", "rows_total", "file_errors", "file_warns", "min_date_observed", "max_date_observed"]
    )

cp_df = pd.DataFrame([_checkpoint_status(tk) for tk in expected_tickers])
ticker_status = expected_df[["ticker"]].copy()
ticker_status = ticker_status.merge(ticker_agg, on="ticker", how="left").merge(cp_df, on="ticker", how="left")

for col in ["files_found", "rows_total", "file_errors", "file_warns"]:
    if col in ticker_status.columns:
        ticker_status[col] = ticker_status[col].fillna(0).astype(int)

ticker_status["present_in_data_root"] = ticker_status["files_found"] > 0
ticker_status["has_file_errors"] = ticker_status["file_errors"] > 0
ticker_status["has_only_warns"] = (ticker_status["file_warns"] > 0) & (~ticker_status["has_file_errors"])
ticker_status["is_checkpoint_complete"] = ticker_status["checkpoint_exists"].fillna(False) & (ticker_status["checkpoint_partial"] != True)
ticker_status["audit_status"] = "missing"
ticker_status.loc[ticker_status["present_in_data_root"] & ~ticker_status["has_file_errors"], "audit_status"] = "present_ok"
ticker_status.loc[ticker_status["present_in_data_root"] & ticker_status["has_only_warns"] & ~ticker_status["has_file_errors"], "audit_status"] = "present_warn"
ticker_status.loc[ticker_status["present_in_data_root"] & ticker_status["has_file_errors"], "audit_status"] = "present_with_errors"

summary = {
    "generated_at_utc": _now_utc(),
    "paths_used": {
        "data_root": str(DATA_ROOT),
        "scan_root": str(SCAN_ROOT),
        "root_layout": ROOT_LAYOUT,
        "expected_input": str(EXPECTED_INPUT),
        "audit_dir": str(AUDIT_DIR),
        "checkpoint_dir": str(CHECKPOINT_DIR),
    },
    "counts": {
        "expected_tickers": int(len(expected_set)),
        "present_tickers": int(len(present_tickers)),
        "missing_tickers": int(len(missing_tickers)),
        "unexpected_tickers": int(len(unexpected_tickers)),
        "parquet_files_found": int(len(files_df)),
        "parquet_files_error": int((files_df["status"] == "error").sum()) if not files_df.empty else 0,
        "parquet_files_warn": int((files_df["status"] == "warn").sum()) if not files_df.empty else 0,
        "tickers_present_ok": int((ticker_status["audit_status"] == "present_ok").sum()),
        "tickers_present_warn": int((ticker_status["audit_status"] == "present_warn").sum()),
        "tickers_present_with_errors": int((ticker_status["audit_status"] == "present_with_errors").sum()),
        "tickers_with_checkpoint": int(ticker_status["checkpoint_exists"].fillna(False).sum()),
        "tickers_with_complete_checkpoint": int(ticker_status["is_checkpoint_complete"].fillna(False).sum()),
    },
    "samples": {
        "missing_tickers": missing_tickers[:50],
        "unexpected_tickers": unexpected_tickers[:50],
        "error_files": files_df.loc[files_df["status"] == "error", "file"].head(MAX_EXAMPLE_FILES).tolist() if not files_df.empty else [],
        "warn_files": files_df.loc[files_df["status"] == "warn", "file"].head(MAX_EXAMPLE_FILES).tolist() if not files_df.empty else [],
    },
}

summary_json = AUDIT_DIR / "summary.json"
files_csv = AUDIT_DIR / "file_audit.csv"
tickers_csv = AUDIT_DIR / "ticker_status.csv"
missing_csv = AUDIT_DIR / "missing_tickers.csv"
unexpected_csv = AUDIT_DIR / "unexpected_tickers.csv"

files_out = files_df.copy()
for col in ["issues", "warns"]:
    if col in files_out.columns:
        files_out[col] = files_out[col].apply(lambda x: json.dumps(x, ensure_ascii=False))

files_out.to_csv(files_csv, index=False, encoding="utf-8")
ticker_status.to_csv(tickers_csv, index=False, encoding="utf-8")
pd.DataFrame({"ticker": missing_tickers}).to_csv(missing_csv, index=False, encoding="utf-8")
pd.DataFrame({"ticker": unexpected_tickers}).to_csv(unexpected_csv, index=False, encoding="utf-8")
summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

if WRITE_PARQUET_COPY:
    files_parquet = AUDIT_DIR / "file_audit.parquet"
    tickers_parquet = AUDIT_DIR / "ticker_status.parquet"
    files_df.to_parquet(files_parquet, index=False)
    ticker_status.to_parquet(tickers_parquet, index=False)

print("audit_ohlcv_1m_missing_repair completed")
print("DATA_ROOT:", DATA_ROOT)
print("EXPECTED_INPUT:", EXPECTED_INPUT)
print("AUDIT_DIR:", AUDIT_DIR)
print()
print("Counts:")
for k, v in summary["counts"].items():
    print(f"- {k}: {v}")
print()
print("Artifacts:")
print("-", summary_json)
print("-", files_csv)
print("-", tickers_csv)
print("-", missing_csv)
print("-", unexpected_csv)
if WRITE_PARQUET_COPY:
    print("-", AUDIT_DIR / "file_audit.parquet")
    print("-", AUDIT_DIR / "ticker_status.parquet")
