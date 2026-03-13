from pathlib import Path
import re
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pandas.errors import ParserError

# =========================
# Config (sobrescribible desde notebook)
# =========================
DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_1m"))
RUN_ID = str(globals().get("RUN_ID", datetime.now().strftime("%Y%m%d_%H%M%S_ohlcv_1m")))
RUN_BASE = Path(globals().get("RUN_BASE", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\ohlcv_1m_audit"))
RUN_DIR = Path(globals().get("RUN_DIR", RUN_BASE / RUN_ID))
RUN_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILES = int(globals().get("MAX_FILES", 5000))
RESET_STATE = bool(globals().get("RESET_STATE", False))
RESCAN_ALL = bool(globals().get("RESCAN_ALL", False))
RETRY_POLICY = str(globals().get("RETRY_POLICY", "hard_only"))  # hard_only | hard_and_soft
MAX_RETRY_ATTEMPTS = int(globals().get("MAX_RETRY_ATTEMPTS", 3))
DISCOVERY_MODE = str(globals().get("DISCOVERY_MODE", "fast")).lower()  # fast | full
INCLUDE_STAGING = bool(globals().get("INCLUDE_STAGING", False))

MAX_TS_DATE_MISMATCH_PCT = float(globals().get("MAX_TS_DATE_MISMATCH_PCT", 0.0))
MAX_TSUTC_T_MISMATCH_PCT = float(globals().get("MAX_TSUTC_T_MISMATCH_PCT", 0.0))
MAX_ZERO_VOLUME_RATIO_WARN = float(globals().get("MAX_ZERO_VOLUME_RATIO_WARN", 95.0))
CLOSEABLE_SOFT_FAIL_CAUSES = set(globals().get("CLOSEABLE_SOFT_FAIL_CAUSES", []))

STATE_FILE = Path(globals().get("STATE_FILE", RUN_DIR / "ohlcv_1m_agent_state.json"))
EVENTS_HISTORY_CSV = Path(globals().get("EVENTS_HISTORY_CSV", RUN_DIR / "ohlcv_1m_events_history.csv"))
EVENTS_CURRENT_CSV = Path(globals().get("EVENTS_CURRENT_CSV", RUN_DIR / "ohlcv_1m_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", RUN_DIR / "retry_queue_ohlcv_1m.csv"))
RETRY_CURRENT_CSV = Path(globals().get("RETRY_CURRENT_CSV", RUN_DIR / "retry_queue_ohlcv_1m_current.csv"))
RETRY_ATTEMPTS_CSV = Path(globals().get("RETRY_ATTEMPTS_CSV", RUN_DIR / "retry_attempts_ohlcv_1m.csv"))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", RUN_DIR / "retry_frozen_ohlcv_1m.csv"))
BATCH_MANIFEST_CSV = Path(globals().get("BATCH_MANIFEST_CSV", RUN_DIR / "batch_manifest_ohlcv_1m.csv"))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", RUN_DIR / "run_config_ohlcv_1m.json"))
LIVE_STATUS_JSON = Path(globals().get("LIVE_STATUS_JSON", RUN_DIR / "live_status_ohlcv_1m.json"))

REQUIRED_COLS = ["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"]
PARTITION_RE = re.compile(
    r"[\\/]ticker=(?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]minute_aggs_(?P<ticker_file>[^_]+)_(?P<year_file>\d{4})_(?P<month_file>\d{2})\.parquet$",
    flags=re.IGNORECASE,
)

EVENT_COLUMNS = [
    "file",
    "severity",
    "issues",
    "warns",
    "action",
    "rows",
    "ticker_path",
    "year_path",
    "month_path",
    "processed_at_utc",
    "run_id",
]


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except ParserError:
        try:
            return pd.read_csv(path, on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _append_events_csv(path: Path, out_df: pd.DataFrame) -> None:
    if out_df.empty:
        return
    write_header = not path.exists()
    out_df.to_csv(path, mode="a", header=write_header, index=False, encoding="utf-8")


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"processed": {}, "run_id_last": None, "updated_at_utc": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"processed": {}, "run_id_last": None, "updated_at_utc": None}


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_sig(p: Path) -> str:
    st = p.stat()
    return f"{st.st_size}_{st.st_mtime_ns}"


def _safe_parse_listlike(v):
    if isinstance(v, list):
        return [str(x) for x in v]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s.replace("'", '"'))
            if isinstance(arr, list):
                return [str(x) for x in arr]
        except Exception:
            pass
    return [s]


def _decide_action(severity: str) -> str:
    if severity == "HARD_FAIL":
        return "RETRY"
    if severity == "SOFT_FAIL":
        return "RETRY" if RETRY_POLICY == "hard_and_soft" else "REVIEW"
    return "CLOSE"


def _is_soft_closeable(row: dict) -> bool:
    if str(row.get("severity", "")) != "SOFT_FAIL":
        return False
    warns = set(_safe_parse_listlike(row.get("warns", [])))
    if not warns:
        return False
    return warns.issubset(CLOSEABLE_SOFT_FAIL_CAUSES)


def _iter_parquet_files(root: Path):
    for p in root.rglob("*.parquet"):
        if not p.is_file():
            continue
        if not INCLUDE_STAGING:
            parts_low = [x.lower() for x in p.parts]
            if any(x.startswith("_staging") for x in parts_low):
                continue
        yield p


def _assess_file(fp: Path) -> dict:
    parsed = PARTITION_RE.search(str(fp))
    ticker_path = None
    year_path = None
    month_path = None
    issues = []
    warns = []

    if parsed:
        ticker_path = parsed.group("ticker")
        year_path = int(parsed.group("year"))
        month_path = int(parsed.group("month"))
        if parsed.group("ticker") != parsed.group("ticker_file"):
            issues.append("ticker_mismatch_in_filename_vs_partition")
        if int(parsed.group("year")) != int(parsed.group("year_file")):
            issues.append("year_mismatch_in_filename_vs_partition")
        if int(parsed.group("month")) != int(parsed.group("month_file")):
            issues.append("month_mismatch_in_filename_vs_partition")
    else:
        issues.append("invalid_partition_path")

    rows = None
    if fp.stat().st_size == 0:
        issues.append("zero_byte_file")

    if not issues:
        try:
            tbl = pq.ParquetFile(fp).read()
            rows = int(tbl.num_rows)
            df = tbl.to_pandas()
        except Exception:
            issues.append("parquet_unreadable")
            df = pd.DataFrame()

        if "df" in locals() and not df.empty:
            miss = [c for c in REQUIRED_COLS if c not in df.columns]
            if miss:
                issues.append("missing_required_columns")
            else:
                if len(df) == 0:
                    issues.append("zero_rows")
                else:
                    if df["ticker"].isna().any() or df["date"].isna().any() or df["ts_utc"].isna().any() or df["t"].isna().any():
                        issues.append("null_key_columns")

                    if df["t"].duplicated().any() or df["ts_utc"].duplicated().any():
                        issues.append("duplicate_minute_rows")

                    if ((df["o"] < 0) | (df["h"] < 0) | (df["l"] < 0) | (df["c"] < 0) | (df["vw"] < 0)).any():
                        issues.append("negative_prices")
                    if ((df["v"] < 0) | (df["n"] < 0)).any():
                        issues.append("negative_volume_or_trades")

                    bad_ohlc = ((df["h"] < df[["o", "c", "l"]].max(axis=1)) | (df["l"] > df[["o", "c", "h"]].min(axis=1))).sum()
                    if int(bad_ohlc) > 0:
                        issues.append(f"ohlc_inconsistency_rows:{int(bad_ohlc)}")

                    if ticker_path is not None and not (df["ticker"].astype(str) == str(ticker_path)).all():
                        issues.append("ticker_value_mismatch_vs_path")
                    if year_path is not None and not (pd.to_numeric(df["year"], errors="coerce").fillna(-1).astype(int) == int(year_path)).all():
                        issues.append("year_value_mismatch_vs_path")
                    if month_path is not None and not (pd.to_numeric(df["month"], errors="coerce").fillna(-1).astype(int) == int(month_path)).all():
                        issues.append("month_value_mismatch_vs_path")

                    try:
                        dt = pd.to_datetime(df["t"], unit="ms", utc=True)
                        ny_dt = dt.dt.tz_convert("America/New_York")
                        ny_date = ny_dt.dt.strftime("%Y-%m-%d")
                        ny_ts_utc_fmt = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                        mismatch_date_n = int((ny_date != df["date"].astype(str)).sum())
                        mismatch_date_pct = (mismatch_date_n / len(df) * 100.0) if len(df) else 0.0
                        if mismatch_date_pct > MAX_TS_DATE_MISMATCH_PCT:
                            warns.append(f"t_to_date_mismatch_gt_threshold:{mismatch_date_pct:.4f}%")
                        elif mismatch_date_n > 0:
                            warns.append(f"t_to_date_mismatch_rows:{mismatch_date_n}")

                        mismatch_ts_n = int((ny_ts_utc_fmt != df["ts_utc"].astype(str)).sum())
                        mismatch_ts_pct = (mismatch_ts_n / len(df) * 100.0) if len(df) else 0.0
                        if mismatch_ts_pct > MAX_TSUTC_T_MISMATCH_PCT:
                            warns.append(f"ts_utc_t_mismatch_gt_threshold:{mismatch_ts_pct:.4f}%")
                        elif mismatch_ts_n > 0:
                            warns.append(f"ts_utc_t_mismatch_rows:{mismatch_ts_n}")

                        if not pd.to_numeric(df["t"], errors="coerce").is_monotonic_increasing:
                            warns.append("timestamp_not_sorted")
                    except Exception:
                        warns.append("timestamp_parse_issue")

                    zero_vol_ratio = float((df["v"] == 0).mean() * 100.0) if len(df) else 0.0
                    if zero_vol_ratio > MAX_ZERO_VOLUME_RATIO_WARN:
                        warns.append(f"high_zero_volume_ratio:{zero_vol_ratio:.2f}%")
        else:
            if not issues:
                issues.append("zero_rows")

    if issues:
        severity = "HARD_FAIL"
    elif warns:
        severity = "SOFT_FAIL"
    else:
        severity = "PASS"

    return {
        "file": str(fp),
        "severity": severity,
        "issues": issues,
        "warns": warns,
        "action": _decide_action(severity),
        "rows": rows,
        "ticker_path": ticker_path,
        "year_path": year_path,
        "month_path": month_path,
        "processed_at_utc": _now_utc(),
        "run_id": RUN_ID,
    }


if not DATA_ROOT.exists():
    raise FileNotFoundError(f"DATA_ROOT no existe: {DATA_ROOT}")

if RESET_STATE and STATE_FILE.exists():
    STATE_FILE.unlink(missing_ok=True)

state = _load_state(STATE_FILE)
if RESET_STATE:
    state["processed"] = {}

all_files_count = 0
candidates = []
discovery_complete = False

if DISCOVERY_MODE == "full":
    all_files = [p for p in _iter_parquet_files(DATA_ROOT)]
    all_files = sorted(all_files, key=lambda p: p.stat().st_mtime, reverse=True)
    all_files_count = len(all_files)
    discovery_complete = True
    for p in all_files:
        sp = str(p)
        if RESCAN_ALL:
            candidates.append(p)
        else:
            sig = _file_sig(p)
            if state.get("processed", {}).get(sp) != sig:
                candidates.append(p)
    batch = candidates[:MAX_FILES]
else:
    batch = []
    for p in _iter_parquet_files(DATA_ROOT):
        all_files_count += 1
        sp = str(p)
        if RESCAN_ALL:
            batch.append(p)
        else:
            sig = _file_sig(p)
            if state.get("processed", {}).get(sp) != sig:
                batch.append(p)
        if len(batch) >= MAX_FILES:
            break
    candidates = batch.copy()
    discovery_complete = len(batch) < MAX_FILES

print(f"RUN_ID: {RUN_ID}")
print(f"DATA_ROOT: {DATA_ROOT}")
print(f"RUN_DIR: {RUN_DIR}")
print(f"discovery_mode: {DISCOVERY_MODE}")
print(f"all_files_scanned: {all_files_count} | candidates: {len(candidates)} | batch: {len(batch)} | discovery_complete: {discovery_complete}")
print(f"RETRY_POLICY: {RETRY_POLICY} | MAX_RETRY_ATTEMPTS: {MAX_RETRY_ATTEMPTS}")
print(f"INCLUDE_STAGING: {INCLUDE_STAGING}")

run_cfg = {
    "run_id": RUN_ID,
    "started_at_utc": _now_utc(),
    "data_root": str(DATA_ROOT),
    "run_dir": str(RUN_DIR),
    "max_files": int(MAX_FILES),
    "rescan_all": bool(RESCAN_ALL),
    "discovery_mode": DISCOVERY_MODE,
    "include_staging": bool(INCLUDE_STAGING),
    "retry_policy": RETRY_POLICY,
    "max_retry_attempts": int(MAX_RETRY_ATTEMPTS),
    "max_ts_date_mismatch_pct": float(MAX_TS_DATE_MISMATCH_PCT),
    "max_tsutc_t_mismatch_pct": float(MAX_TSUTC_T_MISMATCH_PCT),
    "max_zero_volume_ratio_warn": float(MAX_ZERO_VOLUME_RATIO_WARN),
    "closeable_soft_fail_causes": sorted(CLOSEABLE_SOFT_FAIL_CAUSES),
    "paths": {
        "state_file": str(STATE_FILE),
        "events_history_csv": str(EVENTS_HISTORY_CSV),
        "events_current_csv": str(EVENTS_CURRENT_CSV),
        "retry_queue_csv": str(RETRY_QUEUE_CSV),
        "retry_current_csv": str(RETRY_CURRENT_CSV),
        "retry_attempts_csv": str(RETRY_ATTEMPTS_CSV),
        "retry_frozen_csv": str(RETRY_FROZEN_CSV),
        "batch_manifest_csv": str(BATCH_MANIFEST_CSV),
        "live_status_json": str(LIVE_STATUS_JSON),
    },
}
RUN_CONFIG_JSON.write_text(json.dumps(run_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

out_rows = []
for fp in batch:
    rec = _assess_file(fp)
    out_rows.append(rec)
    state.setdefault("processed", {})[str(fp)] = _file_sig(fp)

state["run_id_last"] = RUN_ID
state["updated_at_utc"] = _now_utc()
_save_state(STATE_FILE, state)

out_df = pd.DataFrame(out_rows, columns=EVENT_COLUMNS) if out_rows else pd.DataFrame(columns=EVENT_COLUMNS)
if not out_df.empty:
    out_df["issues"] = out_df["issues"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    out_df["warns"] = out_df["warns"].apply(lambda x: json.dumps(x, ensure_ascii=False))

_append_events_csv(EVENTS_HISTORY_CSV, out_df)

hist = _read_csv_safe(EVENTS_HISTORY_CSV)
if hist.empty:
    current = pd.DataFrame(columns=EVENT_COLUMNS)
else:
    if "processed_at_utc" in hist.columns:
        hist = hist.sort_values("processed_at_utc")
    current = hist.drop_duplicates(subset=["file"], keep="last").copy()

current.to_csv(EVENTS_CURRENT_CSV, index=False, encoding="utf-8")

batch_manifest = out_df[["file"]].copy().drop_duplicates() if not out_df.empty else pd.DataFrame(columns=["file"])
batch_manifest.to_csv(BATCH_MANIFEST_CSV, index=False, encoding="utf-8")

rq = current.copy()
if not rq.empty:
    for c in ["issues", "warns"]:
        if c in rq.columns:
            rq[c] = rq[c].apply(_safe_parse_listlike)
    if RETRY_POLICY == "hard_and_soft":
        rq = rq[rq["severity"].isin(["HARD_FAIL", "SOFT_FAIL"])].copy()
    else:
        rq = rq[rq["severity"] == "HARD_FAIL"].copy()

    if not rq.empty:
        soft_close_mask = rq.apply(lambda r: _is_soft_closeable(r.to_dict()), axis=1)
        if len(soft_close_mask) == len(rq):
            rq = rq[~soft_close_mask].copy()

    for c in ["issues", "warns"]:
        if c not in rq.columns:
            rq[c] = []
    if not rq.empty:
        rq["issues"] = rq["issues"].apply(lambda x: json.dumps(x, ensure_ascii=False))
        rq["warns"] = rq["warns"].apply(lambda x: json.dumps(x, ensure_ascii=False))

rq.to_csv(RETRY_QUEUE_CSV, index=False, encoding="utf-8")
rq.to_csv(RETRY_CURRENT_CSV, index=False, encoding="utf-8")

attempts_old = _read_csv_safe(RETRY_ATTEMPTS_CSV)
if attempts_old.empty:
    attempts_old = pd.DataFrame(columns=["file", "attempts", "last_processed_at_utc", "last_severity", "last_run_id", "status"])

attempts_map = {
    str(r["file"]): {
        "attempts": int(r.get("attempts", 0)),
        "last_processed_at_utc": r.get("last_processed_at_utc"),
        "last_severity": r.get("last_severity"),
        "last_run_id": r.get("last_run_id"),
        "status": r.get("status", "active"),
    }
    for _, r in attempts_old.iterrows()
}

for _, r in rq.iterrows():
    f = str(r["file"])
    prev = attempts_map.get(f, {"attempts": 0})
    n = int(prev.get("attempts", 0)) + 1
    attempts_map[f] = {
        "attempts": n,
        "last_processed_at_utc": r.get("processed_at_utc"),
        "last_severity": r.get("severity"),
        "last_run_id": RUN_ID,
        "status": "frozen" if n > MAX_RETRY_ATTEMPTS else "active",
    }

attempts_df = pd.DataFrame([
    {
        "file": f,
        "attempts": d["attempts"],
        "last_processed_at_utc": d["last_processed_at_utc"],
        "last_severity": d["last_severity"],
        "last_run_id": d["last_run_id"],
        "status": d["status"],
    }
    for f, d in attempts_map.items()
])

frozen_df = attempts_df[attempts_df["status"] == "frozen"].copy() if not attempts_df.empty else attempts_df.copy()
attempts_df.to_csv(RETRY_ATTEMPTS_CSV, index=False, encoding="utf-8")
frozen_df.to_csv(RETRY_FROZEN_CSV, index=False, encoding="utf-8")

sev_counts = current.groupby("severity", dropna=False).size().reset_index(name="count") if not current.empty else pd.DataFrame(columns=["severity", "count"])
live = {
    "run_id": RUN_ID,
    "updated_at_utc": _now_utc(),
    "all_files_total": int(all_files_count) if discovery_complete else None,
    "all_files_scanned": int(all_files_count),
    "discovery_complete": bool(discovery_complete),
    "candidates": int(len(candidates)),
    "batch_files": int(len(batch)),
    "events_current_rows": int(len(current)),
    "retry_pending_files": int(rq["file"].nunique()) if (not rq.empty and "file" in rq.columns) else int(len(rq)),
    "retry_frozen_files": int(len(frozen_df)),
    "severity_counts_current": {str(r["severity"]): int(r["count"]) for _, r in sev_counts.iterrows()},
}
LIVE_STATUS_JSON.write_text(json.dumps(live, ensure_ascii=False, indent=2), encoding="utf-8")

print("\nResumen por severidad (current):")
if sev_counts.empty:
    print("(sin datos)")
else:
    print(sev_counts.to_string(index=False))

print("\nTop 10 causas (issues+warns) en current:")
if current.empty:
    print("(sin datos)")
else:
    rows = []
    for _, r in current.iterrows():
        for it in _safe_parse_listlike(r.get("issues", [])):
            rows.append(("issue", it))
        for wt in _safe_parse_listlike(r.get("warns", [])):
            rows.append(("warn", wt))
    if rows:
        cdf = pd.DataFrame(rows, columns=["cause_type", "cause"])
        top = cdf.groupby(["cause_type", "cause"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False).head(10)
        print(top.to_string(index=False))
    else:
        print("(sin causas)")

print("\nSaved artifacts:")
for p in [
    RUN_CONFIG_JSON,
    STATE_FILE,
    EVENTS_HISTORY_CSV,
    EVENTS_CURRENT_CSV,
    RETRY_QUEUE_CSV,
    RETRY_CURRENT_CSV,
    RETRY_ATTEMPTS_CSV,
    RETRY_FROZEN_CSV,
    BATCH_MANIFEST_CSV,
    LIVE_STATUS_JSON,
]:
    print("-", p)
